/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.execution

import java.util
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

import com.google.common.collect.Lists
import com.google.protobuf.Any

import io.glutenproject.GlutenConfig
import io.glutenproject.expression._
import io.glutenproject.substrait.{AggregationParams, SubstraitContext}
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.vectorized.{ExpressionEvaluator, OperatorMetrics}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Columnar Based HashAggregateExec.
 */
abstract class HashAggregateExecBaseTransformer(
                                     requiredChildDistributionExpressions: Option[Seq[Expression]],
                                     groupingExpressions: Seq[NamedExpression],
                                     aggregateExpressions: Seq[AggregateExpression],
                                     aggregateAttributes: Seq[Attribute],
                                     initialInputBufferOffset: Int,
                                     resultExpressions: Seq[NamedExpression],
                                     child: SparkPlan)
  extends BaseAggregateExec
    with TransformSupport {

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override lazy val metrics = Map(
    "inputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
    "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
    "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
    "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
    "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
    "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
    "count" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
    "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_input"),
    "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
    "numMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of memory allocations"),

    "preProjectionInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of preProjection input rows"),
    "preProjectionInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of preProjection input vectors"),
    "preProjectionInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of preProjection input bytes"),
    "preProjectionRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of preProjection raw input rows"),
    "preProjectionRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of preProjection raw input bytes"),
    "preProjectionOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of preProjection output rows"),
    "preProjectionOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of preProjection output vectors"),
    "preProjectionOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of preProjection output bytes"),
    "preProjectionCount" -> SQLMetrics.createMetric(
      sparkContext, "preProjection cpu wall time count"),
    "preProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_preProjection"),
    "preProjectionPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "preProjection peak memory bytes"),
    "preProjectionNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of preProjection memory allocations"),

    "aggInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of aggregation input rows"),
    "aggInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of aggregation input vectors"),
    "aggInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of aggregation input bytes"),
    "aggRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of aggregation raw input rows"),
    "aggRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of aggregation raw input bytes"),
    "aggOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of aggregation output rows"),
    "aggOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of aggregation output vectors"),
    "aggOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of aggregation output bytes"),
    "aggCount" -> SQLMetrics.createMetric(
      sparkContext, "aggregation cpu wall time count"),
    "aggWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_aggregation"),
    "aggPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "aggregation peak memory bytes"),
    "aggNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of aggregation memory allocations"),

    "extractionInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of extraction input rows"),
    "extractionInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of extraction input vectors"),
    "extractionInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of extraction input bytes"),
    "extractionRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of extraction raw input rows"),
    "extractionRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of extraction raw input bytes"),
    "extractionOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of extraction output rows"),
    "extractionOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of extraction output vectors"),
    "extractionOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of extraction output bytes"),
    "extractionCount" -> SQLMetrics.createMetric(
      sparkContext, "extraction cpu wall time count"),
    "extractionWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_extraction"),
    "extractionPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "extraction peak memory bytes"),
    "extractionNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of extraction memory allocations"),

    "postProjectionInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection input rows"),
    "postProjectionInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection input vectors"),
    "postProjectionInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of postProjection input bytes"),
    "postProjectionRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection raw input rows"),
    "postProjectionRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of postProjection raw input bytes"),
    "postProjectionOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection output rows"),
    "postProjectionOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection output vectors"),
    "postProjectionOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of postProjection output bytes"),
    "postProjectionCount" -> SQLMetrics.createMetric(
      sparkContext, "postProjection cpu wall time count"),
    "postProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_postProjection"),
    "postProjectionPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "postProjection peak memory bytes"),
    "postProjectionNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection memory allocations"),
    "finalOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of final output rows"),
    "finalOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of final output vectors"))

  val inputRows: SQLMetric = longMetric("inputRows")
  val inputVectors: SQLMetric = longMetric("inputVectors")
  val inputBytes: SQLMetric = longMetric("inputBytes")
  val rawInputRows: SQLMetric = longMetric("rawInputRows")
  val rawInputBytes: SQLMetric = longMetric("rawInputBytes")
  val outputRows: SQLMetric = longMetric("outputRows")
  val outputVectors: SQLMetric = longMetric("outputVectors")
  val outputBytes: SQLMetric = longMetric("outputBytes")
  val count: SQLMetric = longMetric("count")
  val wallNanos: SQLMetric = longMetric("wallNanos")
  val peakMemoryBytes: SQLMetric = longMetric("peakMemoryBytes")
  val numMemoryAllocations: SQLMetric = longMetric("numMemoryAllocations")

  val preProjectionInputRows: SQLMetric = longMetric("preProjectionInputRows")
  val preProjectionInputVectors: SQLMetric = longMetric("preProjectionInputVectors")
  val preProjectionInputBytes: SQLMetric = longMetric("preProjectionInputBytes")
  val preProjectionRawInputRows: SQLMetric = longMetric("preProjectionRawInputRows")
  val preProjectionRawInputBytes: SQLMetric = longMetric("preProjectionRawInputBytes")
  val preProjectionOutputRows: SQLMetric = longMetric("preProjectionOutputRows")
  val preProjectionOutputVectors: SQLMetric = longMetric("preProjectionOutputVectors")
  val preProjectionOutputBytes: SQLMetric = longMetric("preProjectionOutputBytes")
  val preProjectionCount: SQLMetric = longMetric("preProjectionCount")
  val preProjectionWallNanos: SQLMetric = longMetric("preProjectionWallNanos")
  val preProjectionPeakMemoryBytes: SQLMetric = longMetric("preProjectionPeakMemoryBytes")
  val preProjectionNumMemoryAllocations: SQLMetric =
    longMetric("preProjectionNumMemoryAllocations")

  val aggInputRows: SQLMetric = longMetric("aggInputRows")
  val aggInputVectors: SQLMetric = longMetric("aggInputVectors")
  val aggInputBytes: SQLMetric = longMetric("aggInputBytes")
  val aggRawInputRows: SQLMetric = longMetric("aggRawInputRows")
  val aggRawInputBytes: SQLMetric = longMetric("aggRawInputBytes")
  val aggOutputRows: SQLMetric = longMetric("aggOutputRows")
  val aggOutputVectors: SQLMetric = longMetric("aggOutputVectors")
  val aggOutputBytes: SQLMetric = longMetric("aggOutputBytes")
  val aggCount: SQLMetric = longMetric("aggCount")
  val aggWallNanos: SQLMetric = longMetric("aggWallNanos")
  val aggPeakMemoryBytes: SQLMetric = longMetric("aggPeakMemoryBytes")
  val aggNumMemoryAllocations: SQLMetric = longMetric("aggNumMemoryAllocations")

  val extractionInputRows: SQLMetric = longMetric("extractionInputRows")
  val extractionInputVectors: SQLMetric = longMetric("extractionInputVectors")
  val extractionInputBytes: SQLMetric = longMetric("extractionInputBytes")
  val extractionRawInputRows: SQLMetric = longMetric("extractionRawInputRows")
  val extractionRawInputBytes: SQLMetric = longMetric("extractionRawInputBytes")
  val extractionOutputRows: SQLMetric = longMetric("extractionOutputRows")
  val extractionOutputVectors: SQLMetric = longMetric("extractionOutputVectors")
  val extractionOutputBytes: SQLMetric = longMetric("extractionOutputBytes")
  val extractionCount: SQLMetric = longMetric("extractionCount")
  val extractionWallNanos: SQLMetric = longMetric("extractionWallNanos")
  val extractionPeakMemoryBytes: SQLMetric = longMetric("extractionPeakMemoryBytes")
  val extractionNumMemoryAllocations: SQLMetric =
    longMetric("extractionNumMemoryAllocations")

  val postProjectionInputRows: SQLMetric = longMetric("postProjectionInputRows")
  val postProjectionInputVectors: SQLMetric = longMetric("postProjectionInputVectors")
  val postProjectionInputBytes: SQLMetric = longMetric("postProjectionInputBytes")
  val postProjectionRawInputRows: SQLMetric = longMetric("postProjectionRawInputRows")
  val postProjectionRawInputBytes: SQLMetric = longMetric("postProjectionRawInputBytes")
  val postProjectionOutputRows: SQLMetric = longMetric("postProjectionOutputRows")
  val postProjectionOutputVectors: SQLMetric = longMetric("postProjectionOutputVectors")
  val postProjectionOutputBytes: SQLMetric = longMetric("postProjectionOutputBytes")
  val postProjectionCount: SQLMetric = longMetric("postProjectionCount")
  val postProjectionWallNanos: SQLMetric = longMetric("postProjectionWallNanos")
  val postProjectionPeakMemoryBytes: SQLMetric = longMetric("postProjectionPeakMemoryBytes")
  val postProjectionNumMemoryAllocations: SQLMetric =
    longMetric("postProjectionNumMemoryAllocations")

  val finalOutputRows: SQLMetric = longMetric("finalOutputRows")
  val finalOutputVectors: SQLMetric = longMetric("finalOutputVectors")

  lazy val aggregateResultAttributes = {
    val groupingAttributes = groupingExpressions.map(expr => {
      ConverterUtils.getAttrFromExpr(expr).toAttribute
    })
    groupingAttributes ++ aggregateExpressions.map(expr => {
      expr.resultAttribute
    })
  }
  val sparkConf = sparkContext.getConf
  val resAttributes: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  // The direct outputs of Aggregation.
  protected val allAggregateResultAttributes: List[Attribute] = {
    val groupingAttributes = groupingExpressions.map(expr => {
      ConverterUtils.getAttrFromExpr(expr).toAttribute
    })
    groupingAttributes.toList ::: getAttrForAggregateExpr(
      aggregateExpressions,
      aggregateAttributes)
  }

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = child match {
    case c: TransformSupport =>
      c.getBuildPlans
    case _ =>
      Seq()
  }

  override def getStreamedLeafPlan: SparkPlan = child match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def updateMetrics(outNumBatches: Long, outNumRows: Long): Unit = {
    finalOutputVectors += outNumBatches
    finalOutputRows += outNumRows
  }

  def updateAggregationMetrics(aggregationMetrics: java.util.ArrayList[OperatorMetrics],
                               aggParams: AggregationParams): Unit = {
    var idx = 0
    if (aggParams.postProjectionNeeded) {
      val metrics = aggregationMetrics.get(idx)
      postProjectionInputRows += metrics.inputRows
      postProjectionInputVectors += metrics.inputVectors
      postProjectionInputBytes += metrics.inputBytes
      postProjectionRawInputRows += metrics.rawInputRows
      postProjectionRawInputBytes += metrics.rawInputBytes
      postProjectionOutputRows += metrics.outputRows
      postProjectionOutputVectors += metrics.outputVectors
      postProjectionOutputBytes += metrics.outputBytes
      postProjectionCount += metrics.count
      postProjectionWallNanos += metrics.wallNanos
      postProjectionPeakMemoryBytes += metrics.peakMemoryBytes
      postProjectionNumMemoryAllocations += metrics.numMemoryAllocations
      idx += 1
    }

    if (aggParams.extractionNeeded) {
      val metrics = aggregationMetrics.get(idx)
      extractionInputRows += metrics.inputRows
      extractionInputVectors += metrics.inputVectors
      extractionInputBytes += metrics.inputBytes
      extractionRawInputRows += metrics.rawInputRows
      extractionRawInputBytes += metrics.rawInputBytes
      extractionOutputRows += metrics.outputRows
      extractionOutputVectors += metrics.outputVectors
      extractionOutputBytes += metrics.outputBytes
      extractionCount += metrics.count
      extractionWallNanos += metrics.wallNanos
      extractionPeakMemoryBytes += metrics.peakMemoryBytes
      extractionNumMemoryAllocations += metrics.numMemoryAllocations
      idx += 1
    }

    val aggMetrics = aggregationMetrics.get(idx)
    aggInputRows += aggMetrics.inputRows
    aggInputVectors += aggMetrics.inputVectors
    aggInputBytes += aggMetrics.inputBytes
    aggRawInputRows += aggMetrics.rawInputRows
    aggRawInputBytes += aggMetrics.rawInputBytes
    aggOutputRows += aggMetrics.outputRows
    aggOutputVectors += aggMetrics.outputVectors
    aggOutputBytes += aggMetrics.outputBytes
    aggCount += aggMetrics.count
    aggWallNanos += aggMetrics.wallNanos
    aggPeakMemoryBytes += aggMetrics.peakMemoryBytes
    aggNumMemoryAllocations += aggMetrics.numMemoryAllocations
    idx += 1

    if (aggParams.preProjectionNeeded) {
      val metrics = aggregationMetrics.get(idx)
      preProjectionInputRows += metrics.inputRows
      preProjectionInputVectors += metrics.inputVectors
      preProjectionInputBytes += metrics.inputBytes
      preProjectionRawInputRows += metrics.rawInputRows
      preProjectionRawInputBytes += metrics.rawInputBytes
      preProjectionOutputRows += metrics.outputRows
      preProjectionOutputVectors += metrics.outputVectors
      preProjectionOutputBytes += metrics.outputBytes
      preProjectionCount += metrics.count
      preProjectionWallNanos += metrics.wallNanos
      preProjectionPeakMemoryBytes += metrics.peakMemoryBytes
      preProjectionNumMemoryAllocations += metrics.numMemoryAllocations
      idx += 1
    }

    if (aggParams.isReadRel) {
      val metrics = aggregationMetrics.get(idx)
      inputRows += metrics.inputRows
      inputVectors += metrics.inputVectors
      inputBytes += metrics.inputBytes
      rawInputRows += metrics.rawInputRows
      rawInputBytes += metrics.rawInputBytes
      outputRows += metrics.outputRows
      outputVectors += metrics.outputVectors
      outputBytes += metrics.outputBytes
      count += metrics.count
      wallNanos += metrics.wallNanos
      peakMemoryBytes += metrics.peakMemoryBytes
      numMemoryAllocations += metrics.numMemoryAllocations
      idx += 1
    }
  }

  override def getChild: SparkPlan = child

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions
    val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"HashAggregateTransformer(keys=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"HashAggregateTransformer(keys=$keyString, functions=$functionString)"
    }
  }

  // override def canEqual(that: Any): Boolean = false

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  override def doValidate(): Boolean = {
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId
    val aggParams = new AggregationParams
    val relNode =
      try {
        getAggRel(substraitContext, operatorId, aggParams, null, validation = true)
      } catch {
        case e: Throwable =>
          logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
          return false
      }
    val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
    // Then, validate the generated plan in native engine.
    if (GlutenConfig.getConf.enableNativeValidation) {
      val validator = new ExpressionEvaluator()
      validator.doValidate(planNode.toProtobuf.toByteArray)
    } else {
      true
    }
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }

    val aggParams = new AggregationParams
    val operatorId = context.nextOperatorId

    val (relNode, inputAttributes, outputAttributes) = if (childCtx != null) {
      (getAggRel(context, operatorId, aggParams, childCtx.root), childCtx.outputAttributes, output)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      aggParams.isReadRel = true
      val attrList = new util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)
      (getAggRel(context, operatorId, aggParams, readRel), child.output, output)
    }
    TransformContext(inputAttributes, outputAttributes, relNode)
  }

  // Members declared in org.apache.spark.sql.execution.AliasAwareOutputPartitioning
  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  // Members declared in org.apache.spark.sql.execution.CodegenSupport
  protected def doProduce(ctx: CodegenContext): String = throw new UnsupportedOperationException()

  // Members declared in org.apache.spark.sql.execution.SparkPlan
  protected override def doExecute()
  : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] =
    throw new UnsupportedOperationException()

  protected def needsPreProjection: Boolean = {
    var needsProjection = false
    breakable {
      for (expr <- groupingExpressions) {
        if (!expr.isInstanceOf[Attribute]) {
          needsProjection = true
          break
        }
      }
    }
    breakable {
      for (expr <- aggregateExpressions) {
        expr.mode match {
          case Partial | PartialMerge =>
            for (aggChild <- expr.aggregateFunction.children) {
              if (!aggChild.isInstanceOf[Attribute] && !aggChild.isInstanceOf[Literal]) {
                needsProjection = true
                break
              }
            }
          // Do not need to consider pre-projection for Final Agg.
          case _ =>
        }
      }
    }
    needsProjection
  }

  protected def needsPostProjection(aggOutAttributes: List[Attribute]): Boolean = {
    // Check if Post-Projection is needed after the Aggregation.
    var needsProjection = false
    // If the result expressions has different size with output attribute,
    // post-projection is needed.
    if (resultExpressions.size != aggOutAttributes.size) {
      needsProjection = true
    } else {
      // Compare each item in result expressions and output attributes.
      breakable {
        for (exprIdx <- resultExpressions.indices) {
          resultExpressions(exprIdx) match {
            case exprAttr: Attribute =>
              val resAttr = aggOutAttributes(exprIdx)
              // If the result attribute and result expression has different name or type,
              // post-projection is needed.
              if (exprAttr.name != resAttr.name ||
                  exprAttr.dataType != resAttr.dataType) {
                needsProjection = true
                break
              }
            case _ =>
              // If result expression is not instance of Attribute,
              // post-projection is needed.
              needsProjection = true
              break
          }
        }
      }
    }
    needsProjection
  }

  protected def getAggRelWithPreProjection(context: SubstraitContext,
                                           originalInputAttributes: Seq[Attribute],
                                           operatorId: Long,
                                           input: RelNode = null,
                                           validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Will add a Projection before Aggregate.
    // Logic was added to prevent selecting the same column for more than once,
    // so the expression in preExpressions will be unique.
    var preExpressions: Seq[Expression] = Seq()
    var selections: Seq[Int] = Seq()

    // Get the needed expressions from grouping expressions.
    groupingExpressions.foreach(expr => {
      val foundExpr = preExpressions.find(e => e.semanticEquals(expr)).orNull
      if (foundExpr != null) {
        // If found, no need to add it to preExpressions again.
        // The selecting index will be found.
        selections = selections :+ preExpressions.indexOf(foundExpr)
      } else {
        // If not found, add this expression into preExpressions.
        // A new selecting index will be created.
        preExpressions = preExpressions :+ expr.clone()
        selections = selections :+ (preExpressions.size - 1)
      }
    })

    // Get the needed expressions from aggregation expressions.
    aggregateExpressions.foreach(aggExpr => {
      val aggregateFunc = aggExpr.aggregateFunction
      aggExpr.mode match {
        case Partial =>
          aggregateFunc.children.toList.map(childExpr => {
            val foundExpr = preExpressions.find(e => e.semanticEquals(childExpr)).orNull
            if (foundExpr != null) {
              selections = selections :+ preExpressions.indexOf(foundExpr)
            } else {
              preExpressions = preExpressions :+ childExpr.clone()
              selections = selections :+ (preExpressions.size - 1)
            }
          })
        case other =>
          throw new UnsupportedOperationException(s"$other not supported.")
      }
    })

    // Create the expression nodes needed by Project node.
    val preExprNodes = new util.ArrayList[ExpressionNode]()
    for (expr <- preExpressions) {
      val preExpr: Expression = ExpressionConverter
        .replaceWithExpressionTransformer(expr, originalInputAttributes)
      preExprNodes.add(preExpr.asInstanceOf[ExpressionTransformer].doTransform(args))
    }
    val inputRel = if (!validation) {
      RelBuilder.makeProjectRel(input, preExprNodes, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(input, preExprNodes, extensionNode, context, operatorId)
    }

    // Handle the pure Aggregate after Projection. Both grouping and Aggregate expressions are
    // selections.
    getAggRelAfterProject(context, selections, inputRel, operatorId)
  }

  protected def getAggRelAfterProject(context: SubstraitContext, selections: Seq[Int],
                                      inputRel: RelNode, operatorId: Long): RelNode = {
    val groupingList = new util.ArrayList[ExpressionNode]()
    var colIdx = 0
    while (colIdx < groupingExpressions.size) {
      val groupingExpr: ExpressionNode = ExpressionBuilder.makeSelection(selections(colIdx))
      groupingList.add(groupingExpr)
      colIdx += 1
    }

    // Create Aggregation functions.
    val aggregateFunctionList = new util.ArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(aggExpr => {
      val aggregateFunc = aggExpr.aggregateFunction
      val childrenNodeList = new util.ArrayList[ExpressionNode]()
      val childrenNodes = aggregateFunc.children.toList.map(_ => {
        val aggExpr = ExpressionBuilder.makeSelection(selections(colIdx))
        colIdx += 1
        aggExpr
      })
      for (node <- childrenNodes) {
        childrenNodeList.add(node)
      }
      addFunctionNode(context.registeredFunction, aggregateFunc, childrenNodeList,
        aggExpr.mode, aggregateFunctionList)
    })

    RelBuilder.makeAggregateRel(
      inputRel, groupingList, aggregateFunctionList, context, operatorId)
  }

  protected def addFunctionNode(args: java.lang.Object,
                                aggregateFunction: AggregateFunction,
                                childrenNodeList: util.ArrayList[ExpressionNode],
                                aggregateMode: AggregateMode,
                                aggregateNodeList: util.ArrayList[AggregateFunctionNode]): Unit = {
    aggregateNodeList.add(
      ExpressionBuilder.makeAggregateFunction(
        AggregateFunctionsBuilder.create(args, aggregateFunction),
        childrenNodeList,
        modeToKeyWord(aggregateMode),
        ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)))
  }

  protected def applyPostProjection(context: SubstraitContext,
                                    aggRel: RelNode,
                                    operatorId: Long,
                                    validation: Boolean): RelNode = {
    val args = context.registeredFunction

    // Will add an projection after Agg.
    val resExprNodes = new util.ArrayList[ExpressionNode]()
    resultExpressions.foreach(expr => {
      val aggExpr: Expression = ExpressionConverter
        .replaceWithExpressionTransformer(expr, allAggregateResultAttributes)
      resExprNodes.add(aggExpr.asInstanceOf[ExpressionTransformer].doTransform(args))
    })
    if (!validation) {
      RelBuilder.makeProjectRel(aggRel, resExprNodes, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- allAggregateResultAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(aggRel, resExprNodes, extensionNode, context, operatorId)
    }
  }

  /**
   * This method calculates the output attributes of Aggregation.
   */
  protected def getAttrForAggregateExpr(aggregateExpressions: Seq[AggregateExpression],
                                        aggregateAttributeList: Seq[Attribute]): List[Attribute] = {
    var aggregateAttr = new ListBuffer[Attribute]()
    val size = aggregateExpressions.size
    var res_index = 0
    for (expIdx <- 0 until size) {
      val exp: AggregateExpression = aggregateExpressions(expIdx)
      val mode = exp.mode
      val aggregateFunc = exp.aggregateFunction
      aggregateFunc match {
        case Average(_, _) =>
          mode match {
            case Partial =>
              val avg = aggregateFunc.asInstanceOf[Average]
              val aggBufferAttr = avg.inputAggBufferAttributes
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              res_index += 2
            case PartialMerge =>
              val avg = aggregateFunc.asInstanceOf[Average]
              val aggBufferAttr = avg.inputAggBufferAttributes
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              res_index += 1
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Sum(_, _) =>
          mode match {
            case Partial | PartialMerge =>
              val sum = aggregateFunc.asInstanceOf[Sum]
              val aggBufferAttr = sum.inputAggBufferAttributes
              if (aggBufferAttr.size == 2) {
                // decimal sum check sum.resultType
                val sum_attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
                aggregateAttr += sum_attr
                val isempty_attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(1))
                aggregateAttr += isempty_attr
                res_index += 2
              } else {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
                aggregateAttr += attr
                res_index += 1
              }
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Count(_) =>
          mode match {
            case Partial | PartialMerge =>
              val count = aggregateFunc.asInstanceOf[Count]
              val aggBufferAttr = count.inputAggBufferAttributes
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
              aggregateAttr += attr
              res_index += 1
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Max(_) =>
          mode match {
            case Partial | PartialMerge =>
              val max = aggregateFunc.asInstanceOf[Max]
              val aggBufferAttr = max.inputAggBufferAttributes
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
              aggregateAttr += attr
              res_index += 1
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Min(_) =>
          mode match {
            case Partial | PartialMerge =>
              val min = aggregateFunc.asInstanceOf[Min]
              val aggBufferAttr = min.inputAggBufferAttributes
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
              aggregateAttr += attr
              res_index += 1
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case StddevSamp(_, _) =>
          mode match {
            case Partial =>
              val stddevSamp = aggregateFunc.asInstanceOf[StddevSamp]
              val aggBufferAttr = stddevSamp.inputAggBufferAttributes
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              res_index += 3
            case PartialMerge =>
              throw new UnsupportedOperationException("not currently supported: PartialMerge.")
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case other =>
          throw new UnsupportedOperationException(s"not currently supported: $other.")
      }
    }
    aggregateAttr.toList
  }

  protected def modeToKeyWord(aggregateMode: AggregateMode): String = {
    aggregateMode match {
      case Partial => "PARTIAL"
      case PartialMerge => "PARTIAL_MERGE"
      case Final => "FINAL"
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }

  protected def getAggRelWithoutPreProjection(context: SubstraitContext,
                                              originalInputAttributes: Seq[Attribute],
                                              operatorId: Long,
                                              input: RelNode = null,
                                              validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Get the grouping nodes.
    val groupingList = new util.ArrayList[ExpressionNode]()
    groupingExpressions.foreach(expr => {
      // Use 'child.output' as based Seq[Attribute], the originalInputAttributes
      // may be different for each backend.
      val groupingExpr: Expression = ExpressionConverter
        .replaceWithExpressionTransformer(expr, child.output)
      val exprNode = groupingExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
      groupingList.add(exprNode)
    })
    // Get the aggregate function nodes.
    val aggregateFunctionList = new util.ArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(aggExpr => {
      val aggregateFunc = aggExpr.aggregateFunction
      val childrenNodeList = new util.ArrayList[ExpressionNode]()
      val childrenNodes = aggExpr.mode match {
        case Partial =>
          aggregateFunc.children.toList.map(expr => {
            val aggExpr: Expression = ExpressionConverter
              .replaceWithExpressionTransformer(expr, originalInputAttributes)
            aggExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
          })
        case Final =>
          aggregateFunc.inputAggBufferAttributes.toList.map(attr => {
            val aggExpr: Expression = ExpressionConverter
              .replaceWithExpressionTransformer(attr, originalInputAttributes)
            aggExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
          })
        case other =>
          throw new UnsupportedOperationException(s"$other not supported.")
      }
      for (node <- childrenNodes) {
        childrenNodeList.add(node)
      }
      addFunctionNode(args, aggregateFunc, childrenNodeList, aggExpr.mode, aggregateFunctionList)
    })
    if (!validation) {
      RelBuilder.makeAggregateRel(
        input, groupingList, aggregateFunctionList, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(inputTypeNodeList).toProtobuf))
      RelBuilder.makeAggregateRel(
        input, groupingList, aggregateFunctionList, extensionNode, context, operatorId)
    }
  }

  protected def getAggRel(context: SubstraitContext,
                          operatorId: Long,
                          aggParams: AggregationParams,
                          input: RelNode = null,
                          validation: Boolean = false): RelNode = {
    val originalInputAttributes = child.output
    val aggRel = if (needsPreProjection) {
      getAggRelWithPreProjection(
        context, originalInputAttributes, operatorId, input, validation)
    } else {
      getAggRelWithoutPreProjection(
        context, originalInputAttributes, operatorId, input, validation)
    }
    // Will check if post-projection is needed. If yes, a ProjectRel will be added after the
    // AggregateRel.
    if (!needsPostProjection(allAggregateResultAttributes)) {
      aggRel
    } else {
    applyPostProjection(context, aggRel, operatorId, validation)
    }
  }
}
