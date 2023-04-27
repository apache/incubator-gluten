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

import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.{AggregationParams, SubstraitContext}
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.sketch.BloomFilter

import java.util
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

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
  extends BaseAggregateExec with TransformSupport with GlutenPlan {

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genHashAggregateTransformerMetrics(sparkContext)

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

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genHashAggregateTransformerMetricsUpdater(metrics)

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

  private def checkType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType
       | DoubleType | StringType | TimestampType | DateType | BinaryType => true
      case d: DecimalType => true
      case a: ArrayType => true
      case n: NullType => true
      case other => logInfo(s"Type ${dataType} not support"); false
    }
  }

  override def doValidateInternal(): Boolean = {
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId
    val aggParams = new AggregationParams
    val relNode = {
      try {
        getAggRel(substraitContext, operatorId, aggParams, null, validation = true)
      } catch {
        case e: Throwable =>
          logValidateFailure(
            s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}", e)
          return false
      }
    }
    if (aggregateAttributes.exists(attr => !checkType(attr.dataType))) {
      return false
    }
    if (groupingExpressions.exists(attr => !checkType(attr.dataType))) {
      return false
    }
    val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
    // Then, validate the generated plan in native engine.
    if (GlutenConfig.getConf.enableNativeValidation) {
      BackendsApiManager.getValidatorApiInstance.doValidate(planNode)
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
        if (expr.filter.isDefined && !expr.filter.get.isInstanceOf[Attribute] &&
          !expr.filter.get.isInstanceOf[Literal]) {
          needsProjection = true
          break
        }
        expr.mode match {
          case Partial =>
            for (aggChild <- expr.aggregateFunction.children) {
              if (!aggChild.isInstanceOf[Attribute] && !aggChild.isInstanceOf[Literal]) {
                needsProjection = true
                break
              }
            }
          // No need to consider pre-projection for PartialMerge and Final Agg.
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
    // Indices of filter used columns.
    var filterSelections: Seq[Int] = Seq()

    def appendIfNotFound(expression: Expression): Unit = {
      val foundExpr = preExpressions.find(e => e.semanticEquals(expression)).orNull
      if (foundExpr != null) {
        // If found, no need to add it to preExpressions again.
        // The selecting index will be found.
        selections = selections :+ preExpressions.indexOf(foundExpr)
      } else {
        // If not found, add this expression into preExpressions.
        // A new selecting index will be created.
        preExpressions = preExpressions :+ expression.clone()
        selections = selections :+ (preExpressions.size - 1)
      }
    }

    // Get the needed expressions from grouping expressions.
    groupingExpressions.foreach(expression => appendIfNotFound(expression))

    // Get the needed expressions from aggregation expressions.
    aggregateExpressions.foreach(aggExpr => {
      val aggregateFunc = aggExpr.aggregateFunction
      aggExpr.mode match {
        case Partial =>
          aggregateFunc.children.foreach(expression => appendIfNotFound(expression))
        case other =>
          throw new UnsupportedOperationException(s"$other not supported.")
      }
    })

    // Handle expressions used in Aggregate filter.
    aggregateExpressions.foreach(aggExpr => {
      if (aggExpr.filter.isDefined) {
        appendIfNotFound(aggExpr.filter.orNull)
        filterSelections = filterSelections :+ selections.last
      }
    })

    // Create the expression nodes needed by Project node.
    val preExprNodes = new util.ArrayList[ExpressionNode]()
    for (expr <- preExpressions) {
      preExprNodes.add(ExpressionConverter
        .replaceWithExpressionTransformer(expr, originalInputAttributes).doTransform(args))
    }
    val emitStartIndex = originalInputAttributes.size
    val inputRel = if (!validation) {
      RelBuilder.makeProjectRel(input, preExprNodes, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        input, preExprNodes, extensionNode, context, operatorId, emitStartIndex)
    }

    // Handle the pure Aggregate after Projection. Both grouping and Aggregate expressions are
    // selections.
    getAggRelAfterProject(context, selections, filterSelections, inputRel, operatorId)
  }

  protected def getAggRelAfterProject(context: SubstraitContext,
                                      selections: Seq[Int],
                                      filterSelections: Seq[Int],
                                      inputRel: RelNode,
                                      operatorId: Long): RelNode = {
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

    val aggFilterList = new util.ArrayList[ExpressionNode]()
    aggregateExpressions.foreach(aggExpr => {
      if (aggExpr.filter.isDefined) {
        aggFilterList.add(ExpressionBuilder.makeSelection(selections(colIdx)))
        colIdx += 1
      } else {
        // The number of filters should be aligned with that of aggregate functions.
        aggFilterList.add(null)
      }
    })

    RelBuilder.makeAggregateRel(
      inputRel, groupingList, aggregateFunctionList, aggFilterList, context, operatorId)
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
      resExprNodes.add(ExpressionConverter
        .replaceWithExpressionTransformer(expr, allAggregateResultAttributes).doTransform(args))
    })
    val emitStartIndex = allAggregateResultAttributes.size
    if (!validation) {
      RelBuilder.makeProjectRel(aggRel, resExprNodes, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- allAggregateResultAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        aggRel, resExprNodes, extensionNode, context, operatorId, emitStartIndex)
    }
  }

  /**
   * This method calculates the output attributes of Aggregation.
   */
  protected def getAttrForAggregateExpr(aggregateExpressions: Seq[AggregateExpression],
                                        aggregateAttributeList: Seq[Attribute]): List[Attribute] = {
    var aggregateAttr = new ListBuffer[Attribute]()
    val size = aggregateExpressions.size
    var resIndex = 0
    for (expIdx <- 0 until size) {
      val exp: AggregateExpression = aggregateExpressions(expIdx)
      val mode = exp.mode
      val aggregateFunc = exp.aggregateFunction
      aggregateFunc match {
        case Average(_, _) =>
          mode match {
            case Partial | PartialMerge =>
              val avg = aggregateFunc.asInstanceOf[Average]
              val aggBufferAttr = avg.inputAggBufferAttributes
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              resIndex += 2
            case Final =>
              aggregateAttr += aggregateAttributeList(resIndex)
              resIndex += 1
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
                aggregateAttr += ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
                val isEmptyAttr = ConverterUtils.getAttrFromExpr(aggBufferAttr(1))
                aggregateAttr += isEmptyAttr
                resIndex += 2
              } else {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
                aggregateAttr += attr
                resIndex += 1
              }
            case Final =>
              aggregateAttr += aggregateAttributeList(resIndex)
              resIndex += 1
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
              resIndex += 1
            case Final =>
              aggregateAttr += aggregateAttributeList(resIndex)
              resIndex += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case _: Max | _: Min | _: BitAndAgg | _: BitOrAgg | _: BitXorAgg =>
          mode match {
            case Partial | PartialMerge =>
              val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
              assert(aggBufferAttr.size == 1,
                s"Aggregate function ${aggregateFunc} expects one buffer attribute.")
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
              aggregateAttr += attr
              resIndex += 1
            case Final =>
              aggregateAttr += aggregateAttributeList(resIndex)
              resIndex += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case _: Corr =>
          mode match {
            case Partial | PartialMerge =>
              val expectedBufferSize = 6
              val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
              assert(aggBufferAttr.size == expectedBufferSize,
                s"Aggregate function ${aggregateFunc}" +
                  s" expects ${expectedBufferSize} buffer attribute.")
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              resIndex += expectedBufferSize
            case Final =>
              aggregateAttr += aggregateAttributeList(resIndex)
              resIndex += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: ${other}.")
          }
        case _: CovPopulation | _: CovSample =>
          mode match {
            case Partial | PartialMerge =>
              val expectedBufferSize = 4
              val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
              assert(aggBufferAttr.size == expectedBufferSize,
                s"Aggregate function ${aggregateFunc}" +
                  s" expects ${expectedBufferSize} buffer attribute.")
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              resIndex += expectedBufferSize
            case Final =>
              aggregateAttr += aggregateAttributeList(resIndex)
              resIndex += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: ${other}.")
          }
        case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop =>
          mode match {
            case Partial | PartialMerge =>
              val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              resIndex += 3
            case Final =>
              aggregateAttr += aggregateAttributeList(resIndex)
              resIndex += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case bloom if bloom.getClass.getSimpleName.equals("BloomFilterAggregate") =>
        // for spark33
          mode match {
            case Partial =>
              val bloom = aggregateFunc.asInstanceOf[TypedImperativeAggregate[BloomFilter]]
              val aggBufferAttr = bloom.inputAggBufferAttributes
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              resIndex += aggBufferAttr.size
            case Final =>
              aggregateAttr += aggregateAttributeList(resIndex)
              resIndex += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case CollectList(_, _, _) =>
          mode match {
            case Partial =>
              val collectList = aggregateFunc.asInstanceOf[CollectList]
              val aggBufferAttr = collectList.inputAggBufferAttributes
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              resIndex += aggBufferAttr.size
            case Final =>
              aggregateAttr += aggregateAttributeList(resIndex)
              resIndex += 1
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
      val exprNode = ExpressionConverter
        .replaceWithExpressionTransformer(expr, child.output).doTransform(args)
      groupingList.add(exprNode)
    })
    // Get the aggregate function nodes.
    val aggFilterList = new util.ArrayList[ExpressionNode]()
    val aggregateFunctionList = new util.ArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(aggExpr => {
      if (aggExpr.filter.isDefined) {
        val exprNode = ExpressionConverter
          .replaceWithExpressionTransformer(aggExpr.filter.get, child.output).doTransform(args)
        aggFilterList.add(exprNode)
      } else {
        // The number of filters should be aligned with that of aggregate functions.
        aggFilterList.add(null)
      }
      val aggregateFunc = aggExpr.aggregateFunction
      val childrenNodeList = new util.ArrayList[ExpressionNode]()
      val childrenNodes = aggExpr.mode match {
        case Partial =>
          aggregateFunc.children.toList.map(expr => {
            ExpressionConverter
              .replaceWithExpressionTransformer(expr, originalInputAttributes)
              .doTransform(args)
          })
        case PartialMerge | Final =>
          aggregateFunc.inputAggBufferAttributes.toList.map(attr => {
            ExpressionConverter
              .replaceWithExpressionTransformer(attr, originalInputAttributes)
              .doTransform(args)
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
        input, groupingList, aggregateFunctionList, aggFilterList, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeAggregateRel(
        input, groupingList, aggregateFunctionList, aggFilterList,
        extensionNode, context, operatorId)
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
