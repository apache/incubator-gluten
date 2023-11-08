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

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.{AggregationParams, SubstraitContext}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.Any

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** Columnar Based HashAggregateExec. */
abstract class HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends BaseAggregateExec
  with UnaryTransformSupport {

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genHashAggregateTransformerMetrics(sparkContext)

  // The direct outputs of Aggregation.
  protected lazy val allAggregateResultAttributes: List[Attribute] = {
    val groupingAttributes = groupingExpressions.map(
      expr => {
        ConverterUtils.getAttrFromExpr(expr).toAttribute
      })
    groupingAttributes.toList ::: getAttrForAggregateExprs(
      aggregateExpressions,
      aggregateAttributes)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
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

  protected def checkType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          StringType | TimestampType | DateType | BinaryType =>
        true
      case _: DecimalType => true
      case _: ArrayType => true
      case _: NullType => true
      case _ => false
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    val aggParams = new AggregationParams
    val relNode = getAggRel(substraitContext, operatorId, aggParams, null, validation = true)
    if (aggregateAttributes.exists(attr => !checkType(attr.dataType))) {
      return ValidationResult.notOk(
        "Found not supported data type in aggregation expression," +
          s"${aggregateAttributes.map(_.dataType)}")
    }
    if (groupingExpressions.exists(attr => !checkType(attr.dataType))) {
      return ValidationResult.notOk(
        "Found not supported data type in group expression," +
          s"${groupingExpressions.map(_.dataType)}")
    }
    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }

    val aggParams = new AggregationParams
    val operatorId = context.nextOperatorId(this.nodeName)

    val (relNode, inputAttributes, outputAttributes) = if (childCtx != null) {
      (getAggRel(context, operatorId, aggParams, childCtx.root), childCtx.outputAttributes, output)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      aggParams.isReadRel = true
      val readRel = RelBuilder.makeReadRel(child.output.asJava, context, operatorId)
      (getAggRel(context, operatorId, aggParams, readRel), child.output, output)
    }
    TransformContext(inputAttributes, outputAttributes, relNode)
  }

  // Members declared in org.apache.spark.sql.execution.AliasAwareOutputPartitioning
  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  // Check if Pre-Projection is needed before the Aggregation.
  protected def needsPreProjection: Boolean = {
    groupingExpressions.exists {
      case _: Attribute => false
      case _ => true
    } || aggregateExpressions.exists {
      expr =>
        expr.filter match {
          case None | Some(_: Attribute) | Some(_: Literal) =>
          case _ => return true
        }
        expr.mode match {
          case Partial =>
            expr.aggregateFunction.children.exists {
              case _: Attribute | _: Literal => false
              case _ => true
            }
          // No need to consider pre-projection for PartialMerge and Final Agg.
          case _ => false
        }
    }
  }

  // Check if Post-Projection is needed after the Aggregation.
  protected def needsPostProjection(aggOutAttributes: List[Attribute]): Boolean = {
    // If the result expressions has different size with output attribute,
    // post-projection is needed.
    resultExpressions.size != aggOutAttributes.size ||
    // Compare each item in result expressions and output attributes.
    resultExpressions.zip(aggOutAttributes).exists {
      case (exprAttr: Attribute, resAttr) =>
        // If the result attribute and result expression has different name or type,
        // post-projection is needed.
        exprAttr.name != resAttr.name || exprAttr.dataType != resAttr.dataType
      case _ =>
        // If result expression is not instance of Attribute,
        // post-projection is needed.
        true
    }
  }

  protected def getAggRelWithPreProjection(
      context: SubstraitContext,
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
    aggregateExpressions.foreach(
      aggExpr => {
        val aggregateFunc = aggExpr.aggregateFunction
        aggExpr.mode match {
          case Partial =>
            aggregateFunc.children.foreach(expression => appendIfNotFound(expression))
          case other =>
            throw new UnsupportedOperationException(s"$other not supported.")
        }
      })

    // Handle expressions used in Aggregate filter.
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          appendIfNotFound(aggExpr.filter.orNull)
          filterSelections = filterSelections :+ selections.last
        }
      })

    // Create the expression nodes needed by Project node.
    val preExprNodes = preExpressions
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, originalInputAttributes)
          .doTransform(args))
      .asJava
    val emitStartIndex = originalInputAttributes.size
    val inputRel = if (!validation) {
      RelBuilder.makeProjectRel(input, preExprNodes, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for a validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        input,
        preExprNodes,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }

    // Handle the pure Aggregate after Projection. Both grouping and Aggregate expressions are
    // selections.
    getAggRelAfterProject(context, selections, filterSelections, inputRel, operatorId)
  }

  protected def getAggRelAfterProject(
      context: SubstraitContext,
      selections: Seq[Int],
      filterSelections: Seq[Int],
      inputRel: RelNode,
      operatorId: Long): RelNode = {
    val groupingList = new JArrayList[ExpressionNode]()
    var colIdx = 0
    while (colIdx < groupingExpressions.size) {
      val groupingExpr: ExpressionNode = ExpressionBuilder.makeSelection(selections(colIdx))
      groupingList.add(groupingExpr)
      colIdx += 1
    }

    // Create Aggregation functions.
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodeList = new JArrayList[ExpressionNode]()
        val childrenNodes = aggregateFunc.children.toList.map(
          _ => {
            val aggExpr = ExpressionBuilder.makeSelection(selections(colIdx))
            colIdx += 1
            aggExpr
          })
        for (node <- childrenNodes) {
          childrenNodeList.add(node)
        }
        addFunctionNode(
          context.registeredFunction,
          aggregateFunc,
          childrenNodeList,
          aggExpr.mode,
          aggregateFunctionList)
      })

    val aggFilterList = new JArrayList[ExpressionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          aggFilterList.add(ExpressionBuilder.makeSelection(selections(colIdx)))
          colIdx += 1
        } else {
          // The number of filters should be aligned with that of aggregate functions.
          aggFilterList.add(null)
        }
      })

    RelBuilder.makeAggregateRel(
      inputRel,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      context,
      operatorId)
  }

  protected def addFunctionNode(
      args: java.lang.Object,
      aggregateFunction: AggregateFunction,
      childrenNodeList: JList[ExpressionNode],
      aggregateMode: AggregateMode,
      aggregateNodeList: JList[AggregateFunctionNode]): Unit = {
    aggregateNodeList.add(
      ExpressionBuilder.makeAggregateFunction(
        AggregateFunctionsBuilder.create(args, aggregateFunction),
        childrenNodeList,
        modeToKeyWord(aggregateMode),
        ConverterUtils.getTypeNode(aggregateFunction.dataType, aggregateFunction.nullable)
      ))
  }

  protected def applyPostProjection(
      context: SubstraitContext,
      aggRel: RelNode,
      operatorId: Long,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction

    // Will add an projection after Agg.
    val resExprNodes = resultExpressions
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, allAggregateResultAttributes)
          .doTransform(args))
      .asJava
    val emitStartIndex = allAggregateResultAttributes.size
    if (!validation) {
      RelBuilder.makeProjectRel(aggRel, resExprNodes, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = allAggregateResultAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        aggRel,
        resExprNodes,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }
  }

  /** This method calculates the output attributes of Aggregation. */
  protected def getAttrForAggregateExprs(
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributeList: Seq[Attribute]): List[Attribute] = {
    val aggregateAttr = new ListBuffer[Attribute]()
    val size = aggregateExpressions.size
    var resIndex = 0
    for (expIdx <- 0 until size) {
      val exp: AggregateExpression = aggregateExpressions(expIdx)
      resIndex = getAttrForAggregateExpr(exp, aggregateAttributeList, aggregateAttr, resIndex)
    }
    aggregateAttr.toList
  }

  protected def getAttrForAggregateExpr(
      exp: AggregateExpression,
      aggregateAttributeList: Seq[Attribute],
      aggregateAttr: ListBuffer[Attribute],
      index: Int): Int = {
    var resIndex = index
    val mode = exp.mode
    val aggregateFunc = exp.aggregateFunction
    // First handle the custom aggregate functions
    if (
      ExpressionMappings.expressionExtensionTransformer.extensionExpressionsMapping.contains(
        aggregateFunc.getClass)
    ) {
      ExpressionMappings.expressionExtensionTransformer
        .getAttrsIndexForExtensionAggregateExpr(
          aggregateFunc,
          mode,
          exp,
          aggregateAttributeList,
          aggregateAttr,
          index)
    } else {
      if (!checkAggFuncModeSupport(aggregateFunc, mode)) {
        throw new UnsupportedOperationException(
          s"Unsupported aggregate mode: $mode for ${aggregateFunc.prettyName}")
      }
      mode match {
        case Partial | PartialMerge =>
          val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
          for (index <- aggBufferAttr.indices) {
            val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
            aggregateAttr += attr
          }
          resIndex += aggBufferAttr.size
          resIndex
        case Final =>
          aggregateAttr += aggregateAttributeList(resIndex)
          resIndex += 1
          resIndex
        case other =>
          throw new UnsupportedOperationException(s"Unsupported aggregate mode: $other.")
      }
    }
  }

  protected def checkAggFuncModeSupport(
      aggFunc: AggregateFunction,
      mode: AggregateMode): Boolean = {
    aggFunc match {
      case _: CollectList | _: CollectSet =>
        mode match {
          case Partial | Final => true
          case _ => false
        }
      case bloom if bloom.getClass.getSimpleName.equals("BloomFilterAggregate") =>
        mode match {
          case Partial | Final => true
          case _ => false
        }
      case _ =>
        mode match {
          case Partial | PartialMerge | Final => true
          case _ => false
        }
    }
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

  protected def getAggRelWithoutPreProjection(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode = null,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Get the grouping nodes.
    // Use 'child.output' as based Seq[Attribute], the originalInputAttributes
    // may be different for each backend.
    val groupingList = groupingExpressions
      .map(
        ExpressionConverter
          .replaceWithExpressionTransformer(_, child.output)
          .doTransform(args))
      .asJava
    // Get the aggregate function nodes.
    val aggFilterList = new JArrayList[ExpressionNode]()
    val aggregateFunctionList = new JArrayList[AggregateFunctionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          val exprNode = ExpressionConverter
            .replaceWithExpressionTransformer(aggExpr.filter.get, child.output)
            .doTransform(args)
          aggFilterList.add(exprNode)
        } else {
          // The number of filters should be aligned with that of aggregate functions.
          aggFilterList.add(null)
        }
        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodes = aggExpr.mode match {
          case Partial =>
            aggregateFunc.children.toList.map(
              expr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(expr, originalInputAttributes)
                  .doTransform(args)
              })
          case PartialMerge | Final =>
            aggregateFunc.inputAggBufferAttributes.toList.map(
              attr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(attr, originalInputAttributes)
                  .doTransform(args)
              })
          case other =>
            throw new UnsupportedOperationException(s"$other not supported.")
        }
        addFunctionNode(
          args,
          aggregateFunc,
          childrenNodes.asJava,
          aggExpr.mode,
          aggregateFunctionList)
      })
    if (!validation) {
      RelBuilder.makeAggregateRel(
        input,
        groupingList,
        aggregateFunctionList,
        aggFilterList,
        context,
        operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeAggregateRel(
        input,
        groupingList,
        aggregateFunctionList,
        aggFilterList,
        extensionNode,
        context,
        operatorId)
    }
  }

  protected def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode
}
