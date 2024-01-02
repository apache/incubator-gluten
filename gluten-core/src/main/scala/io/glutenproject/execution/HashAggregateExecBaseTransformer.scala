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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.{AggregationParams, SubstraitContext}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.types._

import com.google.protobuf.StringValue

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
  with UnaryTransformSupport
  with AliasHelper {

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genHashAggregateTransformerMetrics(sparkContext)

  // The direct outputs of Aggregation.
  lazy val allAggregateResultAttributes: List[Attribute] = {
    val groupingAttributes = groupingExpressions.map(
      expr => {
        ConverterUtils.getAttrFromExpr(expr).toAttribute
      })
    groupingAttributes.toList ::: getAttrForAggregateExprs(
      aggregateExpressions,
      aggregateAttributes)
  }

  protected def isCapableForStreamingAggregation: Boolean = {
    if (!conf.getConf(GlutenConfig.COLUMNAR_PREFER_STREAMING_AGGREGATE)) {
      return false
    }
    if (groupingExpressions.isEmpty) {
      return false
    }

    val childOrdering = child match {
      case agg: HashAggregateExecBaseTransformer
          if agg.groupingExpressions == this.groupingExpressions =>
        // If the child aggregate supports streaming aggregate then the ordering is not changed.
        // So we can propagate ordering if there is no shuffle exchange between aggregates and
        // they have same grouping keys,
        agg.child.outputOrdering
      case _ => child.outputOrdering
    }
    val requiredOrdering = groupingExpressions.map(expr => SortOrder.apply(expr, Ascending))
    SortOrder.orderingSatisfies(childOrdering, requiredOrdering)
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
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)

    val aggParams = new AggregationParams
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = getAggRel(context, operatorId, aggParams, childCtx.root)
    TransformContext(childCtx.outputAttributes, output, relNode)
  }

  // Members declared in org.apache.spark.sql.execution.AliasAwareOutputPartitioning
  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  // Check if Post-Projection is needed after the Aggregation.
  def needsPostProjection: Boolean = {
    // If the result expressions has different size with output attribute,
    // post-projection is needed.
    resultExpressions.size != allAggregateResultAttributes.size ||
    // Compare each item in result expressions and output attributes. Attribute in Alias
    // should be trimmed before checking.
    resultExpressions.map(trimAliases).zip(allAggregateResultAttributes).exists {
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

  protected def getAggRelInternal(
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

    val extensionNode = getAdvancedExtension(validation, originalInputAttributes)
    RelBuilder.makeAggregateRel(
      input,
      groupingList,
      aggregateFunctionList,
      aggFilterList,
      extensionNode,
      context,
      operatorId)
  }

  protected def getAdvancedExtension(
      validation: Boolean = false,
      originalInputAttributes: Seq[Attribute] = Seq.empty): AdvancedExtensionNode = {
    val enhancement = if (validation) {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf)
    } else {
      null
    }

    val optimization =
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        StringValue.newBuilder
          .setValue(formatExtOptimizationString(isCapableForStreamingAggregation))
          .build)
    ExtensionBuilder.makeAdvancedExtension(optimization, enhancement)
  }

  protected def formatExtOptimizationString(isStreaming: Boolean): String = {
    s"isStreaming=${if (isStreaming) "1" else "0"}\n"
  }

  protected def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode

  /**
   * Abstract classes do not support the use of the copy method, but case classes that inherit from
   * the abstract class can directly use the copy method generated by Scala. Here, an interface is
   * defined in the abstract class to call the copy method of the child case class.
   */
  def copySelf(
      requiredChildDistributionExpressions: Option[Seq[Expression]] =
        requiredChildDistributionExpressions,
      groupingExpressions: Seq[NamedExpression] = groupingExpressions,
      aggregateExpressions: Seq[AggregateExpression] = aggregateExpressions,
      aggregateAttributes: Seq[Attribute] = aggregateAttributes,
      initialInputBufferOffset: Int = initialInputBufferOffset,
      resultExpressions: Seq[NamedExpression] = resultExpressions,
      child: SparkPlan = child): HashAggregateExecBaseTransformer
}
