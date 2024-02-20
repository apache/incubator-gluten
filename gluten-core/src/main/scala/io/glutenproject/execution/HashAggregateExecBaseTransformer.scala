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
      case BooleanType | StringType | TimestampType | DateType | BinaryType =>
        true
      case _: NumericType => true
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

    val unsupportedAggExprs = aggregateAttributes.filterNot(attr => checkType(attr.dataType))
    if (unsupportedAggExprs.nonEmpty) {
      return ValidationResult.notOk(
        "Found unsupported data type in aggregation expression: " +
          unsupportedAggExprs
            .map(attr => s"${attr.name}#${attr.exprId.id}:${attr.dataType}")
            .mkString(", "))
    }
    val unsupportedGroupExprs = groupingExpressions.filterNot(attr => checkType(attr.dataType))
    if (unsupportedGroupExprs.nonEmpty) {
      return ValidationResult.notOk(
        "Found unsupported data type in grouping expression: " +
          unsupportedGroupExprs
            .map(attr => s"${attr.name}#${attr.exprId.id}:${attr.dataType}")
            .mkString(", "))
    }
    aggregateExpressions.foreach {
      expr =>
        if (!checkAggFuncModeSupport(expr.aggregateFunction, expr.mode)) {
          throw new UnsupportedOperationException(
            s"Unsupported aggregate mode: ${expr.mode} for ${expr.aggregateFunction.prettyName}")
        }
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

  protected def checkAggFuncModeSupport(
      aggFunc: AggregateFunction,
      mode: AggregateMode): Boolean = {
    aggFunc match {
      case s: Sum if s.prettyName.equals("try_sum") => false
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
            aggregateFunc.inputAggBufferAttributes.toList.map {
              attr =>
                val sameAttr = originalInputAttributes.find(_.exprId == attr.exprId)
                val rewriteAttr = if (sameAttr.isEmpty) {
                  // When aggregateExpressions includes subquery, Spark's PlanAdaptiveSubqueries
                  // Rule will transform the subquery within the final agg. The aggregateFunction
                  // in the aggregateExpressions of the final aggregation will be cloned, resulting
                  // in creating new aggregateFunction object. The inputAggBufferAttributes will
                  // also generate new AttributeReference instances with larger exprId, which leads
                  // to a failure in binding with the output of the partial agg. We need to adapt
                  // to this situation; when encountering a failure to bind, it is necessary to
                  // allow the binding of inputAggBufferAttribute with the same name but different
                  // exprId.
                  val attrsWithSameName =
                    originalInputAttributes.drop(groupingExpressions.size).collect {
                      case a if a.name == attr.name => a
                    }
                  val aggBufferAttrsWithSameName = aggregateExpressions.toIndexedSeq
                    .flatMap(_.aggregateFunction.inputAggBufferAttributes)
                    .filter(_.name == attr.name)
                  assert(
                    attrsWithSameName.size == aggBufferAttrsWithSameName.size,
                    "The attribute with the same name in final agg inputAggBufferAttribute must" +
                      "have the same size of corresponding attributes in originalInputAttributes."
                  )
                  attrsWithSameName(aggBufferAttrsWithSameName.indexOf(attr))
                } else attr
                ExpressionConverter
                  .replaceWithExpressionTransformer(rewriteAttr, originalInputAttributes)
                  .doTransform(args)
            }
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
}

object HashAggregateExecTransformerUtil {
  // Return whether the outputs partial aggregation should be combined for Velox computing.
  // When the partial outputs are multiple-column, row construct is needed.
  def rowConstructNeeded(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    aggregateExpressions.exists {
      aggExpr =>
        aggExpr.mode match {
          case PartialMerge | Final =>
            aggExpr.aggregateFunction.inputAggBufferAttributes.size > 1
          case _ => false
        }
    }
  }
}

abstract class HashAggregateExecPullOutBaseHelper(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute]) {
  // The direct outputs of Aggregation.
  lazy val allAggregateResultAttributes: List[Attribute] =
    groupingExpressions.map(ConverterUtils.getAttrFromExpr(_)).toList :::
      getAttrForAggregateExprs

  /** This method calculates the output attributes of Aggregation. */
  protected def getAttrForAggregateExprs: List[Attribute]
}
