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
import io.glutenproject.execution.CHHashAggregateExecTransformer.getAggregateResultAttributes
import io.glutenproject.expression._
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.{AggregationParams, SubstraitContext}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{LocalFilesBuilder, RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.types.DataType

import com.google.protobuf.Any

import java.util
import java.util.Locale

object CHHashAggregateExecTransformer {
  def getAggregateResultAttributes(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression]): Seq[Attribute] = {
    val groupingAttributes = groupingExpressions.map(
      expr => {
        ConverterUtils.getAttrFromExpr(expr).toAttribute
      })
    groupingAttributes ++ aggregateExpressions.map(
      expr => {
        expr.resultAttribute
      })
  }
}

case class CHHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  lazy val aggregateResultAttributes =
    getAggregateResultAttributes(groupingExpressions, aggregateExpressions)

  
  override def output: Seq[Attribute] = {
    lazy val originalOutput = resultExpressions.map(_.toAttribute)
    val distinctAggregateFunctionModes = aggregateExpressions.map(_.mode).distinct

    // The output result schema of final stage and partial stage are different.
    // Final stage: the output result schema is the same as the select clause.
    // Partial stage: the output result schema is, the grouping keys + the aggregate expressions.
    // For example, "select avg(n_nationkey), n_regionkey, sum(n_nationkey ) from nation group by
    // n_regionkey" .the output result schema of final stage is: avg(n_nationkey), n_regionkey,
    // sum(n_nationkey ), but the output result schema of partial stage is: n_regionkey,
    // partial_avg(n_nationkey), partial_sum(n_nationkey)
    //
    // How to know whether it is final stage or partial stage?
    // 1. If the aggregateExpressions contains Final mode, it is final stage.
    // 2. If the aggregateExpressions is empty, use the output as schema anyway.
    if (
      distinctAggregateFunctionModes.contains(Final) && (distinctAggregateFunctionModes.contains(
        Partial) ||
        distinctAggregateFunctionModes.contains(PartialMerge))
    ) {
      throw new IllegalStateException("Aggregate Final co-exists with Partial or PartialMerge")
    }

    if (distinctAggregateFunctionModes.contains(Final) || aggregateExpressions.isEmpty) {
      // Final aggregage stage
      originalOutput
    } else {
      // Partial aggregage stage
      //
      // add grouping keys
      var realOutput = Seq[Attribute]()
      groupingExpressions.foreach(
        expr => {
          realOutput = realOutput :+ ConverterUtils.getAttrFromExpr(expr).toAttribute
        })
      // add aggregate expressions
      // Special cases:
      // 1. avg, the partial result is two columns in spark, sum and count. but in clickhouse, it
      //    has only one column avg.
      aggregateExpressions.foreach(
        expr => {
          val attr = expr.resultAttribute
          expr.mode match {
            case Partial | PartialMerge =>
              val columnName = genPartialAggregateResultColumnName(attr)
              val (dataType, nullable) = getColumnType(attr, aggregateExpressions)
              val newAttr = AttributeReference(columnName, dataType, nullable)()
              realOutput = realOutput :+ newAttr
            case _ => realOutput = realOutput :+ attr
          }
        })
      realOutput
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
    val operatorId = context.nextOperatorId(this.nodeName)

    val (relNode, inputAttributes, outputAttributes) = if (childCtx != null) {
      // The final HashAggregateExecTransformer and partial HashAggregateExecTransformer
      // are in the one WholeStageTransformer.
      if (
        child.isInstanceOf[CHHashAggregateExecTransformer] &&
        childCtx.outputAttributes == aggregateResultAttributes
      ) {
        (
          getAggRel(context, operatorId, aggParams, childCtx.root),
          childCtx.outputAttributes,
          output)
      } else {
        (
          getAggRel(context, operatorId, aggParams, childCtx.root),
          childCtx.outputAttributes,
          aggregateResultAttributes)
      }
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      // Notes: Currently, ClickHouse backend uses the output attributes of
      // aggregateResultAttributes as Shuffle output,
      // which is different from Velox backend.
      // 1. When the child is file scan rdd ( in case of separating file scan )
      // 2. When the child is Union all operator
      val (inputAttrs, outputAttrs) =
        if (
          (child.find(_.isInstanceOf[Exchange]).isEmpty
            && child.find(_.isInstanceOf[QueryStageExec]).isEmpty)
          || (child.isInstanceOf[InputAdapter]
            && child.asInstanceOf[InputAdapter].child.isInstanceOf[UnionExecTransformer])
        ) {
          (child.output, aggregateResultAttributes)
        } else {
          (aggregateResultAttributes, output)
        }

      val attrList = new java.util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)

      (getAggRel(context, operatorId, aggParams, readRel), inputAttrs, outputAttrs)
    }
    TransformContext(inputAttributes, outputAttributes, relNode)
  }

  override def getAggRel(
      context: SubstraitContext,
      operatorId: Long,
      aggParams: AggregationParams,
      input: RelNode = null,
      validation: Boolean = false): RelNode = {
    val originalInputAttributes = child.output
    val aggRel = if (needsPreProjection) {
      aggParams.preProjectionNeeded = true
      getAggRelWithPreProjection(context, originalInputAttributes, operatorId, input, validation)
    } else {
      getAggRelWithoutPreProjection(
        context,
        aggregateResultAttributes,
        operatorId,
        input,
        validation)
    }
    // Will check if post-projection is needed. If yes, a ProjectRel will be added after the
    // AggregateRel.
    val resRel = if (!needsPostProjection(allAggregateResultAttributes)) {
      aggRel
    } else {
      aggParams.postProjectionNeeded = true
      applyPostProjection(context, aggRel, operatorId, validation)
    }
    context.registerAggregationParam(operatorId, aggParams)
    resRel
  }

  override def getAggRelWithoutPreProjection(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode = null,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    // Get the grouping nodes.
    val groupingList = new util.ArrayList[ExpressionNode]()
    groupingExpressions.foreach(
      expr => {
        // Use 'child.output' as based Seq[Attribute], the originalInputAttributes
        // may be different for each backend.
        val exprNode = ExpressionConverter
          .replaceWithExpressionTransformer(expr, child.output)
          .doTransform(args)
        groupingList.add(exprNode)
      })
    // Get the aggregate function nodes.
    val aggregateFunctionList = new util.ArrayList[AggregateFunctionNode]()

    val distinct_modes = aggregateExpressions.map(_.mode).distinct
    // quick check
    if (distinct_modes.contains(PartialMerge)) {
      if (distinct_modes.contains(Final)) {
        throw new IllegalStateException("PartialMerge co-exists with Final")
      }
    }

    val aggFilterList = new util.ArrayList[ExpressionNode]()
    aggregateExpressions.foreach(
      aggExpr => {
        if (aggExpr.filter.isDefined) {
          val exprNode = ExpressionConverter
            .replaceWithExpressionTransformer(aggExpr.filter.get, child.output)
            .doTransform(args)
          aggFilterList.add(exprNode)
        } else {
          aggFilterList.add(null)
        }

        val aggregateFunc = aggExpr.aggregateFunction
        val childrenNodeList = new util.ArrayList[ExpressionNode]()
        val childrenNodes = aggExpr.mode match {
          case Partial =>
            aggregateFunc.children.toList.map(
              expr => {
                ExpressionConverter
                  .replaceWithExpressionTransformer(expr, child.output)
                  .doTransform(args)
              })
          case PartialMerge if distinct_modes.contains(Partial) =>
            // this is the case where PartialMerge co-exists with Partial
            // so far, it only happens in a three-stage count distinct case
            // e.g. select sum(a), count(distinct b) from f
            if (!child.isInstanceOf[BaseAggregateExec]) {
              throw new UnsupportedOperationException(
                "PartialMerge's child not being HashAggregateExecBaseTransformer" +
                  " is unsupported yet")
            }
            val aggTypesExpr = ExpressionConverter
              .replaceWithExpressionTransformer(
                aggExpr.resultAttribute,
                CHHashAggregateExecTransformer.getAggregateResultAttributes(
                  child.asInstanceOf[BaseAggregateExec].groupingExpressions,
                  child.asInstanceOf[BaseAggregateExec].aggregateExpressions)
              )
            Seq(aggTypesExpr.doTransform(args))
          case Final | PartialMerge =>
            Seq(
              ExpressionConverter
                .replaceWithExpressionTransformer(aggExpr.resultAttribute, originalInputAttributes)
                .doTransform(args))
          case other =>
            throw new UnsupportedOperationException(s"$other not supported.")
        }
        for (node <- childrenNodes) {
          childrenNodeList.add(node)
        }
        val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
          AggregateFunctionsBuilder.create(args, aggregateFunc),
          childrenNodeList,
          modeToKeyWord(aggExpr.mode),
          ConverterUtils.getTypeNode(aggregateFunc.dataType, aggregateFunc.nullable)
        )
        aggregateFunctionList.add(aggFunctionNode)
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
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
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

  override def isStreaming: Boolean = false

  def numShufflePartitions: Option[Int] = Some(0)

  override protected def withNewChildInternal(
      newChild: SparkPlan): CHHashAggregateExecTransformer = {
    copy(child = newChild)
  }

  def getColumnType(
      attr: Attribute,
      aggregateExpressions: Seq[AggregateExpression]): (DataType, Boolean) = {
    val aggregateExpression = aggregateExpressions.find(_.resultAttribute == attr)
    val (dataType, nullable) = if (!aggregateExpression.isDefined) {
      (attr.dataType, attr.nullable)
    } else {
      aggregateExpression.get match {
        case aggExpr: AggregateExpression =>
          aggExpr.aggregateFunction match {
            case avg: Average =>
              (avg.child.dataType, attr.nullable)
            case collectList: CollectList =>
              // Be careful with the nullable. We must keep the nullable the same as the column
              // otherwise it will cause a parsing exception on partial aggregated data.
              (collectList.child.dataType, collectList.child.nullable)
            case _ => (attr.dataType, attr.nullable)
          }
        case _ => (attr.dataType, attr.nullable)
      }
    }
    (dataType, nullable)
  }

  def genPartialAggregateResultColumnName(attr: Attribute): String = {
    ConverterUtils.genColumnNameWithExprId(attr) + "#Partial#" +
      ConverterUtils.getShortAttributeName(attr)
  }
}
