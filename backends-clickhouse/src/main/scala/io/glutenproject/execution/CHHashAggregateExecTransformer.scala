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

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.GlutenConfig
import io.glutenproject.expression._
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{LocalFilesBuilder, RelBuilder, RelNode}
import io.glutenproject.vectorized.ExpressionEvaluator
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CHHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan) extends HashAggregateExecBaseTransformer(
    requiredChildDistributionExpressions,
    groupingExpressions,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }
    val (relNode, inputAttributes, outputAttributes) = if (childCtx != null) {
      (
        getAggRel(context.registeredFunction, childCtx.root),
        childCtx.outputAttributes,
        aggregateResultAttributes)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      // Notes: Currently, ClickHouse backend uses the output attributes of
      // aggregateResultAttributes as Shuffle output,
      // which is different from the Velox and Gazelle.
      val typeList = new util.ArrayList[TypeNode]()
      val nameList = new util.ArrayList[String]()
      for (attr <- aggregateResultAttributes) {
        typeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        nameList.add(ConverterUtils.genColumnNameWithExprId(attr))
      }
      // The iterator index will be added in the path of LocalFiles.
      val inputIter = LocalFilesBuilder.makeLocalFiles(
        ConverterUtils.ITERATOR_PREFIX.concat(context.nextIteratorIndex.toString))
      context.setLocalFilesNode(inputIter)
      val readRel = RelBuilder.makeReadRel(typeList, nameList, null, context)

      (getAggRel(context.registeredFunction, readRel), aggregateResultAttributes, output)
    }
    TransformContext(inputAttributes, outputAttributes, relNode)
  }

  override def getAggRelWithoutPreProjection(args: java.lang.Object,
                                             originalInputAttributes: Seq[Attribute],
                                             input: RelNode = null,
                                             validation: Boolean): RelNode = {
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
              .replaceWithExpressionTransformer(expr, child.output)
            aggExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
          })
        case Final =>
          val aggTypesExpr: Expression = ExpressionConverter
            .replaceWithExpressionTransformer(aggExpr.resultAttribute, originalInputAttributes)
          Seq(aggTypesExpr.asInstanceOf[ExpressionTransformer].doTransform(args))
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
        ConverterUtils.getTypeNode(aggregateFunc.dataType, aggregateFunc.nullable))
      aggregateFunctionList.add(aggFunctionNode)
    })
    if (!validation) {
      RelBuilder.makeAggregateRel(input, groupingList, aggregateFunctionList)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(inputTypeNodeList).toProtobuf))
      RelBuilder.makeAggregateRel(input, groupingList, aggregateFunctionList, extensionNode)
    }
  }

  override def getAggRel(args: java.lang.Object,
                         input: RelNode = null,
                         validation: Boolean = false): RelNode = {
    val originalInputAttributes = child.output
    val aggRel = if (needsPreProjection) {
      getAggRelWithPreProjection(args, originalInputAttributes, input, validation)
    } else {
      getAggRelWithoutPreProjection(args, aggregateResultAttributes, input, validation)
    }
    applyPostProjection(args, aggRel, validation)
  }
}
