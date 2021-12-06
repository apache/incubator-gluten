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

package com.intel.oap.execution

import com.intel.oap.GazellePluginConfig
import com.intel.oap.expression._
import com.intel.oap.vectorized._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.util.ExecutorManager
import org.apache.spark.sql.util.StructTypeFWD
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import com.google.common.collect.Lists
import com.intel.oap.GazellePluginConfig
import com.intel.oap.substrait.expression.ExpressionNode
import com.intel.oap.substrait.rel.{RelBuilder, RelNode}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;

case class ConditionProjectExecTransformer(
    condition: Expression,
    projectList: Seq[NamedExpression],
    child: SparkPlan)
    extends UnaryExecNode
    with TransformSupport
    with PredicateHelper
    with AliasAwareOutputPartitioning
    with Logging {

  val sparkConf: SparkConf = sparkContext.getConf

  override def supportsColumnar: Boolean = true

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_condproject"))

  override def doValidate(): Boolean = {
    true
  }

  def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  override protected def outputExpressions: Seq[NamedExpression] =
    if (projectList != null) projectList else output

  val notNullAttributes = if (condition != null) {
    val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
      case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
      case _ => false
    }
    notNullPreds.flatMap(_.references).distinct.map(_.exprId)
  } else {
    null
  }
  override def output: Seq[Attribute] =
    if (projectList != null) {
      projectList.map(_.toAttribute)
    } else if (condition != null) {
      val res = child.output.map { a =>
        if (a.nullable && notNullAttributes.contains(a.exprId)) {
          a.withNullability(false)
        } else {
          a
        }
      }
      res
    } else {
      val res = child.output.map { a => a }
      res
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

  override def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {
    val numOutputRows = longMetric("numOutputRows")
    val procTime = longMetric("processTime")
    procTime.set(process_time / 1000000)
    numOutputRows += out_num_rows
  }

  override def getChild: SparkPlan = child

  // override def canEqual(that: Any): Boolean = false

  def getRelNode(args: java.lang.Object, childRel: RelNode): RelNode = {
    prepareCondProjectRel(args, condition, projectList, child.output, childRel)
  }

  override def doTransform(args: java.lang.Object): TransformContext = {
    val (childCtx, kernelFunction) = child match {
      case c: TransformSupport =>
        val ctx = c.doTransform(args)
        (ctx, getRelNode(args, ctx.root))
      case _ =>
        (null, getRelNode(args, null))
    }
    if (kernelFunction == null) {
      return childCtx
    }
    val inputAttributes = if (childCtx != null) {
      childCtx.inputAttributes
    } else {
      child.output
    }
    TransformContext(inputAttributes, output, kernelFunction)
  }

  override def doTransform(args: java.lang.Object,
                           index: java.lang.Integer,
                           paths: java.util.ArrayList[String],
                           starts: java.util.ArrayList[java.lang.Long],
                           lengths: java.util.ArrayList[java.lang.Long]): TransformContext = {
    val (childCtx, kernelFunction) = child match {
      case c: TransformSupport =>
        val ctx = c.doTransform(args, index, paths, starts, lengths)
        (ctx, getRelNode(args, ctx.root))
      case _ =>
        (null, getRelNode(args, null))
    }
    if (kernelFunction == null) {
      return childCtx
    }
    val inputAttributes = if (childCtx != null) {
      childCtx.inputAttributes
    } else {
      child.output
    }
    TransformContext(inputAttributes, output, kernelFunction)
  }

  protected override def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  def prepareCondProjectRel(args: java.lang.Object,
                            condExpr: Expression,
                            projectList: Seq[NamedExpression],
                            originalInputAttributes: Seq[Attribute],
                            input: RelNode): RelNode = {
    val typeNodes = ConverterUtils.getTypeNodeFromAttributes(originalInputAttributes)
    val filterNode = if (condExpr != null) {
      val columnarCondExpr: Expression = ExpressionConverter
        .replaceWithExpressionTransformer(condExpr, attributeSeq = originalInputAttributes)
      val condExprNode =
        columnarCondExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
      RelBuilder.makeFilterRel(input, condExprNode, typeNodes)
    } else {
      null
    }
    val projectRel = if (projectList != null && projectList.nonEmpty) {
      val columnarProjExprs: Seq[Expression] = projectList.map(expr => {
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, attributeSeq = originalInputAttributes)
      })
      val projExprNodeList = new java.util.ArrayList[ExpressionNode]()
      for (expr <- columnarProjExprs) {
        projExprNodeList.add(expr.asInstanceOf[ExpressionTransformer].doTransform(args))
      }
      if (filterNode != null) {
        // The result of Filter will be the input of Project
        RelBuilder.makeProjectRel(filterNode, projExprNodeList, typeNodes)
      } else {
        // The original input will be the input of Project.
        RelBuilder.makeProjectRel(input, projExprNodeList, typeNodes)
      }
    } else {
      null
    }
    if (projectRel == null) {
      filterNode
    } else {
      projectRel
    }
  }
}

case class UnionExecTransformer(children: Seq[SparkPlan]) extends SparkPlan with TransformSupport {
  override def supportsColumnar: Boolean = true
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(StructTypeFWD.merge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId,
          firstAttr.qualifier)
      }
    }
  }
  protected override def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    throw new UnsupportedOperationException(s"This operator doesn't support inputRDDs.")
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    throw new UnsupportedOperationException(s"This operator doesn't support getBuildPlans.")
  }

  override def getStreamedLeafPlan: SparkPlan = {
    throw new UnsupportedOperationException(s"This operator doesn't support getStreamedLeafPlan.")
  }

  override def getChild: SparkPlan = {
    throw new UnsupportedOperationException(s"This operator doesn't support getChild.")
  }

  override def doValidate(): Boolean = false

  override def doTransform(args: Object): TransformContext = {
    throw new UnsupportedOperationException(s"This operator doesn't support doTransform.")
  }

  override def doTransform(args: java.lang.Object,
                           index: java.lang.Integer,
                           paths: java.util.ArrayList[String],
                           starts: java.util.ArrayList[java.lang.Long],
                           lengths: java.util.ArrayList[java.lang.Long]): TransformContext = {
    throw new UnsupportedOperationException(s"This operator doesn't support doTransform.")
  }
}
