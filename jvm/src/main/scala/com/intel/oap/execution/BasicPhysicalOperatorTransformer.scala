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

import com.intel.oap.expression._
import com.intel.oap.substrait.expression.ExpressionNode
import com.intel.oap.substrait.rel.{RelBuilder, RelNode}
import com.intel.oap.substrait.SubstraitContext
import com.intel.oap.GazelleJniConfig
import org.apache.spark.SparkConf

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.util.StructTypeFWD
import org.apache.spark.sql.vectorized.ColumnarBatch

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

  override def supportsColumnar: Boolean = GazelleJniConfig.getConf.enableColumnarIterator

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

  override def doTransform(context: SubstraitContext): TransformContext = {
    val (childCtx, currRel) = child match {
      case c: TransformSupport =>
        val ctx = c.doTransform(context)
        (ctx, getRelNode(context.registeredFunction, ctx.root))
      case _ =>
        (null, getRelNode(context.registeredFunction, null))
    }
    if (currRel == null) {
      return childCtx
    }
    val inputAttributes = if (childCtx != null) {
      // Use the outputAttributes of child context as inputAttributes
      childCtx.outputAttributes
    } else {
      child.output
    }
    TransformContext(inputAttributes, output, currRel)
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
    val filterNode = if (condExpr != null) {
      val columnarCondExpr: Expression = ExpressionConverter
        .replaceWithExpressionTransformer(condExpr, attributeSeq = originalInputAttributes)
      val condExprNode =
        columnarCondExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
      RelBuilder.makeFilterRel(input, condExprNode)
    } else {
      null
    }
    if (projectList != null && projectList.nonEmpty) {
      val columnarProjExprs: Seq[Expression] = projectList.map(expr => {
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, attributeSeq = originalInputAttributes)
      })
      val projExprNodeList = new java.util.ArrayList[ExpressionNode]()
      for (expr <- columnarProjExprs) {
        projExprNodeList.add(expr.asInstanceOf[ExpressionTransformer].doTransform(args))
      }
      if (filterNode != null) {
        // The result of Filter will be the input of Project.
        RelBuilder.makeProjectRel(filterNode, projExprNodeList)
      } else {
        // The original input will be the input of Project.
        RelBuilder.makeProjectRel(input, projExprNodeList)
      }
    } else {
      filterNode
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

  override def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(s"This operator doesn't support doTransform.")
  }
}
