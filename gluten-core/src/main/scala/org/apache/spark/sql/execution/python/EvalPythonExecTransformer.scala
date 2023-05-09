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
package org.apache.spark.api.python

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.TransformContext
import io.glutenproject.execution.TransformSupport
import io.glutenproject.expression._
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`._
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression._
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.EvalPythonExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.collect.Lists
import com.google.protobuf.Any

import java.util.ArrayList

case class EvalPythonExecTransformer(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan)
  extends EvalPythonExec
  with TransformSupport
  with GlutenPlan {

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetricsUpdater(metrics)

  override protected def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {
    throw new NotImplementedError("EvalPythonExecTransformer doesn't support evaluate")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"EvalPythonExecTransformer doesn't support doExecute")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): EvalPythonExecTransformer =
    copy(udfs, resultAttrs, newChild)

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = child match {
    case c: TransformSupport =>
      val childPlans = c.getBuildPlans
      childPlans :+ (this, null)
    case _ =>
      Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = child match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def supportsColumnar: Boolean = true

  override def doValidateInternal(): Boolean = {
    /// All udfs should be scalar python udf
    for (udf <- udfs) {
      if (!PythonUDF.isScalarPythonUDF(udf)) {
        logWarning(
          s"Validation failed for ${this.getClass.toString} because $udf is not scalar python udf")
        return false
      }
    }

    val context = new SubstraitContext
    val args = context.registeredFunction
    val operatorId = context.nextOperatorId

    val expressionNodes = new java.util.ArrayList[ExpressionNode]
    child.output.zipWithIndex.foreach(
      x => expressionNodes.add(ExpressionBuilder.makeSelection(x._2)))
    udfs.foreach(
      udf => {
        expressionNodes.add(
          ExpressionConverter.replaceWithExpressionTransformer(udf, child.output).doTransform(args))
      })

    val relNode =
      try {
        RelBuilder.makeProjectRel(null, expressionNodes, context, operatorId)
      } catch {
        case e: Throwable =>
          logValidateFailure(
            s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}",
            e)
          return false
      }

    if (relNode != null && GlutenConfig.getConf.enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(context, Lists.newArrayList(relNode))
      BackendsApiManager.getValidatorApiInstance.doValidate(planNode)
    } else {
      true
    }
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport => c.doTransform(context)
      case _ => null
    }

    val args = context.registeredFunction
    val operatorId = context.nextOperatorId
    val expressionNodes = new java.util.ArrayList[ExpressionNode]
    child.output.zipWithIndex.foreach(
      x => expressionNodes.add(ExpressionBuilder.makeSelection(x._2)))
    udfs.foreach(
      udf => {
        expressionNodes.add(
          ExpressionConverter.replaceWithExpressionTransformer(udf, child.output).doTransform(args))
      })

    val relNode = if (childCtx != null) {
      getRelNode(childCtx.root, expressionNodes, context, operatorId, child.output, false)
    } else {
      val attrList = new java.util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)
      getRelNode(readRel, expressionNodes, context, operatorId, child.output, false)
    }

    TransformContext(child.output, output, relNode)
  }

  def getRelNode(
      input: RelNode,
      expressionNodes: ArrayList[ExpressionNode],
      context: SubstraitContext,
      operatorId: Long,
      inputAttributes: Seq[Attribute],
      validation: Boolean): RelNode = {
    if (!validation) {
      RelBuilder.makeProjectRel(input, expressionNodes, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- inputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(input, expressionNodes, extensionNode, context, operatorId, -1)
    }
  }
}
