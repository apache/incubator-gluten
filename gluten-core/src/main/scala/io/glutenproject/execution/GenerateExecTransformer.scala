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

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.rdd.RDD

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.expression.ExpressionConverter
import io.glutenproject.expression.ExpressionTransformer
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.substrait.rel.RelNode
import io.glutenproject.substrait.rel.RelBuilder
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.`type`.TypeNode
import io.glutenproject.substrait.expression.ExpressionBuilder

import java.util.ArrayList
import com.google.protobuf.Any
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.{MetricsUpdater, NoopMetricsUpdater}

// Transformer for GeneratorExec, which Applies a [[Generator]] to a stream of input rows.
// For clickhouse backend, it will transform Spark explode lateral view to CH array join.
case class GenerateExecTransformer(
  generator: Generator,
  requiredChildOutput: Seq[Attribute],
  outer: Boolean,
  generatorOutput: Seq[Attribute],
  child: SparkPlan)
  extends UnaryExecNode
  with TransformSupport {

  override def output: Seq[Attribute] = requiredChildOutput ++ generatorOutput

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"GenerateExecTransformer doesn't support doExecute")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): GenerateExecTransformer =
    copy(generator, requiredChildOutput, outer, generatorOutput, newChild)

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

  override def nodeName: String = "GenerateExec"

  override protected def doValidateInternal(): ValidationResult = {
    if (BackendsApiManager.veloxBackend) {
      return ValidationResult.notOk(s"Velox backend does not support this operator: ${nodeName}")
    }

    val context = new SubstraitContext
    val args = context.registeredFunction

    val operatorId = context.nextOperatorId(this.nodeName)
    val generatorExpr = ExpressionConverter.replaceWithExpressionTransformer(
      generator, child.output)
    val generatorNode = generatorExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
    val childOutputNodes = new java.util.ArrayList[ExpressionNode]
    for (target <- requiredChildOutput) {
      val found = child.output.zipWithIndex.filter(_._1.name == target.name)
      if (found.nonEmpty) {
        val exprNode = ExpressionBuilder.makeSelection(found(0)._2)
        childOutputNodes.add(exprNode)
      } else {
        throw new RuntimeException(s"Can't found column ${target.name} in child output")
      }
    }

    val relNode = getRelNode(context, operatorId, child.output, null, generatorNode,
      childOutputNodes, true)

    doNativeValidation(context, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport => c.doTransform(context)
      case _ => null
    }

    val args = context.registeredFunction
    val operatorId = context.nextOperatorId(this.nodeName)
    val generatorExpr = ExpressionConverter.replaceWithExpressionTransformer(
      generator, child.output)
    val generatorNode = generatorExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
    val childOutputNodes = new java.util.ArrayList[ExpressionNode]
    for (target <- requiredChildOutput) {
      val found = child.output.zipWithIndex.filter(_._1.name == target.name)
      if (found.nonEmpty) {
        val exprNode = ExpressionBuilder.makeSelection(found(0)._2)
        childOutputNodes.add(exprNode)
      } else {
        throw new RuntimeException(s"Can't found column ${target.name} in child output")
      }
    }

    val relNode = if (childCtx != null) {
      getRelNode(context, operatorId, child.output, childCtx.root, generatorNode,
        childOutputNodes, false)
    } else {
      val attrList = new java.util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)
      getRelNode(context, operatorId, child.output, readRel, generatorNode, childOutputNodes, false)
    }
    TransformContext(child.output, output, relNode)
  }

  def getRelNode(context: SubstraitContext,
    operatorId: Long,
    inputAttributes: Seq[Attribute],
    input: RelNode,
    generator: ExpressionNode,
    childOuput: ArrayList[ExpressionNode],
    validation: Boolean) : RelNode = {
    if (!validation) {
      RelBuilder.makeGenerateRel(input, generator, childOuput, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- inputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
        RelBuilder.makeGenerateRel(input, generator, childOuput, extensionNode,
          context, operatorId)
    }
  }

  override def metricsUpdater(): MetricsUpdater = new NoopMetricsUpdater
}
