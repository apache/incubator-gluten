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
import io.glutenproject.exception.GlutenException
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan

import com.google.protobuf.Any

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._

// Transformer for GeneratorExec, which Applies a [[Generator]] to a stream of input rows.
// For clickhouse backend, it will transform Spark explode lateral view to CH array join.
case class GenerateExecTransformer(
    generator: Generator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    child: SparkPlan)
  extends UnaryTransformSupport {

  @transient
  override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genGenerateTransformerMetrics(sparkContext)

  override def output: Seq[Attribute] = requiredChildOutput ++ generatorOutput

  override def producedAttributes: AttributeSet = AttributeSet(generatorOutput)

  override protected def withNewChildInternal(newChild: SparkPlan): GenerateExecTransformer =
    copy(generator, requiredChildOutput, outer, generatorOutput, newChild)

  override protected def doValidateInternal(): ValidationResult = {
    val validationResult =
      BackendsApiManager.getTransformerApiInstance.validateGenerator(generator, outer)
    if (!validationResult.isValid) {
      return validationResult
    }
    val context = new SubstraitContext
    val args = context.registeredFunction

    val operatorId = context.nextOperatorId(this.nodeName)
    val generatorExpr =
      ExpressionConverter.replaceWithExpressionTransformer(generator, child.output)
    val generatorNode = generatorExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
    val childOutputNodes = new java.util.ArrayList[ExpressionNode]
    for (target <- requiredChildOutput) {
      val found = child.output.zipWithIndex.filter(_._1.name == target.name)
      if (found.nonEmpty) {
        val exprNode = ExpressionBuilder.makeSelection(found.head._2)
        childOutputNodes.add(exprNode)
      } else {
        throw new GlutenException(s"Can't found column ${target.name} in child output")
      }
    }

    val relNode =
      getRelNode(
        context,
        operatorId,
        child.output,
        null,
        generatorNode,
        childOutputNodes,
        validation = true)

    doNativeValidation(context, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport => c.doTransform(context)
      case _ => null
    }

    val args = context.registeredFunction
    val operatorId = context.nextOperatorId(this.nodeName)
    val generatorExpr =
      ExpressionConverter.replaceWithExpressionTransformer(generator, child.output)
    val generatorNode = generatorExpr.doTransform(args)
    val requiredChildOutputNodes = new JArrayList[ExpressionNode]
    for (target <- requiredChildOutput) {
      val found = child.output.zipWithIndex.filter(_._1.name == target.name)
      if (found.nonEmpty) {
        val exprNode = ExpressionBuilder.makeSelection(found.head._2)
        requiredChildOutputNodes.add(exprNode)
      } else {
        throw new GlutenException(s"Can't found column ${target.name} in child output")
      }
    }

    val inputRel = if (childCtx != null) {
      childCtx.root
    } else {
      val readRel = RelBuilder.makeReadRel(child.output.asJava, context, operatorId)
      readRel
    }
    val projRel =
      if (BackendsApiManager.getSettings.insertPostProjectForGenerate()) {
        // need to insert one projection node for velox backend
        val projectExpressions = new JArrayList[ExpressionNode]()
        val childOutputNodes = child.output.indices
          .map(i => ExpressionBuilder.makeSelection(i).asInstanceOf[ExpressionNode])
          .asJava
        projectExpressions.addAll(childOutputNodes)
        val projectExprNode = ExpressionConverter
          .replaceWithExpressionTransformer(generator.asInstanceOf[Explode].child, child.output)
          .doTransform(args)

        projectExpressions.add(projectExprNode)

        RelBuilder.makeProjectRel(
          inputRel,
          projectExpressions,
          context,
          operatorId,
          childOutputNodes.size)
      } else {
        inputRel
      }

    val relNode = getRelNode(
      context,
      operatorId,
      child.output,
      projRel,
      generatorNode,
      requiredChildOutputNodes,
      validation = false)

    TransformContext(child.output, output, relNode)
  }

  def getRelNode(
      context: SubstraitContext,
      operatorId: Long,
      inputAttributes: Seq[Attribute],
      input: RelNode,
      generator: ExpressionNode,
      childOutput: JList[ExpressionNode],
      validation: Boolean): RelNode = {
    if (!validation) {
      RelBuilder.makeGenerateRel(input, generator, childOutput, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList =
        inputAttributes.map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable)).asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeGenerateRel(input, generator, childOutput, extensionNode, context, operatorId)
    }
  }

  override def metricsUpdater(): MetricsUpdater = {
    BackendsApiManager.getMetricsApiInstance.genGenerateTransformerMetricsUpdater(metrics)
  }
}
