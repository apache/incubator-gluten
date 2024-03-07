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
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter}
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan

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
      BackendsApiManager.getValidatorApiInstance.doGeneratorValidate(generator, outer)
    if (!validationResult.isValid) {
      return validationResult
    }
    val context = new SubstraitContext
    val args = context.registeredFunction

    val operatorId = context.nextOperatorId(this.nodeName)
    val generatorExpr =
      ExpressionConverter.replaceWithExpressionTransformer(generator, child.output)
    val generatorNode = generatorExpr.doTransform(args)
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
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)
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

    val inputRel = childCtx.root
    val projRel =
      if (BackendsApiManager.getSettings.insertPostProjectForGenerate()) {
        // need to insert one projection node for velox backend
        val projectExpressions = new JArrayList[ExpressionNode]()
        val childOutputNodes = child.output.indices
          .map(i => ExpressionBuilder.makeSelection(i).asInstanceOf[ExpressionNode])
          .asJava
        projectExpressions.addAll(childOutputNodes)
        val projectExprNode = ExpressionConverter
          .replaceWithExpressionTransformer(
            generator.asInstanceOf[UnaryExpression].child,
            child.output)
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
      generatorNode: ExpressionNode,
      childOutput: JList[ExpressionNode],
      validation: Boolean): RelNode = {
    val generateRel = if (!validation) {
      RelBuilder.makeGenerateRel(input, generatorNode, childOutput, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList =
        inputAttributes.map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable)).asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeGenerateRel(
        input,
        generatorNode,
        childOutput,
        extensionNode,
        context,
        operatorId)
    }
    applyPostProjectOnGenerator(generateRel, context, operatorId, childOutput, validation)
  }

  // There are 3 types of CollectionGenerator in spark: Explode, PosExplode and Inline.
  // Only Inline needs the post projection.
  private def applyPostProjectOnGenerator(
      generateRel: RelNode,
      context: SubstraitContext,
      operatorId: Long,
      childOutput: JList[ExpressionNode],
      validation: Boolean): RelNode = {
    generator match {
      case Inline(inlineChild) =>
        inlineChild match {
          case _: AttributeReference =>
          case _ =>
            throw new UnsupportedOperationException("Child of Inline is not AttributeReference.")
        }
        val requiredOutput = (0 until childOutput.size).map {
          ExpressionBuilder.makeSelection(_)
        }
        val flattenStruct: Seq[ExpressionNode] = generatorOutput.indices.map {
          i =>
            val selectionNode = ExpressionBuilder.makeSelection(requiredOutput.size)
            selectionNode.addNestedChildIdx(i)
        }
        val postProjectRel = RelBuilder.makeProjectRel(
          generateRel,
          (requiredOutput ++ flattenStruct).asJava,
          context,
          operatorId,
          1 + requiredOutput.size // 1 stands for the inner struct field from array.
        )
        if (validation) {
          // No need to validate the project rel on the native side as
          // it only flattens the generator's output.
          generateRel
        } else {
          postProjectRel
        }
      case _ => generateRel
    }
  }

  override def metricsUpdater(): MetricsUpdater = {
    BackendsApiManager.getMetricsApiInstance.genGenerateTransformerMetricsUpdater(metrics)
  }
}
