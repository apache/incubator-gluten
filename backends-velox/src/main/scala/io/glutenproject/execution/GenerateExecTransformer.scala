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
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, ExpressionNames}
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.{GenerateMetricsUpdater, MetricsUpdater}
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StructType}

import com.google.common.collect.Lists
import com.google.protobuf.StringValue

import scala.collection.JavaConverters._

case class GenerateExecTransformer(
    generator: Generator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    child: SparkPlan)
  extends GenerateExecTransformerBase(
    generator,
    requiredChildOutput,
    outer,
    generatorOutput,
    child) {

  @transient
  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def metricsUpdater(): MetricsUpdater = new GenerateMetricsUpdater(metrics)

  override protected def withNewChildInternal(newChild: SparkPlan): GenerateExecTransformer =
    copy(generator, requiredChildOutput, outer, generatorOutput, newChild)

  override protected def doGeneratorValidate(
      generator: Generator,
      outer: Boolean): ValidationResult = {
    if (outer) {
      return ValidationResult.notOk(s"Velox backend does not support outer")
    }
    generator match {
      case _: JsonTuple =>
        ValidationResult.notOk(s"Velox backend does not support this json_tuple")
      case _: ExplodeBase =>
        ValidationResult.ok
      case Inline(child) =>
        child match {
          case AttributeReference(_, ArrayType(_: StructType, _), _, _) =>
            ValidationResult.ok
          case _ =>
            // TODO: Support Literal/CreateArray.
            ValidationResult.notOk(
              s"Velox backend does not support inline with expression " +
                s"${child.getClass.getSimpleName}.")
        }
      case _ =>
        ValidationResult.ok
    }
  }

  override protected def getRelNode(
      context: SubstraitContext,
      inputRel: RelNode,
      generatorNode: ExpressionNode,
      validation: Boolean): RelNode = {
    val operatorId = context.nextOperatorId(this.nodeName)

    val newInput = if (!validation) {
      applyPreProject(inputRel, context, operatorId)
    } else {
      // No need to validate the pre-projection. The generator output has been validated in
      // doGeneratorValidate.
      inputRel
    }

    val generateRel = RelBuilder.makeGenerateRel(
      newInput,
      generatorNode,
      requiredChildOutputNodes.asJava,
      getExtensionNode(validation),
      context,
      operatorId)

    if (!validation) {
      applyPostProject(generateRel, context, operatorId)
    } else {
      // No need to validate the post-projection on the native side as
      // it only flattens the generator's output.
      generateRel
    }
  }

  private def getExtensionNode(validation: Boolean): AdvancedExtensionNode = {
    if (!validation) {
      // Start with "GenerateParameters:"
      val parametersStr = new StringBuffer("GenerateParameters:")
      // isPosExplode: 1 for PosExplode, 0 for others.
      val isPosExplode = if (generator.isInstanceOf[PosExplode]) {
        "1"
      } else {
        "0"
      }
      parametersStr
        .append("isPosExplode=")
        .append(isPosExplode)
        .append("\n")
      val message = StringValue
        .newBuilder()
        .setValue(parametersStr.toString)
        .build()
      val optimization = BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
      ExtensionBuilder.makeAdvancedExtension(optimization, null)
    } else {
      getExtensionNodeForValidation
    }
  }

  // Select child outputs and append generator input.
  private def applyPreProject(
      inputRel: RelNode,
      context: SubstraitContext,
      operatorId: Long
  ): RelNode = {
    val projectExpressions: Seq[ExpressionNode] =
      child.output.indices
        .map(ExpressionBuilder.makeSelection(_)) :+
        ExpressionConverter
          .replaceWithExpressionTransformer(
            generator.asInstanceOf[UnaryExpression].child,
            child.output)
          .doTransform(context.registeredFunction)

    RelBuilder.makeProjectRel(
      inputRel,
      projectExpressions.asJava,
      context,
      operatorId,
      child.output.size)
  }

  // There are 3 types of CollectionGenerator in spark: Explode, PosExplode and Inline.
  // Adds postProject for PosExplode and Inline.
  private def applyPostProject(
      generateRel: RelNode,
      context: SubstraitContext,
      operatorId: Long): RelNode = {
    generator match {
      case Inline(_) =>
        val requiredOutput = requiredChildOutputNodes.indices.map {
          ExpressionBuilder.makeSelection(_)
        }
        val flattenStruct: Seq[ExpressionNode] = generatorOutput.indices.map {
          i =>
            val selectionNode = ExpressionBuilder.makeSelection(requiredOutput.size)
            selectionNode.addNestedChildIdx(i)
        }
        RelBuilder.makeProjectRel(
          generateRel,
          (requiredOutput ++ flattenStruct).asJava,
          context,
          operatorId,
          1 + requiredOutput.size // 1 stands for the inner struct field from array.
        )
      case PosExplode(posExplodeChild) =>
        // Ordinal populated by Velox UnnestNode starts with 1.
        // Need to substract 1 to align with Spark's output.
        val unnestedSize = posExplodeChild.dataType match {
          case _: MapType => 2
          case _: ArrayType => 1
        }
        val subFunctionName = ConverterUtils.makeFuncName(
          ExpressionNames.SUBTRACT,
          Seq(LongType, LongType),
          FunctionConfig.OPT)
        val functionMap = context.registeredFunction
        val addFunctionId = ExpressionBuilder.newScalarFunction(functionMap, subFunctionName)
        val literalNode = ExpressionBuilder.makeLiteral(1L, LongType, false)
        val ordinalNode = ExpressionBuilder.makeCast(
          TypeBuilder.makeI32(false),
          ExpressionBuilder.makeScalarFunction(
            addFunctionId,
            Lists.newArrayList(
              ExpressionBuilder.makeSelection(requiredChildOutputNodes.size + unnestedSize),
              literalNode),
            ConverterUtils.getTypeNode(LongType, generator.elementSchema.head.nullable)
          ),
          true // Generated ordinal column shouldn't have null.
        )
        val requiredChildNodes =
          requiredChildOutputNodes.indices.map(ExpressionBuilder.makeSelection(_))
        val unnestColumns = (0 until unnestedSize)
          .map(i => ExpressionBuilder.makeSelection(i + requiredChildOutputNodes.size))
        val generatorOutput: Seq[ExpressionNode] =
          (requiredChildNodes :+ ordinalNode) ++ unnestColumns
        RelBuilder.makeProjectRel(
          generateRel,
          generatorOutput.asJava,
          context,
          operatorId,
          generatorOutput.size
        )
      case _ => generateRel
    }
  }
}
