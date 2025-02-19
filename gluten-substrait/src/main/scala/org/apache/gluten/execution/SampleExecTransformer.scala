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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.ExpressionConverter
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, LessThan, Literal, Rand}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DoubleType

import scala.collection.JavaConverters._

/**
 * SampleExec supports two sampling methods: with replacement and without replacement. This
 * transformer currently supports only sampling without replacement. For sampling without
 * replacement, sampleExec uses `seed + partitionId` as the seed for each partition. The `upperBound
 * \- lowerBound` value is used as the fraction, and the XORShiftRandom number generator is
 * employed. Each row undergoes a Bernoulli trial, and if the generated random number falls within
 * the range [lowerBound, upperBound), the row is included; otherwise, it is skipped.
 *
 * This transformer converts sampleExec to a Substrait Filter relation, achieving a similar sampling
 * effect through the filter op with rand sampling expression. Specifically, the `upperBound -
 * lowerBound` value is used as the fraction, and the node be translated to `filter(rand(seed +
 * partitionId) < fraction)` for random sampling.
 */
case class SampleExecTransformer(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: SparkPlan)
  extends UnaryTransformSupport
  with Logging {
  def fraction: Double = upperBound - lowerBound

  def condition: Expression = {
    val randExpr: Expression = Rand(seed)
    val sampleRateExpr: Expression = Literal(fraction, DoubleType)
    LessThan(randExpr, sampleRateExpr)
  }

  override def output: Seq[Attribute] = child.output

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genSampleTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genSampleTransformerMetricsUpdater(metrics)

  def getRelNode(
      context: SubstraitContext,
      condExpr: Expression,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    assert(condExpr != null)
    val condExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(condExpr, originalInputAttributes)
      .doTransform(context)
    RelBuilder.makeFilterRel(
      context,
      condExprNode,
      originalInputAttributes.asJava,
      operatorId,
      input,
      validation
    )
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (withReplacement) {
      return ValidationResult.failed(
        "Unsupported sample exec in native with " +
          s"withReplacement parameter is $withReplacement")
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId((this.nodeName))
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val relNode =
      getRelNode(substraitContext, condition, child.output, operatorId, null, validation = true)
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val currRel =
      getRelNode(context, condition, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Filter rel should be valid.")
    TransformContext(output, currRel)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SampleExecTransformer =
    copy(child = newChild)
}
