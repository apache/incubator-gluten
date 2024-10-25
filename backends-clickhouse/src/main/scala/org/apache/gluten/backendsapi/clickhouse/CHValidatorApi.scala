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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.{BackendsApiManager, ValidatorApi}
import org.apache.gluten.expression.ExpressionConverter
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.SelectionNode
import org.apache.gluten.substrait.plan.PlanNode
import org.apache.gluten.utils.CHExpressionUtil
import org.apache.gluten.vectorized.CHNativeExpressionEvaluator

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.utils.RangePartitionerBoundsGenerator
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class CHValidatorApi extends ValidatorApi with AdaptiveSparkPlanHelper with Logging {

  override def doNativeValidateWithFailureReason(plan: PlanNode): ValidationResult = {
    if (CHNativeExpressionEvaluator.doValidate(plan.toProtobuf.toByteArray)) {
      ValidationResult.succeeded
    } else {
      ValidationResult.failed("CH native check failed.")
    }
  }

  /**
   * Validate expression for specific backend, including input type. If the expression isn't
   * implemented by the backend or it returns mismatched results with Vanilla Spark, it will fall
   * back to Vanilla Spark.
   *
   * @return
   *   true by default
   */
  override def doExprValidate(substraitExprName: String, expr: Expression): Boolean = {
    CHExpressionUtil.CH_BLACKLIST_SCALAR_FUNCTION.get(substraitExprName) match {
      case Some(validator) =>
        return validator.doValidate(expr)
      case _ =>
    }
    CHExpressionUtil.CH_AGGREGATE_FUNC_BLACKLIST.get(substraitExprName) match {
      case Some(validator) =>
        return validator.doValidate(expr)
      case _ =>
    }
    true
  }

  /** Validate whether the compression method support splittable at clickhouse backend. */
  override def doCompressionSplittableValidate(compressionMethod: String): Boolean = {
    compressionMethod == "BZip2Codec"
  }

  override def doColumnarShuffleExchangeExecValidate(
      outputPartitioning: Partitioning,
      child: SparkPlan): Option[String] = {
    val outputAttributes = child.output
    // check repartition expression
    val substraitContext = new SubstraitContext
    outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        val allSelectionNodes = exprs.forall {
          expr =>
            val node = ExpressionConverter
              .replaceWithExpressionTransformer(expr, outputAttributes)
              .doTransform(substraitContext.registeredFunction)
            node.isInstanceOf[SelectionNode]
        }
        if (
          allSelectionNodes ||
          BackendsApiManager.getSettings.supportShuffleWithProject(outputPartitioning, child)
        ) {
          None
        } else {
          Some("expressions are not supported in HashPartitioning")
        }
      case rangePartitoning: RangePartitioning =>
        if (
          GlutenConfig.getConf.enableColumnarSort &&
          RangePartitionerBoundsGenerator.supportedOrderings(rangePartitoning, child)
        ) {
          None
        } else {
          Some("do not support range partitioning columnar sort")
        }
      case _ => None
    }
  }
}
