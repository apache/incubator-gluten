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
package io.glutenproject.backendsapi.clickhouse

import io.glutenproject.backendsapi.ValidatorApi
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.utils.CHExpressionUtil
import io.glutenproject.vectorized.CHNativeExpressionEvaluator

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.delta.DeltaLogFileIndex
import org.apache.spark.sql.execution.{CommandResultExec, FileSourceScanExec, RDDScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec

class CHValidatorApi extends ValidatorApi with AdaptiveSparkPlanHelper {
  override def doValidate(plan: PlanNode): Boolean = {
    val validator = new CHNativeExpressionEvaluator()
    validator.doValidate(plan.toProtobuf.toByteArray)
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
    CHExpressionUtil.CH_EXPR_BLACKLIST_TYPE_EXISTS.get(substraitExprName) match {
      case Some(unsupportedTypeSet) =>
        if (unsupportedTypeSet.contains(CHExpressionUtil.EMPTY_TYPE)) {
          return false
        }
        if (expr.children.map(_.dataType.typeName).exists(unsupportedTypeSet.contains)) {
          return false
        }
      case _ =>
    }

    CHExpressionUtil.CH_EXPR_BLACKLIST_TYPE_MATCH.get(substraitExprName) match {
      case Some(unsupportedTypeSeq) =>
        if (expr.children.map(_.dataType.typeName).equals(unsupportedTypeSeq)) {
          return false
        }
      case _ =>
    }

    true
  }

  /**
   * Validate aggregate function for specific backend. If the aggregate function isn't implemented
   * by the backend, it will fall back to Vanilla Spark.
   */
  override def doAggregateFunctionValidate(
      substraitFuncName: String,
      func: AggregateFunction): Boolean = {
    if (CHExpressionUtil.CH_AGGREGATE_FUNC_BLACKLIST.isEmpty) return true
    val value = CHExpressionUtil.CH_AGGREGATE_FUNC_BLACKLIST.get(substraitFuncName)
    if (value.isEmpty) {
      return true
    }
    val inputTypeNames = value.get
    inputTypeNames.foreach {
      inputTypeName =>
        if (inputTypeName.equals(CHExpressionUtil.EMPTY_TYPE)) {
          return false
        } else {
          for (input <- func.children) {
            if (inputTypeName.equals(input.dataType.typeName)) {
              return false
            }
          }
        }
    }
    true
  }

  /** Validate against a whole Spark plan, before being interpreted by Gluten. */
  override def doSparkPlanValidate(plan: SparkPlan): Boolean = {
    // TODO: Currently there are some fallback issues on CH backend when SparkPlan is
    // TODO: SerializeFromObjectExec, ObjectHashAggregateExec and V2CommandExec.
    // For example:
    //   val tookTimeArr = Array(12, 23, 56, 100, 500, 20)
    //   import spark.implicits._
    //   val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
    //   df.summary().show(100, false)

    def includedDeltaOperator(scanExec: FileSourceScanExec): Boolean = {
      scanExec.relation.location.isInstanceOf[DeltaLogFileIndex]
    }

    val includedUnsupportedPlans = collect(plan) {
      // case s: SerializeFromObjectExec => true
      // case d: DeserializeToObjectExec => true
      // case o: ObjectHashAggregateExec => true
      case rddScanExec: RDDScanExec if rddScanExec.nodeName.contains("Delta Table State") => true
      case f: FileSourceScanExec if includedDeltaOperator(f) => true
      case v2CommandExec: V2CommandExec => true
      case commandResultExec: CommandResultExec => true
    }

    !includedUnsupportedPlans.contains(true)
  }
}
