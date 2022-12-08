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

import io.glutenproject.backendsapi.IValidatorApi
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.utils.CHExpressionUtil
import io.glutenproject.vectorized.CHNativeExpressionEvaluator

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

class CHValidatorApi extends IValidatorApi {
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
    if (CHExpressionUtil.CH_EXPR_BLACKLIST.isEmpty) return true
    val value = CHExpressionUtil.CH_EXPR_BLACKLIST.get(substraitExprName)
    if (value.isEmpty) {
      return true
    }
    val inputTypeNames = value.get
    inputTypeNames.foreach {
      inputTypeName =>
        if (inputTypeName.equals(CHExpressionUtil.EMPTY_TYPE)) {
          return false
        } else {
          for (input <- expr.children) {
            if (inputTypeName.equals(input.dataType.typeName)) {
              return false
            }
          }
        }
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
}
