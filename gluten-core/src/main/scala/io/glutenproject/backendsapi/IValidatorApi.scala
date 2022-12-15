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

package io.glutenproject.backendsapi

import io.glutenproject.substrait.plan.PlanNode
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.types.StructType

/**
 * Determine if a plan or expression can be accepted by the backend, or we fallback
 * the execution to vanilla Spark.
 */
trait IValidatorApi {

  /**
   * Validate expression for specific backend, including input type.
   * If the expression isn't implemented by the backend or
   * it returns mismatched results with Vanilla Spark,
   * it will fall back to Vanilla Spark.
   *
   * @return true by default
   */
  def doExprValidate(substraitExprName: String, expr: Expression): Boolean = true

  /**
   * Validate aggregate function for specific backend.
   * If the aggregate function isn't implemented by the backend,
   * it will fall back to Vanilla Spark.
   */
  def doAggregateFunctionValidate(
      substraitFuncName: String,
      func: AggregateFunction): Boolean = true

  def doValidate(plan: PlanNode): Boolean

  /**
   * Validate the input schema.
   * Transformers like UnionExecTransformer that do not generate Substrait plan
   * need to validate the input schema and fall back if there are any unsupported types.
   *
   * @return true by default
   */
  def doValidateSchema(schema: StructType): Boolean = true
}
