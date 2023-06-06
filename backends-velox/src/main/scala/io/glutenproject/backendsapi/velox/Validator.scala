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

package io.glutenproject.backendsapi.velox

import io.glutenproject.backendsapi.ValidatorApi
import io.glutenproject.execution.RowToColumnConverter
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.vectorized.NativePlanEvaluator

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType

class Validator extends ValidatorApi {

  override def doExprValidate(substraitExprName: String, expr: Expression): Boolean =
    doExprValidate(Map(), substraitExprName, expr)

  override def doValidate(plan: PlanNode): Boolean = {
    val validator = new NativePlanEvaluator()
    validator.doValidate(plan.toProtobuf.toByteArray)
  }

  override def doSparkPlanValidate(plan: SparkPlan): Boolean = true

  override def doSchemaValidate(schema: StructType): Boolean = {
    RowToColumnConverter.supportSchema(schema)
  }
}
