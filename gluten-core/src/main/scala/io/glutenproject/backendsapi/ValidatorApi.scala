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

import io.glutenproject.expression.{ExpressionMappings, ExpressionNames}
import io.glutenproject.extension.ValidationResult
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.validate.NativePlanValidationInfo

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Generator}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType

object TypeKey {
  final val EMPTY_TYPE = ""
  final val ARRAY_TYPE = "array"
  final val MAP_TYPE = "map"
  final val STRUCT_TYPE = "struct"
}

/**
 * Determine if a plan or expression can be accepted by the backend, or we fallback the execution to
 * vanilla Spark.
 */
trait ValidatorApi {

  /**
   * Validate target expression within an input blacklist. Return false if target expression (with
   * the information of its args' types) matches any of the entry in the blacklist.
   */
  protected def doExprValidate(
      blacklist: Map[String, Set[String]],
      substraitExprName: String,
      expr: Expression): Boolean = {
    // To handle cast(struct as string) AS col_name expression
    val key = if (substraitExprName.toLowerCase().equals(ExpressionNames.ALIAS)) {
      ExpressionMappings.expressionsMap.get(expr.asInstanceOf[Alias].child.getClass)
    } else Some(substraitExprName)
    if (key.isEmpty) return false
    if (blacklist.isEmpty) return true
    val value = blacklist.get(key.get)
    if (value.isEmpty) {
      return true
    }
    val inputTypeNames = value.get
    inputTypeNames.foreach {
      inputTypeName =>
        if (inputTypeName.equals(TypeKey.EMPTY_TYPE)) {
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
   * Validate expression for specific backend, including input type. If the expression isn't
   * implemented by the backend or it returns mismatched results with Vanilla Spark, it will fall
   * back to Vanilla Spark.
   *
   * @return
   *   true by default
   */
  def doExprValidate(substraitExprName: String, expr: Expression): Boolean = true

  /** Validate against a whole Spark plan, before being interpreted by Gluten. */
  def doSparkPlanValidate(plan: SparkPlan): Boolean

  /** Validate against Substrait plan node in native backend. */
  def doNativeValidateWithFailureReason(plan: PlanNode): NativePlanValidationInfo

  /** Validate against Compression method, such as bzip2. */
  def doCompressionSplittableValidate(compressionMethod: String): Boolean = false

  /**
   * Validate the input schema. Transformers like UnionExecTransformer that do not generate
   * Substrait plan need to validate the input schema and fall back if there are any unsupported
   * types.
   *
   * @return
   *   true by default
   */
  def doSchemaValidate(schema: DataType): Boolean = true

  /** Validate against ColumnarShuffleExchangeExec. */
  def doColumnarShuffleExchangeExecValidate(
      outputPartitioning: Partitioning,
      child: SparkPlan): Boolean

  /** Validate against Generator expression. */
  def doGeneratorValidate(generator: Generator, outer: Boolean): ValidationResult =
    ValidationResult.ok
}
