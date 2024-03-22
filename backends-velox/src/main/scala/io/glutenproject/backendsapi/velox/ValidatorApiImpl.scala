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
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.validate.NativePlanValidationInfo
import io.glutenproject.vectorized.NativePlanEvaluator

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.util.TaskResources

class ValidatorApiImpl extends ValidatorApi {

  /** For velox backend, key validation is on native side. */
  override def doExprValidate(substraitExprName: String, expr: Expression): Boolean =
    true

  override def doNativeValidateWithFailureReason(plan: PlanNode): NativePlanValidationInfo = {
    TaskResources.runUnsafe {
      val validator = NativePlanEvaluator.create()
      validator.doNativeValidateWithFailureReason(plan.toProtobuf.toByteArray)
    }
  }

  override def doSparkPlanValidate(plan: SparkPlan): Boolean = true

  private def isPrimitiveType(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          StringType | BinaryType | _: DecimalType | DateType | TimestampType | NullType =>
        true
      case _ => false
    }
  }

  override def doSchemaValidate(schema: DataType): Option[String] = {
    if (isPrimitiveType(schema)) {
      return None
    }
    schema match {
      case map: MapType =>
        doSchemaValidate(map.keyType).orElse(doSchemaValidate(map.valueType))
      case struct: StructType =>
        struct.fields.foreach {
          f =>
            val reason = doSchemaValidate(f.dataType)
            if (reason.isDefined) {
              return reason
            }
        }
        None
      case array: ArrayType =>
        doSchemaValidate(array.elementType)
      case _ =>
        Some(s"Schema / data type not supported: $schema")
    }
  }

  override def doColumnarShuffleExchangeExecValidate(
      outputPartitioning: Partitioning,
      child: SparkPlan): Option[String] = {
    doSchemaValidate(child.schema)
  }
}
