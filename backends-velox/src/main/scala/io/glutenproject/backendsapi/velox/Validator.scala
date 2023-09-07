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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampType}

class Validator extends ValidatorApi {

  override def doExprValidate(substraitExprName: String, expr: Expression): Boolean =
    doExprValidate(Map(), substraitExprName, expr)

  override def doNativeValidateWithFailureReason(plan: PlanNode): NativePlanValidationInfo = {
    val validator = new NativePlanEvaluator()
    validator.doNativeValidateWithFailureReason(plan.toProtobuf.toByteArray)
  }

  override def doSparkPlanValidate(plan: SparkPlan): Boolean = true

  private def primitiveTypeValidate(dataType: DataType): Boolean = {
    dataType match {
      case _: BooleanType =>
      case _: ByteType =>
      case _: ShortType =>
      case _: IntegerType =>
      case _: LongType =>
      case _: FloatType =>
      case _: DoubleType =>
      case _: StringType =>
      case _: BinaryType =>
      case _: DecimalType =>
      case _: DateType =>
      case _: TimestampType =>
      case _ => return false
    }
    true
  }

  override def doSchemaValidate(schema: DataType): Boolean = {
    if (primitiveTypeValidate(schema)) {
      return true
    }
    schema match {
      case map: MapType =>
        return doSchemaValidate(map.keyType) && doSchemaValidate(map.valueType)
      case struct: StructType =>
        for (field <- struct.fields) {
          if (!doSchemaValidate(field.dataType)) {
            return false
          }
        }
      case array: ArrayType =>
        return doSchemaValidate(array.elementType)
      case _ => return false
    }
    true
  }
}
