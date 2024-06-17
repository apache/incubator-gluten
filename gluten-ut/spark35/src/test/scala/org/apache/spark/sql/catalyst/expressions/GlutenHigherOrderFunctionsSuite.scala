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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, MapType, StringType}

class GlutenHigherOrderFunctionsSuite extends HigherOrderFunctionsSuite with GlutenTestsTrait {
  import org.apache.spark.sql.catalyst.dsl.expressions._
  testGluten("TransformKeys") {
    val ai0 = Literal.create(
      create_map(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4),
      MapType(IntegerType, IntegerType, valueContainsNull = false))
    val ai1 = Literal.create(
      Map.empty[Int, Int],
      MapType(IntegerType, IntegerType, valueContainsNull = true))
    val ai2 = Literal.create(
      create_map(1 -> 1, 2 -> null, 3 -> 3),
      MapType(IntegerType, IntegerType, valueContainsNull = true))
    val ai3 = Literal.create(null, MapType(IntegerType, IntegerType, valueContainsNull = false))

    val plusOne: (Expression, Expression) => Expression = (k, v) => k + 1
    val plusValue: (Expression, Expression) => Expression = (k, v) => k + v
    val modKey: (Expression, Expression) => Expression = (k, v) => k % 3

    checkEvaluation(transformKeys(ai0, plusOne), create_map(2 -> 1, 3 -> 2, 4 -> 3, 5 -> 4))
    checkEvaluation(transformKeys(ai0, plusValue), create_map(2 -> 1, 4 -> 2, 6 -> 3, 8 -> 4))
    checkEvaluation(
      transformKeys(transformKeys(ai0, plusOne), plusValue),
      create_map(3 -> 1, 5 -> 2, 7 -> 3, 9 -> 4))

    checkExceptionInExpression[SparkRuntimeException](
      transformKeys(ai0, modKey),
      """Duplicate map key"""
    )
    withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
      // Duplicated map keys will be removed w.r.t. the last wins policy.
      checkEvaluation(transformKeys(ai0, modKey), create_map(1 -> 4, 2 -> 2, 0 -> 3))
    }
    checkEvaluation(transformKeys(ai1, plusOne), Map.empty[Int, Int])
    checkEvaluation(transformKeys(ai1, plusOne), Map.empty[Int, Int])
    checkEvaluation(transformKeys(transformKeys(ai1, plusOne), plusValue), Map.empty[Int, Int])
    checkEvaluation(transformKeys(ai2, plusOne), create_map(2 -> 1, 3 -> null, 4 -> 3))
    checkEvaluation(
      transformKeys(transformKeys(ai2, plusOne), plusOne),
      create_map(3 -> 1, 4 -> null, 5 -> 3))
    checkEvaluation(transformKeys(ai3, plusOne), null)

    val as0 = Literal.create(
      create_map("a" -> "xy", "bb" -> "yz", "ccc" -> "zx"),
      MapType(StringType, StringType, valueContainsNull = false))
    val as1 = Literal.create(
      create_map("a" -> "xy", "bb" -> "yz", "ccc" -> null),
      MapType(StringType, StringType, valueContainsNull = true))
    val as2 = Literal.create(null, MapType(StringType, StringType, valueContainsNull = false))
    val as3 = Literal.create(
      Map.empty[StringType, StringType],
      MapType(StringType, StringType, valueContainsNull = true))

    val concatValue: (Expression, Expression) => Expression = (k, v) => Concat(Seq(k, v))
    val convertKeyToKeyLength: (Expression, Expression) => Expression =
      (k, v) => Length(k) + 1

    checkEvaluation(
      transformKeys(as0, concatValue),
      create_map("axy" -> "xy", "bbyz" -> "yz", "ccczx" -> "zx"))
    checkEvaluation(
      transformKeys(transformKeys(as0, concatValue), concatValue),
      create_map("axyxy" -> "xy", "bbyzyz" -> "yz", "ccczxzx" -> "zx"))
    checkEvaluation(transformKeys(as3, concatValue), Map.empty[String, String])
    checkEvaluation(
      transformKeys(transformKeys(as3, concatValue), convertKeyToKeyLength),
      Map.empty[Int, String])
    checkEvaluation(
      transformKeys(as0, convertKeyToKeyLength),
      create_map(2 -> "xy", 3 -> "yz", 4 -> "zx"))
    checkEvaluation(
      transformKeys(as1, convertKeyToKeyLength),
      create_map(2 -> "xy", 3 -> "yz", 4 -> null))
    checkEvaluation(transformKeys(as2, convertKeyToKeyLength), null)
    checkEvaluation(transformKeys(as3, convertKeyToKeyLength), Map.empty[Int, String])

    val ax0 = Literal.create(
      create_map(1 -> "x", 2 -> "y", 3 -> "z"),
      MapType(IntegerType, StringType, valueContainsNull = false))

    checkEvaluation(transformKeys(ax0, plusOne), create_map(2 -> "x", 3 -> "y", 4 -> "z"))

    // map key can't be map
    val makeMap: (Expression, Expression) => Expression = (k, v) => CreateMap(Seq(k, v))
    val map = transformKeys(ai0, makeMap)
    map.checkInputDataTypes() match {
      case TypeCheckResult.TypeCheckSuccess => fail("should not allow map as map key")
      case TypeCheckResult.DataTypeMismatch(errorSubClass, messageParameters) =>
        assert(errorSubClass == "INVALID_MAP_KEY_TYPE")
        assert(messageParameters === Map("keyType" -> "\"MAP<INT, INT>\""))
    }
  }
}
