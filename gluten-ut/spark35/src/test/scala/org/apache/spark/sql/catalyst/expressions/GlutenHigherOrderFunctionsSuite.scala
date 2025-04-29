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

import org.apache.spark.sql.{Column, DataFrame, GlutenTestsTrait, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, IntegerType, StringType, StructField, StructType}

class GlutenHigherOrderFunctionsSuite extends HigherOrderFunctionsSuite with GlutenTestsTrait {
  import org.apache.spark.sql.catalyst.dsl.expressions._

  // If LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC is false, the schema of literal exists
  // expression is not nullable. Even though the native engine output null, the actual result
  // is false. As a result, the `Array Exists` unit test in parent suite cannot detect the issue.
  testGluten("ArrayExists") {
    def createDF[T](arr: Seq[T], dataType: DataType): DataFrame = {
      _spark.createDataFrame(
        _spark.sparkContext.parallelize(Seq(Row(arr))),
        StructType(
          StructField("arr", ArrayType(dataType, containsNull = true), nullable = true) :: Nil)
      )
    }

    def checkExistsResult(df: DataFrame, f: Expression => Expression, expected: Any): Unit = {
      val itemDataType = df.schema.head.dataType match {
        case ArrayType(dt, _) => dt
      }
      val arrayFieldName = df.schema.head.name

      val lv = NamedLambdaVariable("arg", itemDataType, nullable = true)
      val filterLambda = LambdaFunction(f(lv), Seq(lv))

      val resultDf =
        df.select(Column(ArrayExists(UnresolvedAttribute(arrayFieldName), filterLambda)))
      val result = resultDf.collect.head.get(0)

      if (result != expected) {
        fail(
          s"Incorrect evaluation: $f, " +
            s"actual: $result, " +
            s"expected: $expected")
      }
    }

    val ai0 = createDF(Seq(1, 2, 3), IntegerType)
    val ai1 = createDF(Seq[Integer](1, null, 3), IntegerType)
    val ain = createDF(null, IntegerType)

    val isEven: Expression => Expression = x => x % 2 === 0
    val isNullOrOdd: Expression => Expression = x => x.isNull || x % 2 === 1
    val alwaysFalse: Expression => Expression = _ => Literal.FalseLiteral
    val alwaysNull: Expression => Expression = _ => Literal(null, BooleanType)

    for (followThreeValuedLogic <- Seq(false, true)) {
      withSQLConf(
        SQLConf.LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC.key
          -> followThreeValuedLogic.toString) {
        checkExistsResult(ai0, isEven, true)
        checkExistsResult(ai0, isNullOrOdd, true)
        checkExistsResult(ai0, alwaysFalse, false)
        checkExistsResult(ai0, alwaysNull, if (followThreeValuedLogic) null else false)
        checkExistsResult(ai1, isEven, if (followThreeValuedLogic) null else false)
        checkExistsResult(ai1, isNullOrOdd, true)
        checkExistsResult(ai1, alwaysFalse, false)
        checkExistsResult(ai1, alwaysNull, if (followThreeValuedLogic) null else false)
        checkExistsResult(ain, isEven, null)
        checkExistsResult(ain, isNullOrOdd, null)
        checkExistsResult(ain, alwaysFalse, null)
        checkExistsResult(ain, alwaysNull, null)
      }
    }

    val as0 = createDF(Seq("a0", "b1", "a2", "c3"), StringType)
    val as1 = createDF(Seq(null, "b", "c"), StringType)
    val asn = createDF(null, StringType)

    val startsWithA: Expression => Expression = x => x.startsWith("a")

    for (followThreeValuedLogic <- Seq(false, true)) {
      withSQLConf(
        SQLConf.LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC.key
          -> followThreeValuedLogic.toString) {
        checkExistsResult(as0, startsWithA, true)
        checkExistsResult(as0, alwaysFalse, false)
        checkExistsResult(as0, alwaysNull, if (followThreeValuedLogic) null else false)
        checkExistsResult(as1, startsWithA, if (followThreeValuedLogic) null else false)
        checkExistsResult(as1, alwaysFalse, false)
        checkExistsResult(as1, alwaysNull, if (followThreeValuedLogic) null else false)
        checkExistsResult(asn, startsWithA, null)
        checkExistsResult(asn, alwaysFalse, null)
        checkExistsResult(asn, alwaysNull, null)
      }
    }
  }
}
