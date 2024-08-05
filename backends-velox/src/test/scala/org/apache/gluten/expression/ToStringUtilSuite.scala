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
package org.apache.gluten.expression

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ToStringUtil, UnsafeProjection}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class ToStringUtilSuite extends SparkFunSuite {

  test("UnsafeRow to string") {
    val util = ToStringUtil(Option.apply(SQLConf.get.sessionLocalTimeZone))
    val row1 =
      InternalRow.apply(UTF8String.fromString("hello"), UTF8String.fromString("world"), 123)
    val struct = new StructType().add("a", StringType).add("b", StringType).add("c", IntegerType)
    assert(util.evalRow(row1, struct) == "hello | world | 123")
    assert(util.evalRow(row1, struct, 4) == "hell... | worl... | 123")
    val rowWithNull = InternalRow.apply(null, null, 4)
    val row2 = UnsafeProjection
      .create(Array[DataType](StringType, StringType, IntegerType))
      .apply(rowWithNull)
    val it = List(row1, row2, row1, row1, row2).toIterator
    assert(util.evalRows(it, 0, 2, struct) == "hello | world | 123\nNULL | NULL | 4")
  }

}
