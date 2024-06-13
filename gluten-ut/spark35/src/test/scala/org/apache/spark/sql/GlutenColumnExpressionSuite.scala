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
package org.apache.spark.sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{expr, input_file_name}

class GlutenColumnExpressionSuite extends ColumnExpressionSuite with GlutenSQLTestsTrait {
  testGluten("input_file_name with scan is fallback") {
    withTempPath { dir =>
      val rawData = Seq(
        Row(1, "Alice", Seq(Row(Seq(1, 2, 3)))),
        Row(2, "Bob", Seq(Row(Seq(4, 5)))),
        Row(3, "Charlie", Seq(Row(Seq(6, 7, 8, 9))))
      )
      val schema = StructType(Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("nested_column", ArrayType(StructType(Array(
          StructField("array_in_struct", ArrayType(IntegerType), nullable = true)
        ))), nullable = true)
      ))
      val data: DataFrame = spark.createDataFrame(
        sparkContext.parallelize(rawData), schema)
      data.write.parquet(dir.getCanonicalPath)

      // Test the 3 expressions when reading from files
      val q = spark.read.parquet(dir.getCanonicalPath).select(
        input_file_name(), expr("nested_column"))
      logWarning(s"gyytest q is ${q.queryExecution.executedPlan}")
      val firstRow = q.head()
      assert(firstRow.getString(0).contains(dir.toURI.getPath))
    }
  }
}
