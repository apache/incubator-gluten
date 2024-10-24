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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, Row}

class GlutenParquetFieldIdIOSuite extends ParquetFieldIdIOSuite with GlutenSQLTestsBaseTrait {
  testGluten("Parquet writer with ARRAY and MAP") {
    spark.sql("""
                |CREATE TABLE T1 (
                | a INT,
                | b ARRAY<STRING>,
                | c MAP<STRING,STRING>
                |)
                |USING PARQUET
                |""".stripMargin)

    spark.sql("""
                | INSERT OVERWRITE T1 VALUES
                |  (1, ARRAY(1, 2, 3), MAP("key1","value1"))
                |""".stripMargin)

    checkAnswer(
      spark.sql("SELECT * FROM T1"),
      Row(1, Array("1", "2", "3"), Map("key1" -> "value1")) :: Nil
    )
  }

  testGluten("Parquet writer with STRUCT") {
    spark.sql("""
                |CREATE TABLE IF NOT EXISTS people (
                |    id INT,
                |    name STRING,
                |    age INT,
                |    address STRUCT<
                |        street: STRING,
                |        city: STRING,
                |        zip: STRING
                |    >
                |)
                |USING parquet
                |""".stripMargin)

    spark.sql("""
                |INSERT INTO people VALUES
                |    (1, "Alice", 30, struct("123 Main St", "Springfield", "12345")),
                |    (2, "Bob", 25, struct("456 Maple Ave", "Shelbyville", "54321")),
                |    (3, "Charlie", 35, struct("789 Oak St", "Capital City", "67890"))
                |""".stripMargin)
    checkAnswer(
      spark.sql("SELECT * FROM people"),
      Row(1, "Alice", 30, Row("123 Main St", "Springfield", "12345")) ::
        Row(2, "Bob", 25, Row("456 Maple Ave", "Shelbyville", "54321")) ::
        Row(3, "Charlie", 35, Row("789 Oak St", "Capital City", "67890")) :: Nil
    )
  }
}
