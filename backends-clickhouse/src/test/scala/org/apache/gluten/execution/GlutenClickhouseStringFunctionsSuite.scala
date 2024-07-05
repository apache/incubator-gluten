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
package org.apache.gluten.execution

import org.apache.spark.SparkConf

class GlutenClickhouseStringFunctionsSuite extends GlutenClickHouseWholeStageTransformerSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "SNAPPY")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
  }

  test("GLUTEN-5821: trim_character support value from column.") {
    withTable("trim") {
      sql("create table trim(a String, b String) using parquet")
      sql("""
            |insert into trim values
            |  ('aba', 'a'),('bba', 'b'),('abcdef', 'abcd'), (null, '123'),('123', null)
            |""".stripMargin)

      val sql_str =
        s"""select
           |    trim(both b from a)
           |  from trim
          """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

  test("GLUTEN-5897: fix regexp_extract with bracket") {
    withTable("regexp_extract_bracket") {
      sql("create table regexp_extract_bracket(a String) using parquet")
      sql("""
            |insert into regexp_extract_bracket
            | values ('123.123abc-abc'),('123-LOW'),('123]abc-abc')
            |""".stripMargin)

      val sql_str =
        s"""select
           |    regexp_extract(a, '([0-9][[\\.][0-9]]*)', 1)
           |  , regexp_extract(a, '([0-9][[\\.][0-9]]*)', 1)
           |  , regexp_extract(a, '([0-9][[]]]*)', 1)
           |  from regexp_extract_bracket
          """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

  test("replace") {
    val tableName = "replace_table"
    withTable(tableName) {
      sql(s"create table $tableName(src String, idx String, dest String) using parquet")
      sql(s"""
             |insert into $tableName values
             |  (null, null, null),
             |  ('1', '1', null),
             |  ('1', '1', '2'),
             |  ('1', null, '2'),
             |  ('1', '1', '3'),
             |  (null, '1', '2'),
             |  ('1', '', '3')
          """.stripMargin)

      val sql_str =
        s"""
           |select
           |    REPLACE(src, idx, dest),
           |    REPLACE(src, null, dest),
           |    REPLACE(null, null, dest),
           |    REPLACE(null, null, null),
           |    REPLACE(src, '1', null)
           |  from $tableName
      """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

  test("base64") {
    val tableName = "base64_table"
    withTable(tableName) {
      sql(s"create table $tableName(data String) using parquet")
      sql(s"""
             |insert into $tableName values
             |  ("hello")
          """.stripMargin)

      val sql_str =
        s"""
           |select
           |    base64(data)
           |  from $tableName
      """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

  test("unbase64") {
    val tableName = "unbase64_table"
    withTable(tableName) {
      sql(s"create table $tableName(data String) using parquet")
      sql(s"""
             |insert into $tableName values
             |  ("U3BhcmsgU1FM")
          """.stripMargin)

      val sql_str =
        s"""
           |select
           |    unbase64(data)
           |  from $tableName
      """.stripMargin

      runQueryAndCompare(sql_str) { _ => }
    }
  }

}
