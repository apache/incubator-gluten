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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.SparkConf

class GlutenClickhouseFunctionSuite extends GlutenClickHouseTPCHAbstractSuite {
  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  override protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1073741824")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.minPartitionNum", "1")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.gluten.sql.columnar.columnartorow", "true")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.clickHouseLibPath)
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      // TODO: support default ANSI policy
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set("spark.sql.warehouse.dir", warehouse)
      .setMaster("local[1]")
  }

  test("test uuid - write and read") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      (GlutenConfig.GLUTEN_ENABLED.key, "true")) {
      withTable("uuid_test") {
        spark.sql("create table if not exists uuid_test (id string) using parquet")

        val df = spark.sql("select regexp_replace(uuid(), '-', '') as id from range(1)")
        df.cache()
        df.write.insertInto("uuid_test")

        val df2 = spark.table("uuid_test")
        val diffCount = df.exceptAll(df2).count()
        assert(diffCount == 0)
      }
    }
  }

  test("Support In list option contains non-foldable expression") {
    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (1, 2, l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (1, 2, l_partkey - 1, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey not in (1, 2, l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey in (l_partkey + 1, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))

    runQueryAndCompare(
      """
        |SELECT * FROM lineitem
        |WHERE l_orderkey not in (l_partkey, l_suppkey, l_linenumber)
        |""".stripMargin
    )(df => checkFallbackOperators(df, 0))
  }

  test("GLUTEN-5981 null value from get_json_object") {
    withTable("json_t1") {
      spark.sql("create table json_t1 (a string) using parquet")
      spark.sql("insert into json_t1 values ('{\"a\":null}')")
      runQueryAndCompare(
        """
          |SELECT get_json_object(a, '$.a') is null from json_t1
          |""".stripMargin
      )(df => checkFallbackOperators(df, 0))
    }
  }

  test("Fix arrayDistinct(Array(Nullable(Decimal))) core dump") {
    withTable("json_t1") {
      val create_sql =
        """
          |create table if not exists test(
          | dec array<decimal(10, 2)>
          |) using parquet
          |""".stripMargin
      val fill_sql =
        """
          |insert into test values(array(1, 2, null)), (array(null, 2,3, 5))
          |""".stripMargin
      val query_sql =
        """
          |select array_distinct(dec) from test;
          |""".stripMargin
      spark.sql(create_sql)
      spark.sql(fill_sql)
      compareResultsAgainstVanillaSpark(query_sql, true, { _ => })
    }
  }

  test("intersect all") {
    withTable("t1", "t2") {
      spark.sql("create table t1 (a int, b string) using parquet")
      spark.sql("insert into t1 values (1, '1'),(2, '2'),(3, '3'),(4, '4'),(5, '5'),(6, '6')")
      spark.sql("create table t2 (a int, b string) using parquet")
      spark.sql("insert into t2 values (4, '4'),(5, '5'),(6, '6'),(7, '7'),(8, '8'),(9, '9')")
      runQueryAndCompare(
        """
          |SELECT a,b FROM t1 INTERSECT ALL SELECT a,b FROM t2
          |""".stripMargin
      )(df => checkFallbackOperators(df, 0))
    }
  }

  test("array decimal32 CH column to row") {
    compareResultsAgainstVanillaSpark("SELECT array(1.0, 2.0)", true, { _ => }, false)
    compareResultsAgainstVanillaSpark("SELECT map(1.0, '2', 3.0, '4')", true, { _ => }, false)
  }

  test("array decimal32 spark row to CH column") {
    withTable("test_array_decimal") {
      sql("""
            |create table test_array_decimal(val array<decimal(5,1)>)
            |using parquet
            |""".stripMargin)
      sql("""
            |insert into test_array_decimal
            |values array(1.0, 2.0), array(3.0, 4.0),
            |array(5.0, 6.0), array(7.0, 8.0), array(7.0, 7.0)
            |""".stripMargin)
      // disable native scan so will get a spark row to CH column
      withSQLConf(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
        val q = "SELECT max(val) from test_array_decimal"
        compareResultsAgainstVanillaSpark(q, true, { _ => }, false)
        val q2 = "SELECT max(val[0]) from test_array_decimal"
        compareResultsAgainstVanillaSpark(q2, true, { _ => }, false)
        val q3 = "SELECT max(val[1]) from test_array_decimal"
        compareResultsAgainstVanillaSpark(q3, true, { _ => }, false)
      }
    }
  }

}
