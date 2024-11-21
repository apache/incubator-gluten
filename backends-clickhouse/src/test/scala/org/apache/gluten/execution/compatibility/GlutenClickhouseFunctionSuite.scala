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
package org.apache.gluten.execution.compatibility

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.{GlutenClickHouseTPCHAbstractSuite, ProjectExecTransformer}
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig

class GlutenClickhouseFunctionSuite extends GlutenClickHouseTPCHAbstractSuite {
  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries"
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
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
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
      (GlutenConfig.NATIVE_WRITER_ENABLED.key, "true"),
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

  test("https://github.com/apache/incubator-gluten/issues/6938") {
    val testSQL =
      s"""
         |select * from (
         |  select 1 as x, r_name as y, 's' as z from region
         |  union all
         |  select 2 as x, n_name as y, null as z from nation
         |) order by y,x,z
         |""".stripMargin
    runQueryAndCompare(testSQL)(_ => ())
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

  test("duplicate column name issue") {
    withTable("left_table", "right_table") {
      sql("create table left_table(id int, name string) using orc")
      sql("create table right_table(id int, book string) using orc")
      sql("insert into left_table values (1,'a'),(2,'b'),(3,'c'),(4,'d')")
      sql("insert into right_table values (1,'a'),(1,'b'),(2,'c'),(2,'d')")
      compareResultsAgainstVanillaSpark(
        """
          |select p1.id, p1.name, p2.book
          | from left_table p1 left join
          | (select id, id, book
          |    from right_table where id <= 2) p2
          | on p1.id=p2.id
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("function_input_file_expr") {
    withTable("test_table") {
      sql("create table test_table(a int) using parquet")
      sql("insert into test_table values(1)")
      compareResultsAgainstVanillaSpark(
        """
          |select a,input_file_name(), input_file_block_start(),
          |input_file_block_length() from test_table
          |""".stripMargin,
        true,
        { _ => }
      )
      compareResultsAgainstVanillaSpark(
        """
          |select input_file_name(), input_file_block_start(),
          |input_file_block_length() from test_table
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7389: cast map to string diff with spark") {
    withTable("test_7389") {
      sql("create table test_7389(a map<string, int>) using parquet")
      sql("insert into test_7389 values(map('a', 1, 'b', 2))")
      compareResultsAgainstVanillaSpark(
        """
          |select cast(a as string) from test_7389
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7594: cast const map to string") {
    withSQLConf(
      (
        "spark.sql.optimizer.excludedRules",
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding," +
          "org.apache.spark.sql.catalyst.optimizer.NullPropagation")) {
      runQueryAndCompare(
        """
          |select cast(map(1,'2') as string)
          |""".stripMargin,
        true,
        false
      )(checkGlutenOperatorMatch[ProjectExecTransformer])
    }
  }

  test("GLUTEN-7550 get_json_object in IN") {
    withTable("test_7550") {
      sql("create table test_7550(a string) using parquet")
      val insert_sql =
        """
          |insert into test_7550 values('{\'a\':\'1\'}'),('{\'a\':\'2\'}'),('{\'a\':\'3\'}')
          |""".stripMargin
      sql(insert_sql)
      compareResultsAgainstVanillaSpark(
        """
          |select a, get_json_object(a, '$.a') in ('1', '2') from test_7550
          |""".stripMargin,
        true,
        { _ => }
      )
      compareResultsAgainstVanillaSpark(
        """
          |select a in ('1', '2') from test_7550
          |where get_json_object(a, '$.a') in ('1', '2')
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7552 normalize json path") {
    withTable("test_7552") {
      sql("create table test_7552(a string) using parquet")
      val insert_sql =
        """
          |insert into test_7552 values('{\'a\':\'1\'}')
          |,('{"a":3}')
          |,('{"3a":4}')
          |,('{"a c":5}')
          |,('{"3 d":6"}')
          |,('{"a:b":7}')
          |,('{"=a":8}')
          |""".stripMargin
      sql(insert_sql)
      compareResultsAgainstVanillaSpark(
        """
          |select a
          |, get_json_object(a, '$.a')
          |, get_json_object(a, '$.3a')
          |, get_json_object(a, '$.a c')
          |, get_json_object(a, '$.3 d')
          |, get_json_object(a, '$.a:b')
          |, get_json_object(a, '$.=a')
          |from test_7552
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7563 too large number in json") {
    withTable("test_7563") {
      sql("create table test_7563(a string) using parquet")
      val insert_sql =
        """
          |insert into test_7563 values
          |('{"a":2.696539702293474E308}')
          |,('{"a":1232}')
          |,('{"a":1234xxx}')
          |,('{"a":2.696539702293474E30123}')
          |""".stripMargin
      sql(insert_sql)
      compareResultsAgainstVanillaSpark(
        """
          |select a, get_json_object(a, '$.a') from test_7563
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7591 get_json_object: normalize empty object fail") {
    withTable("test_7591") {
      sql("create table test_7591(a string) using parquet")
      val insert_sql =
        """
          |insert into test_7591
          |select if(id < 10005, concat('{"a":', id), concat('{"a":', id , ', "b":{}}')) from
          |(SELECT explode(sequence(1, 10010)) as id);
          |""".stripMargin
      sql(insert_sql)
      compareResultsAgainstVanillaSpark(
        """
          |select get_json_object(a, '$.a') from test_7591
          |where get_json_object(a, '$.a') is not null
          |""".stripMargin,
        true,
        { _ => }
      )
    }
  }

  test("GLUTEN-7545: https://github.com/apache/incubator-gluten/issues/7545") {
    withTable("regexp_test") {
      sql("create table if not exists regexp_test (id string) using parquet")
      sql("insert into regexp_test values('1999-6-1')")
      compareResultsAgainstVanillaSpark(
        """
          |select regexp_replace(id,
          |'([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})',
          |'$1-$2-$3') from regexp_test
        """.stripMargin,
        true,
        { _ => }
      )
    }
  }

}
