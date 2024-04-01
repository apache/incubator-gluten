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

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.nio.file.Files
import java.sql.Date

import scala.reflect.ClassTag

class GlutenFunctionValidateSuite extends GlutenClickHouseWholeStageTransformerSuite {

  protected lazy val sparkVersion: String = {
    val version = SPARK_VERSION_SHORT.split("\\.")
    version(0) + "." + version(1)
  }

  protected val tablesPath: String = basePath + "/tpch-data"
  protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  protected val queriesResults: String = rootPath + "queries-output"

  private var parquetPath: String = _

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
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
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val lfile = Files.createTempFile("", ".parquet").toFile
    lfile.deleteOnExit()
    parquetPath = lfile.getAbsolutePath

    val schema = StructType(
      Array(
        StructField("double_field1", DoubleType, true),
        StructField("int_field1", IntegerType, true),
        StructField("string_field1", StringType, true)
      ))
    val data = sparkContext.parallelize(
      Seq(
        Row(1.025, 1, "{\"a\":\"b\"}"),
        Row(1.035, 2, null),
        Row(1.045, 3, "{\"1a\":\"b\"}"),
        Row(1.011, 4, "{\"a 2\":\"b\"}"),
        Row(1.011, 5, "{\"a_2\":\"b\"}"),
        Row(1.011, 5, "{\"a\":\"b\", \"x\":{\"i\":1}}"),
        Row(1.011, 5, "{\"a\":\"b\", \"x\":{\"i\":2}}"),
        Row(1.011, 5, "{\"a\":1, \"x\":{\"i\":2}}"),
        Row(1.0, 5, "{\"a\":\"{\\\"x\\\":5}\"}")
      ))
    val dfParquet = spark.createDataFrame(data, schema)
    dfParquet
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(parquetPath)

    spark.catalog.createTable("json_test", parquetPath, fileFormat)

    val dateSchema = StructType(
      Array(
        StructField("ts", IntegerType, true),
        StructField("day", DateType, true),
        StructField("weekday_abbr", StringType, true)
      )
    )
    val dateRows = sparkContext.parallelize(
      Seq(
        Row(1546309380, Date.valueOf("2019-01-01"), "MO"),
        Row(1546273380, Date.valueOf("2019-01-01"), "TU"),
        Row(1546358340, Date.valueOf("2019-01-01"), "TH"),
        Row(1546311540, Date.valueOf("2019-01-01"), "WE"),
        Row(1546308540, Date.valueOf("2019-01-01"), "FR"),
        Row(1546319340, Date.valueOf("2019-01-01"), "SA"),
        Row(1546319940, Date.valueOf("2019-01-01"), "SU"),
        Row(1546323545, Date.valueOf("2019-01-01"), "MO"),
        Row(1546409940, Date.valueOf("2019-01-02"), "MM"),
        Row(1546496340, Date.valueOf("2019-01-03"), "TH"),
        Row(1546586340, Date.valueOf("2019-01-04"), "WE"),
        Row(1546676341, Date.valueOf("2019-01-05"), "FR"),
        Row(null, null, "SA"),
        Row(1546849141, Date.valueOf("2019-01-07"), null)
      )
    )
    val dateTableFile = Files.createTempFile("", ".parquet").toFile
    dateTableFile.deleteOnExit()
    val dateTableFilePath = dateTableFile.getAbsolutePath
    val dateTablePQ = spark.createDataFrame(dateRows, dateSchema)
    dateTablePQ
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(dateTableFilePath)
    spark.catalog.createTable("date_table", dateTableFilePath, fileFormat)
    val str2Mapfile = Files.createTempFile("", ".parquet").toFile
    str2Mapfile.deleteOnExit()
    val str2MapFilePath = str2Mapfile.getAbsolutePath
    val str2MapSchema = StructType(
      Array(
        StructField("str", StringType, true)
      ))
    val str2MapData = sparkContext.parallelize(
      Seq(
        Row("a:1,b:2,c:3"),
        Row("a:1,b:2"),
        Row("a:1;b:2"),
        Row("a:1,d:4"),
        Row("a:"),
        Row(null),
        Row(":,a:1"),
        Row(":"),
        Row("")
      ))
    val str2MapDfParquet = spark.createDataFrame(str2MapData, str2MapSchema)
    str2MapDfParquet
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(str2MapFilePath)
    spark.catalog.createTable("str2map_table", str2MapFilePath, fileFormat)

    val urlFile = Files.createTempFile("", ".parquet").toFile()
    urlFile.deleteOnExit()
    val urlFilePath = urlFile.getAbsolutePath
    val urlTalbeSchema = StructType(
      Array(
        StructField("url", StringType, true)
      )
    )
    val urlTableData = sparkContext.parallelize(
      Seq(
        Row("http://www.gluten.com"),
        Row("www.gluten.com"),
        Row("http://www.gluten.com?x=1"),
        Row("http://www.gluten?x=1"),
        Row("http://www.gluten.com?x=1#Ref"),
        Row("http://www.gluten.com#Ref?x=1"),
        Row("http://www.gluten.com?x=1&y=2"),
        Row("https://www.gluten.com?x=1&y=2"),
        Row("file://www.gluten.com?x=1&y=2"),
        Row("hdfs://www.gluten.com?x=1&y=2"),
        Row("hdfs://www.gluten.com?x=1&y=2/a/b"),
        Row("hdfs://www.gluten.com/x/y"),
        Row("hdfs://xy:12@www.gluten.com/x/y"),
        Row("xy:12@www.gluten.com/x/y"),
        Row("www.gluten.com/x/y"),
        Row("www.gluten.com?x=1"),
        Row("www.gluten.com:999?x=1"),
        Row("www.gluten.com?x=1&y=2"),
        Row("heel?x=1&y=2"),
        Row("x=1&y=2"),
        Row("/a/b/cx=1&y=2"),
        Row("gluten?x=1&y=2"),
        Row("xxhhh"),
        Row(null)
      )
    )
    val urlPQFile = spark.createDataFrame(urlTableData, urlTalbeSchema)
    urlPQFile.coalesce(1).write.format("parquet").mode("overwrite").parquet(urlFilePath)
    spark.catalog.createTable("url_table", urlFilePath, fileFormat)
  }

  test("Test get_json_object 1") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.a') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 2") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.1a') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 3") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.a_2') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  ignore("Test get_json_object 4") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$[a]') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 5") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$[\\\'a\\\']') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 6") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$[\\\'a 2\\\']') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 7") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$..') from json_test",
      noFallBack = false) { _ => }
  }

  test("Test get_json_object 8") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$..') from json_test",
      noFallBack = false) { _ => }
  }

  test("Test get_json_object 9") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$.x[?(@.i == 1)]') from json_test",
      noFallBack = false) { _ => }
  }

  test("Test nested get_json_object") {
    runQueryAndCompare(
      "SELECT get_json_object(get_json_object(string_field1, '$.a'), '$.x') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 10") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.12345') from json_test") { _ => }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.123.abc') from json_test") { _ => }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.123.123') from json_test") { _ => }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.123abc.123') from json_test") {
      _ =>
    }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.abc.123') from json_test") { _ => }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.123[0]') from json_test") { _ => }
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.123[0].123') from json_test") {
      _ =>
    }
  }

  test("Test covar_samp") {
    runQueryAndCompare("SELECT covar_samp(double_field1, int_field1) from json_test") { _ => }
  }

  test("Test covar_pop") {
    runQueryAndCompare("SELECT covar_pop(double_field1, int_field1) from json_test") { _ => }
  }

  test("test 'function xxhash64'") {
    val df1 = runQueryAndCompare(
      "select xxhash64(cast(id as int)), xxhash64(cast(id as byte)), " +
        "xxhash64(cast(id as short)), " +
        "xxhash64(cast(id as long)), xxhash64(cast(id as float)), xxhash64(cast(id as double)), " +
        "xxhash64(cast(id as string)), xxhash64(cast(id as binary)), " +
        "xxhash64(cast(from_unixtime(id) as date)), " +
        "xxhash64(cast(from_unixtime(id) as timestamp)), xxhash64(cast(id as decimal(5, 2))), " +
        "xxhash64(cast(id as decimal(10, 2))), xxhash64(cast(id as decimal(30, 2))) " +
        "from range(10)"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df1, 10)

    val df2 = runQueryAndCompare(
      "select xxhash64(cast(id as int), 'spark'), xxhash64(cast(id as byte), 'spark'), " +
        "xxhash64(cast(id as short), 'spark'), xxhash64(cast(id as long), 'spark'), " +
        "xxhash64(cast(id as float), 'spark'), xxhash64(cast(id as double), 'spark'), " +
        "xxhash64(cast(id as string), 'spark'), xxhash64(cast(id as binary), 'spark'), " +
        "xxhash64(cast(from_unixtime(id) as date), 'spark'), " +
        "xxhash64(cast(from_unixtime(id) as timestamp), 'spark'), " +
        "xxhash64(cast(id as decimal(5, 2)), 'spark'), " +
        "xxhash64(cast(id as decimal(10, 2)), 'spark'), " +
        "xxhash64(cast(id as decimal(30, 2)), 'spark') from range(10)"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df2, 10)
  }

  test("test function xxhash64 with complex types") {
    val sql =
      """
        |select
        |  xxhash64(array(id, null, id+1, 100)),
        |  xxhash64(array(cast(id as string), null, 'spark')),
        |  xxhash64(array(null)),
        |  xxhash64(cast(null as array<int>)),
        |  xxhash64(array(array(id, null, id+1))),
        |  xxhash64(cast(null as struct<a:int, b:string>)),
        |  xxhash64(struct(id, cast(id as string), 100, 'spark', null))
        |from range(10);
      """.stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test 'function murmur3hash'") {
    val df1 = runQueryAndCompare(
      "select hash(cast(id as int)), hash(cast(id as byte)), hash(cast(id as short)), " +
        "hash(cast(id as long)), hash(cast(id as float)), hash(cast(id as double)), " +
        "hash(cast(id as string)), hash(cast(id as binary)), " +
        "hash(cast(from_unixtime(id) as date)), " +
        "hash(cast(from_unixtime(id) as timestamp)), hash(cast(id as decimal(5, 2))), " +
        "hash(cast(id as decimal(10, 2))), hash(cast(id as decimal(30, 2))) from range(10)"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df1, 10)

    val df2 = runQueryAndCompare(
      "select hash(cast(id as int), 'spark'), hash(cast(id as byte), 'spark'), " +
        "hash(cast(id as short), 'spark'), hash(cast(id as long), 'spark'), " +
        "hash(cast(id as float), 'spark'), hash(cast(id as double), 'spark'), " +
        "hash(cast(id as string), 'spark'), hash(cast(id as binary), 'spark'), " +
        "hash(cast(from_unixtime(id) as date), 'spark'), " +
        "hash(cast(from_unixtime(id) as timestamp), 'spark'), " +
        "hash(cast(id as decimal(5, 2)), 'spark'), hash(cast(id as decimal(10, 2)), 'spark'), " +
        "hash(cast(id as decimal(30, 2)), 'spark') from range(10)"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df2, 10)
  }

  test("test function murmur3hash with complex types") {
    val sql =
      """
        |select
        |  hash(array(id, null, id+1, 100)),
        |  hash(array(cast(id as string), null, 'spark')),
        |  hash(array(null)),
        |  hash(cast(null as array<int>)),
        |  hash(array(array(id, null, id+1))),
        |  hash(cast(null as struct<a:int, b:string>)),
        |  hash(struct(id, cast(id as string), 100, 'spark', null))
        |from range(10);
      """.stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test next_day const") {
    runQueryAndCompare("select next_day(day, 'MO') from date_table") { _ => }
  }
  test("test next_day const all null") {
    runQueryAndCompare("select next_day(day, 'MM') from date_table") { _ => }
  }
  test("test next_day dynamic") {
    runQueryAndCompare("select next_day(day, weekday_abbr) from date_table") { _ => }
  }
  test("test last_day") {
    runQueryAndCompare("select last_day(day) from date_table") { _ => }
  }

  test("test issue: https://github.com/oap-project/gluten/issues/2340") {
    val sql =
      """
        |select array(null, array(id,2)) from range(10)
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test issue: https://github.com/oap-project/gluten/issues/2947") {
    val sql =
      """
        |select if(id % 2 = 0, null, array(id, 2, null)) from range(10)
        |""".stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test str2map") {
    val sql1 =
      """
        |select str, str_to_map(str, ',', ':') from str2map_table
        |""".stripMargin
    runQueryAndCompare(sql1)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test parse_url") {
    val sql1 =
      """
        | select url, parse_url(url, "HOST") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql1)(checkOperatorMatch[ProjectExecTransformer])

    val sql2 =
      """
        | select url, parse_url(url, "QUERY") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql2)(checkOperatorMatch[ProjectExecTransformer])

    val sql3 =
      """
        | select url, parse_url(url, "QUERY", "x") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql3)(checkOperatorMatch[ProjectExecTransformer])

    val sql5 =
      """
        | select url, parse_url(url, "FILE") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql5)(checkOperatorMatch[ProjectExecTransformer])

    val sql6 =
      """
        | select url, parse_url(url, "REF") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql6)(checkOperatorMatch[ProjectExecTransformer])

    val sql7 =
      """
        | select url, parse_url(url, "USERINFO") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql7)(checkOperatorMatch[ProjectExecTransformer])

    val sql8 =
      """
        | select url, parse_url(url, "AUTHORITY") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql8)(checkOperatorMatch[ProjectExecTransformer])

    val sql9 =
      """
        | select url, parse_url(url, "PROTOCOL") from url_table order by url
      """.stripMargin
    runQueryAndCompare(sql9)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test decode and encode") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      // Test codec with 'US-ASCII'
      runQueryAndCompare(
        "SELECT decode(encode('Spark SQL', 'US-ASCII'), 'US-ASCII')",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])

      // Test codec with 'UTF-16'
      runQueryAndCompare(
        "SELECT decode(encode('Spark SQL', 'UTF-16'), 'UTF-16')",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test cast float string to int") {
    runQueryAndCompare(
      "select cast(concat(cast(id as string), '.1') as int) from range(10)"
    )(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test cast string to float") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      runQueryAndCompare(
        "select cast('7.921901' as float), cast('7.921901' as double)",
        noFallBack = false
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test round issue: https://github.com/oap-project/gluten/issues/3462") {
    runQueryAndCompare(
      "select round(0.41875d * id , 4) from range(10);"
    )(checkOperatorMatch[ProjectExecTransformer])

    runQueryAndCompare(
      "select round(0.41875f * id , 4) from range(10);"
    )(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test date comparision expression override") {
    runQueryAndCompare(
      "select * from date_table where to_date(from_unixtime(ts)) < '2019-01-02'",
      noFallBack = true) { _ => }
    runQueryAndCompare(
      "select * from date_table where to_date(from_unixtime(ts)) <= '2019-01-02'",
      noFallBack = true) { _ => }
    runQueryAndCompare(
      "select * from date_table where to_date(from_unixtime(ts)) > '2019-01-02'",
      noFallBack = true) { _ => }
    runQueryAndCompare(
      "select * from date_table where to_date(from_unixtime(ts)) >= '2019-01-02'",
      noFallBack = true) { _ => }
    runQueryAndCompare(
      "select * from date_table where to_date(from_unixtime(ts)) = '2019-01-01'",
      noFallBack = true) { _ => }
    runQueryAndCompare(
      "select * from date_table where from_unixtime(ts) between '2019-01-01' and '2019-01-02'",
      noFallBack = true) { _ => }
    runQueryAndCompare(
      "select * from date_table where from_unixtime(ts, 'yyyy-MM-dd') between" +
        " '2019-01-01' and '2019-01-02'",
      noFallBack = true) { _ => }
  }

  test("test element_at function") {
    withSQLConf(
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        (ConstantFolding.ruleName + "," + NullPropagation.ruleName)) {
      // input type is array<array<int>>
      runQueryAndCompare(
        "SELECT array(array(1,2,3), array(4,5,6))[1], " +
          "array(array(id,id+1,id+2), array(id+3,id+4,id+5)) from range(100)",
        noFallBack = true
      )(checkOperatorMatch[ProjectExecTransformer])

      // input type is array<array<string>>
      runQueryAndCompare(
        "SELECT array(array('1','2','3'), array('4','5','6'))[1], " +
          "array(array('1','2',cast(id as string)), array('4','5',cast(id as string)))[1] " +
          "from range(100)",
        noFallBack = true
      )(checkOperatorMatch[ProjectExecTransformer])

      // input type is array<map<string, int>>
      runQueryAndCompare(
        "SELECT array(map(cast(id as string), id), map(cast(id+1 as string), id+1))[1] " +
          "from range(100)",
        noFallBack = true
      )(checkOperatorMatch[ProjectExecTransformer])
    }
  }

  test("test common subexpression eliminate") {
    def checkOperatorCount[T <: TransformSupport](count: Int)(df: DataFrame)(implicit
        tag: ClassTag[T]): Unit = {
      if (sparkVersion.equals("3.3")) {
        assert(
          getExecutedPlan(df).count(
            plan => {
              plan.getClass == tag.runtimeClass
            }) == count,
          s"executed plan: ${getExecutedPlan(df)}")
      }
    }

    withSQLConf(("spark.gluten.sql.commonSubexpressionEliminate", "true")) {
      // CSE in project
      runQueryAndCompare("select hash(id), hash(id)+1, hash(id)-1 from range(10)") {
        df => checkOperatorCount[ProjectExecTransformer](2)(df)
      }

      // CSE in filter(not work yet)
      // runQueryAndCompare(
      //   "select id from range(10) " +
      //     "where hex(id) != '' and upper(hex(id)) != '' and lower(hex(id)) != ''") { _ => }

      // CSE in window
      runQueryAndCompare(
        "SELECT id, AVG(id) OVER (PARTITION BY id % 2 ORDER BY id) as avg_id, " +
          "SUM(id) OVER (PARTITION BY id % 2 ORDER BY id) as sum_id FROM range(10)") {
        df => checkOperatorCount[ProjectExecTransformer](4)(df)
      }

      // CSE in aggregate
      runQueryAndCompare(
        "select id % 2, max(hash(id)), min(hash(id)) " +
          "from range(10) group by id % 2") {
        df => checkOperatorCount[ProjectExecTransformer](1)(df)
      }
      runQueryAndCompare(
        "select id % 10, sum(id +100) + max(id+100) from range(100) group by id % 10") {
        df => checkOperatorCount[ProjectExecTransformer](2)(df)
      }
      // issue https://github.com/oap-project/gluten/issues/4642
      runQueryAndCompare(
        "select id, if(id % 2 = 0, sum(id), max(id)) as s1, " +
          "if(id %2 = 0, sum(id+1), sum(id+2)) as s2 from range(10) group by id") {
        df => checkOperatorCount[ProjectExecTransformer](2)(df)
      }

      // CSE in sort
      runQueryAndCompare(
        "select id from range(10) " +
          "order by hash(id%10), hash(hash(id%10))") {
        df => checkOperatorCount[ProjectExecTransformer](3)(df)
      }
    }
  }

  test("test function getarraystructfields") {
    val sql =
      """
        |SELECT id,
        |       struct_array[0].field1,
        |       struct_array[0].field2
        |FROM (
        |  SELECT id,
        |         array(struct(id as field1, (id+1) as field2)) as struct_array
        |  FROM range(10)
        |) t
      """.stripMargin
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("test parse string with blank to integer") {
    // issue https://github.com/apache/incubator-gluten/issues/4956
    val sql = "select  cast(concat(' ', cast(id as string)) as bigint) from range(10)"
    runQueryAndCompare(sql)(checkOperatorMatch[ProjectExecTransformer])
  }

  test("avg(bigint) overflow") {
    withSQLConf(
      "spark.gluten.sql.columnar.forceShuffledHashJoin" -> "false",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withTable("myitem") {
        sql("create table big_int(id bigint) using parquet")
        sql("""
              |insert into big_int values (9223372036854775807),
              |(9223372036854775807),
              |(9223372036854775807),
              |(9223372036854775807)
              |""".stripMargin)
        val q = "select avg(id) from big_int"
        runQueryAndCompare(q)(checkOperatorMatch[CHHashAggregateExecTransformer])
        val disinctSQL = "select count(distinct id), avg(distinct id), avg(id) from big_int"
        runQueryAndCompare(disinctSQL)(checkOperatorMatch[CHHashAggregateExecTransformer])
      }
    }
  }

}
