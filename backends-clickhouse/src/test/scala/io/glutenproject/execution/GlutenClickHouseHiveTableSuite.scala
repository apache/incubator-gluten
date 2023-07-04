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
package io.glutenproject.execution

import io.glutenproject.GlutenConfig
import io.glutenproject.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import java.io.File

case class AllDataTypesWithComplextType(
    string_field: String = null,
    int_field: java.lang.Integer = null,
    long_field: java.lang.Long = null,
    float_field: java.lang.Float = null,
    double_field: java.lang.Double = null,
    short_field: java.lang.Short = null,
    byte_field: java.lang.Byte = null,
    bool_field: java.lang.Boolean = null,
    decimal_field: java.math.BigDecimal = null,
    date_field: java.sql.Date = null,
    array: Seq[Int] = null,
    arrayContainsNull: Seq[Option[Int]] = null,
    map: Map[Int, Long] = null,
    mapValueContainsNull: Map[Int, Option[Long]] = null
)

class GlutenClickHouseHiveTableSuite()
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper
  with SharedSparkSession
  with BeforeAndAfterAll {

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def createTPCHNullableTables(): Unit = {}

  override protected def createTPCHNotNullTables(): Unit = {}

  override protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "536870912")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.adaptive.enabled", "true")
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
      .set(GlutenConfig.GLUTEN_LIB_PATH, "/usr/local/clickhouse/lib/libchd.so")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.gluten.sql.columnar.forceshuffledhashjoin", "true")
      .set(
        "spark.sql.warehouse.dir",
        getClass.getResource("/").getPath + "unit-tests-working-home/spark-warehouse")
      .setMaster("local[1]")
  }

  override protected def spark: SparkSession = {
    val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"
    SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .config(
        "javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true")
      .getOrCreate()
  }

  private val txt_table_name = "hive_txt_test"
  private val json_table_name = "hive_json_test"
  private val parquet_table_name = "hive_parquet_test"
  private val txt_table_create_sql = "create table if not exists %s (".format(txt_table_name) +
    "string_field string," +
    "int_field int," +
    "long_field long," +
    "float_field float," +
    "double_field double," +
    "short_field short," +
    "byte_field byte," +
    "bool_field boolean," +
    "decimal_field decimal(23, 12)," +
    "date_field date," +
    "array_field array<int>," +
    "array_field_with_null array<int>," +
    "map_field map<int, long>," +
    "map_field_with_null map<int, long>) stored as textfile"

  private val json_table_create_sql = "create table if not exists %s (".format(json_table_name) +
    "string_field string," +
    "int_field int," +
    "long_field long," +
    "float_field float," +
    "double_field double," +
    "short_field short," +
    "byte_field byte," +
    "bool_field boolean," +
    "decimal_field decimal(23, 12)," +
    "date_field date," +
    "array_field array<int>," +
    "array_field_with_null array<int>," +
    "map_field map<int,long>," +
    "map_field_with_null map<int,long>) " +
    "ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'" +
    "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'" +
    "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"

  private val parquet_table_create_sql =
    "create table if not exists %s (".format(parquet_table_name) +
      "string_field string," +
      "int_field int," +
      "long_field long," +
      "float_field float," +
      "double_field double," +
      "short_field short," +
      "byte_field byte," +
      "bool_field boolean," +
      "decimal_field decimal(23, 12)" +
      "" +
      " ) partitioned by (date_field date) stored as parquet"

  def genTestData(): Seq[AllDataTypesWithComplextType] = {
    (0 to 199).map {
      i =>
        if (i % 100 == 1) {
          AllDataTypesWithComplextType()
        } else {
          AllDataTypesWithComplextType(
            s"$i",
            i,
            i.toLong,
            i.toFloat,
            i.toDouble,
            i.toShort,
            i.toByte,
            i % 2 == 0,
            new java.math.BigDecimal(i + ".56"),
            new java.sql.Date(System.currentTimeMillis()),
            Seq.apply(i + 1, i + 2, i + 3),
            Seq.apply(Option.apply(i + 1), Option.empty, Option.apply(i + 3)),
            Map.apply((i + 1, i + 2), (i + 3, i + 4)),
            Map.empty
          )
        }
    }
  }

  protected def initializeTable(table_name: String, table_create_sql: String): Unit = {
    spark.createDataFrame(genTestData()).createOrReplaceTempView("tmp_t")
    spark.sql(s"drop table IF EXISTS $table_name")
    spark.sql(table_create_sql)
    spark.sql("insert into %s select * from tmp_t".format(table_name))
  }

  override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    FileUtils.copyDirectory(new File(rootPath + resourcePath), new File(tablesPath))
//    initializeTable(txt_table_name, txt_table_create_sql)
//    initializeTable(json_table_name, json_table_create_sql)
  }

  test("test hive text table") {
    val sql =
      s"""
         | select string_field,
         |        sum(int_field),
         |        avg(long_field),
         |        min(float_field),
         |        max(double_field),
         |        sum(short_field),
         |        sum(decimal_field)
         | from $txt_table_name
         | group by string_field
         | order by string_field
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      })
  }

  test("test hive text table with unordered columns") {
    val sql = "select decimal_field, short_field, double_field, float_field, long_field, " +
      s"int_field, string_field from $txt_table_name order by string_field"
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      })
  }

  test("test hive text table with count(1)/count(*)") {
    val sql1 = s"select count(1), count(*) from $txt_table_name"
    compareResultsAgainstVanillaSpark(
      sql1,
      true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      })

    val sql2 = s"select count(*) from $txt_table_name where int_field >= 100"
    compareResultsAgainstVanillaSpark(
      sql2,
      true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      })
  }

  test("fix bug: https://github.com/oap-project/gluten/issues/2022") {
    spark.sql(
      "create table if not exists test_empty_partitions" +
        "(uid string, mac string, country string) partitioned by (day string) stored as textfile")

    var sql = "select * from test_empty_partitions where day = '2023-01-01';"
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      })
  }

  test("fix bug: hive text table limit with fallback") {
    val sql =
      s"""
         | select string_field
         | from $txt_table_name
         | order by string_field
         | limit 10
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      })
  }

  test("hive text table select complex type columns with fallback") {
    val sql = s"select int_field, array_field, map_field from $txt_table_name order by int_field"
    compareResultsAgainstVanillaSpark(sql, true, { _ => }, false)
  }

  test("hive text table case-insensitive column matching") {
    val sql = s"select SHORT_FIELD, int_field, LONG_field from $txt_table_name order by int_field"
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      })
  }

  test("test hive parquet table") {
    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {

      val table_name = parquet_table_name
      val table_create_sql = parquet_table_create_sql // this is a partitioned table
      spark.createDataFrame(genTestData()).createOrReplaceTempView("tmp_t")
      spark.sql(s"drop table IF EXISTS $table_name")
      spark.sql(table_create_sql)
      //    spark.sql(
      //      ("insert overwrite local directory '/tmp/destination' stored as parquet select string_field,int_field,long_field,float_field," +
      //        "double_field,short_field,byte_field,bool_field,decimal_field" +
      //        " from tmp_t").format(table_name))

      // test selected column order different from table schema

      // test composite bucket expression

      // test multiple partition col
      spark.sql(
        ("insert overwrite %s   select string_field,int_field,long_field,float_field," +
          "double_field,short_field,byte_field,bool_field,decimal_field,date_field" +
          " from tmp_t").format(table_name))

//      val sql =
//        s"""
//           | select string_field,
//           |        sum(int_field),
//           |        avg(long_field),
//           |        min(float_field),
//           |        max(double_field),
//           |        sum(short_field),
//           |        sum(decimal_field)
//           | from $parquet_table_name
//           | group by string_field
//           | order by string_field
//           |""".stripMargin
//      val sql2 = s"select count(*), string_field from $parquet_table_name group by string_field"
//      spark.sql(sql2).show(100)
//      compareResultsAgainstVanillaSpark(sql, true, f => {})
    }
    withSQLConf(("spark.gluten.enabled", "false")) {
      val sql2 = s"select count(*), string_field from $parquet_table_name group by string_field"
      spark.sql(sql2).show(100)
      println("")
    }

  }

  test("test hive json table") {
    val sql =
      s"""
         | select string_field,
         |        sum(int_field),
         |        avg(long_field),
         |        min(float_field),
         |        max(double_field),
         |        sum(short_field),
         |        sum(decimal_field)
         | from $json_table_name
         | group by string_field
         | order by string_field
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      df => {
        val jsonFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(jsonFileScan.size == 1)
      })
  }

  test("test hive json table complex data type") {
    val sql =
      s"""
         | select array_field, map_field from $json_table_name
         | where string_field != '' and int_field > 0
         |""".stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      df => {
        val jsonFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(jsonFileScan.size == 1)
      }
    )

  }

  test("GLUTEN-2019: Bug fix not allow quotes") {
    val default_quote_table_name = "test_2019_default"
    val allow_double_quote_table_name = "test_2019_allow_double"
    val allow_single_quote_table_name = "test_2019_allow_single"
    val default_data_path = getClass.getResource("/").getPath + "/text-data/default"
    val allow_double_data_path = getClass.getResource("/").getPath + "/text-data/double-quote"
    val allow_single_data_path = getClass.getResource("/").getPath + "/text-data/single-quote"
    val drop_default_table_sql = "drop table if exists %s".format(default_quote_table_name)
    val drop_double_quote_table_sql =
      "drop table if exists %s".format(allow_double_quote_table_name)
    val drop_single_quote_table_sql =
      "drop table if exists %s".format(allow_single_quote_table_name)
    val create_default_table_sql =
      "create table if not exists %s (".format(default_quote_table_name) +
        "a string," +
        "b string, " +
        "c string)" +
        " row format delimited fields terminated by ',' stored as textfile LOCATION \"%s\""
          .format(default_data_path)
    val create_double_quote_table_sql =
      "create table if not exists %s (".format(allow_double_quote_table_name) +
        "a string," +
        "b string, " +
        "c string)" +
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'" +
        " WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '\"')" +
        " stored as textfile LOCATION \"%s\"".format(allow_double_data_path)
    val create_single_quote_table_sql =
      "create table if not exists %s (".format(allow_single_quote_table_name) +
        "a string," +
        "b string, " +
        "c string)" +
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'" +
        " WITH SERDEPROPERTIES (\"separatorChar\" = \",\", \"quoteChar\" = \"\'\")" +
        " stored as textfile LOCATION \"%s\"".format(allow_single_data_path)

    spark.sql(drop_default_table_sql)
    spark.sql(drop_double_quote_table_sql)
    spark.sql(drop_single_quote_table_sql)
    spark.sql(create_default_table_sql)
    spark.sql(create_double_quote_table_sql)
    spark.sql(create_single_quote_table_sql)

    val sql1 = "select * from " + default_quote_table_name
    val sql2 = "select * from " + allow_double_quote_table_name
    val sql3 = "select * from " + allow_single_quote_table_name
    compareResultsAgainstVanillaSpark(
      sql1,
      true,
      df => {
        val txtFileScan =
          collect(df.queryExecution.executedPlan) { case l: HiveTableScanExecTransformer => l }
        assert(txtFileScan.size == 1)
      })
    compareResultsAgainstVanillaSpark(
      sql2,
      true,
      df => {
        val txtFileScan =
          collect(df.queryExecution.executedPlan) { case l: HiveTableScanExecTransformer => l }
        assert(txtFileScan.size == 1)
      })
    compareResultsAgainstVanillaSpark(
      sql3,
      true,
      df => {
        val txtFileScan =
          collect(df.queryExecution.executedPlan) { case l: HiveTableScanExecTransformer => l }
        assert(txtFileScan.size == 1)
      })
  }

  test("text hive table with space/tab delimiter") {
    val txt_table_name_space_delimiter = "hive_txt_table_space_delimiter"
    val txt_table_name_tab_delimiter = "hive_txt_table_tab_delimiter"
    val drop_space_table_sql = "drop table if exists %s".format(txt_table_name_space_delimiter)
    val drop_tab_table_sql = "drop table if exists %s".format(txt_table_name_tab_delimiter)
    val create_space_table_sql =
      "create table if not exists %s (".format(txt_table_name_space_delimiter) +
        "int_field int," +
        "string_field string" +
        ") row format delimited fields terminated by ' ' stored as textfile"
    val create_tab_table_sql =
      "create table if not exists %s (".format(txt_table_name_tab_delimiter) +
        "int_field int," +
        "string_field string" +
        ") row format delimited fields terminated by '\t' stored as textfile"
    spark.sql(drop_space_table_sql)
    spark.sql(drop_tab_table_sql)
    spark.sql(create_space_table_sql)
    spark.sql(create_tab_table_sql)
    spark.sql("insert into %s values(1, 'ab')".format(txt_table_name_space_delimiter))
    spark.sql("insert into %s values(1, 'ab')".format(txt_table_name_tab_delimiter))
    val sql1 =
      s"""
         | select * from $txt_table_name_space_delimiter where int_field > 0
         |""".stripMargin
    val sql2 =
      s"""
         | select * from $txt_table_name_tab_delimiter where int_field > 0
         |""".stripMargin

    compareResultsAgainstVanillaSpark(
      sql1,
      true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      })
    compareResultsAgainstVanillaSpark(
      sql2,
      true,
      df => {
        val txtFileScan = collect(df.queryExecution.executedPlan) {
          case l: HiveTableScanExecTransformer => l
        }
        assert(txtFileScan.size == 1)
      })
  }
}
