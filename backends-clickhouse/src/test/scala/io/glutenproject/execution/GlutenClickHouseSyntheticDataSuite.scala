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
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseLog

import java.io.File
import java.sql.Date
import java.time.LocalDate

class GlutenClickHouseSyntheticDataSuite extends WholeStageTransformerSuite with Logging {

  override protected val backend: String = "ch"
  override protected val fileFormat: String = "parquet"

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val basePath: String = rootPath + "unit-tests-working-home"
  protected val tablesPath: String = basePath + "/synthetic-data"

  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

  override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
  }

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
      .set(GlutenConfig.GLUTEN_LOAD_NATIVE, "true")
      .set(GlutenConfig.GLUTEN_LOAD_ARROW, "false")
      .set(GlutenConfig.GLUTEN_LIB_PATH, "/usr/local/clickhouse/lib/libch.so")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.gluten.sql.columnar.forceshuffledhashjoin", "true")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.sql.legacy.createHiveTableByDefault", "false")
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
    /* .set("spark.sql.catalogImplementation", "hive")
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${
        metaStorePathAbsolute + "/metastore_db"};create=true") */
  }

  protected override def afterAll(): Unit = {
    ClickHouseLog.clearCache()
    super.afterAll()
    // init GlutenConfig in the next beforeAll
    GlutenConfig.ins = null
  }

  test("test all data types all agg") {
    var sqlStr: String = null
    var expected: Seq[Row] = null;
    withSQLConf(vanillaSparkConfs(): _*) {
      //val customerData = tablesPath + "/table_all_types_all_agg"
      spark.sql(s"DROP TABLE IF EXISTS table_all_types_all_agg")

      val supportedTypes = {
        //aggregator on "float"/"byte"/short"/"string" are not supported in gluten yet
//        "byte" ::
//          "short" ::
        "int" ::
          "bigint" ::
//          "float" ::
          "double" ::
          "date" ::
//          "string" ::
          "boolean" ::
          Nil
      }
      val fields = supportedTypes.map(x => s"c_${x} ${x}").mkString(",")
      val ddl =
        s"CREATE TABLE IF NOT EXISTS table_all_types_all_agg ( $fields )  USING PARQUET " //LOCATION '${customerData}'"
      println(s"the ddl is: $ddl")
      spark.sql(ddl);

      val x = spark
      import x.implicits._

      val source = Seq(
        (
//          Some(Byte.MinValue),
//          Some(Short.MinValue),
          Some(Int.MinValue),
          Some(Long.MinValue),
          //Some(Float.MinValue),
          Some(Double.MinValue),
          Some(LocalDate.of(1970, 1, 1)),
          //Some("a"),
          Some(false)),
        (
//          Some(Byte.MaxValue),
//          Some(Short.MaxValue),
          Some(Int.MaxValue),
          Some(Long.MaxValue),
          //Some(Float.MaxValue),
          Some(Double.MaxValue),
          Some(LocalDate.of(2070, 1, 1)),
          //Some("Z"),
          Some(true))
//        (None, None, None, None)
      )
      //val df_source = spark.sparkContext.parallelize(source).toDF()
      val df_source = source.toDF()
      df_source.createOrReplaceTempView("source_data")
      spark.sql("insert into table_all_types_all_agg select * from source_data")

      val supportedAggs = "count" :: "avg" :: "sum" :: "min" :: "max" :: Nil
      val selected = supportedAggs
        .flatMap(agg => {
          Range(0, supportedTypes.size).map(i => s"${agg}(c_${supportedTypes.apply(i)})")
        })
        .filterNot(x => {
          List(
            // unsupported by spark, so it's ok
            "avg(c_date)",
            "avg(c_string)",
            "avg(c_boolean)",
            "sum(c_date)",
            "sum(c_string)",
            "sum(c_boolean)",
            // supported by spark, unsupported by gluten, need to fix
            // 1. byte/short/float/string case
            // 2. following has bug:
            "avg(c_int)",
            "avg(c_bigint)").contains(x)
        })
        .mkString(",")
      sqlStr = s"select $selected from table_all_types_all_agg"
      println(s"query sql is: $sqlStr")

      val df = spark.sql(sqlStr)
      expected = df.collect()
    }
    val df = spark.sql(sqlStr)
    checkAnswer(df, expected)
  }

  override protected val resourcePath: String = ""
}
