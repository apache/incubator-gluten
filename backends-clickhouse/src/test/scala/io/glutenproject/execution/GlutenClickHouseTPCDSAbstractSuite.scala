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

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import io.glutenproject.GlutenConfig
import io.glutenproject.benchmarks.GenTPCDSTableScripts
import io.glutenproject.utils.UTSystemParameters
import org.apache.commons.io.FileUtils

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseLog
import org.apache.spark.sql.types.DoubleType

abstract class GlutenClickHouseTPCDSAbstractSuite
    extends WholeStageTransformerSuite
    with Logging {

  override protected val backend: String = "ch"
  override protected val resourcePath: String = UTSystemParameters.getTpcdsDataPath() + "/"
  override protected val fileFormat: String = "parquet"

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val basePath: String = rootPath + "unit-tests-working-home"

  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

  protected val tablesPath: String = resourcePath
  protected val tpcdsQueries: String
  protected val queriesResults: String

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
    createTPCDSTables()
  }

  override protected def createTPCHNotNullTables(): Unit = {}

  protected def createTPCDSTables(): Unit = {
    val parquetTables =
      GenTPCDSTableScripts.genTPCDSParquetTables("tpcdsdb", resourcePath, "", "")

    for (sql <- parquetTables) {
      spark.sql(sql).show(10, false)
    }
    spark.sql("use tpcdsdb;")
    val result = spark.sql(s"""
         | show tables;
         |""".stripMargin).collect()
    assert(result.size == 24)
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
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.getClickHouseLibPath())
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.gluten.sql.columnar.forceshuffledhashjoin", "true")
      .set("spark.sql.warehouse.dir", warehouse)
    /* .set("spark.sql.catalogImplementation", "hive")
    .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${
      metaStorePathAbsolute + "/metastore_db"};create=true") */
  }

  protected override def afterAll(): Unit = {
    ClickHouseLog.clearCache()
    super.afterAll()
    FileUtils.forceDelete(new File(basePath))
    // init GlutenConfig in the next beforeAll
    GlutenConfig.ins = null
  }

  protected def runTPCDSQuery(
      queryNum: Int,
      tpcdsQueries: String = tpcdsQueries,
      queriesResults: String = queriesResults,
      compareResult: Boolean = true)(customCheck: DataFrame => Unit): Unit = {
    val sqlStrArr = new ArrayBuffer[String]()
    if (queryNum == 14 || queryNum == 23 || queryNum == 24 || queryNum == 39) {
      var sqlNum = "q" + "%d".format(queryNum) + "-1"
      sqlStrArr += sqlNum
      sqlNum = "q" + "%d".format(queryNum) + "-2"
      sqlStrArr += sqlNum
    } else {
      val sqlNum = "q" + "%d".format(queryNum)
      sqlStrArr += sqlNum
    }

    for (sqlNum <- sqlStrArr) {
      val sqlFile = tpcdsQueries + "/" + sqlNum + ".sql"
      val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8").mkString
      val df = spark.sql(sqlStr)
      val result = df.collect()
      if (compareResult) {
        val schema = df.schema
        if (schema.exists(_.dataType == DoubleType)) {
          compareDoubleResult(sqlNum, result, schema, queriesResults)
        } else {
          compareResultStr(sqlNum, result, queriesResults)
        }
      }
      customCheck(df)
    }
  }
}
