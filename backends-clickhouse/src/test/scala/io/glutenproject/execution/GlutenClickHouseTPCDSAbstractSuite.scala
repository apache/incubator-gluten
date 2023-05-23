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
import io.glutenproject.benchmarks.GenTPCDSTableScripts
import io.glutenproject.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseLog
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.commons.io.FileUtils

import java.io.File
import java.util

import scala.io.Source
import scala.language.postfixOps

abstract class GlutenClickHouseTPCDSAbstractSuite extends WholeStageTransformerSuite with Logging {
  private var _spark: SparkSession = null

  override protected def spark: SparkSession = _spark
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

  protected val tpcdsAllQueries: Seq[String] =
    Range
      .inclusive(1, 99)
      .flatMap(
        queryNum => {
          if (queryNum == 14 || queryNum == 23 || queryNum == 24 || queryNum == 39) {
            Seq("q" + "%d".format(queryNum) + "a", "q" + "%d".format(queryNum) + "b")
          } else {
            Seq("q" + "%d".format(queryNum))
          }
        })

  protected val excludedTpcdsQueries: Set[String] = Set(
    "q5", // attribute binding failed.
    "q14a", // inconsistent results
    "q14b", // inconsistent results
    "q17", // inconsistent results
    "q18", // inconsistent results
    "q31", // inconsistent results
    "q32", // attribute binding failed.
    "q36", // attribute binding failed.
    "q49", // inconsistent results
    "q61", // inconsistent results
    "q67", // inconsistent results
    "q70", // attribute binding failed.
    "q71", // inconsistent results, unstable order
    "q75", // attribute binding failed.
    "q77", // inconsistent results
    "q78", // inconsistent results
    "q80", // fatal
    "q83", // decimal error
    "q90", // inconsistent results(decimal)
    "q92" // attribute binding failed.
  )

  protected val independentTestTpcdsQueries: Set[String] = Set("q9", "q21")

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

  override protected def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = SparkSession
        .builder()
        .appName("Gluten-UT-TPC_DS")
        .master(s"local[8]")
        .config(sparkConf)
        .getOrCreate()
    }
  }

  override protected def createTPCHNotNullTables(): Unit = {}

  protected def createTPCDSTables(): Unit = {
    val parquetTables =
      GenTPCDSTableScripts.genTPCDSParquetTables("tpcdsdb", resourcePath, "", "")

    for (sql <- parquetTables) {
      spark.sql(sql)
    }
    spark.sql("use tpcdsdb;")
    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
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
      .set("spark.gluten.sql.columnar.columnarToRow", "true")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.getClickHouseLibPath())
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
      .set("spark.sql.warehouse.dir", warehouse)
    /* .set("spark.sql.catalogImplementation", "hive")
    .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${
      metaStorePathAbsolute + "/metastore_db"};create=true") */
  }

  override protected def afterAll(): Unit = {
    ClickHouseLog.clearCache()

    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }

    FileUtils.forceDelete(new File(basePath))
    // init GlutenConfig in the next beforeAll
    GlutenConfig.ins = null
  }

  protected def runTPCDSQuery(
      queryNum: String,
      tpcdsQueries: String = tpcdsQueries,
      queriesResults: String = queriesResults,
      compareResult: Boolean = true)(customCheck: DataFrame => Unit): Unit = {

    val sqlFile = tpcdsQueries + "/" + queryNum + ".sql"
    val df = spark.sql(Source.fromFile(new File(sqlFile), "UTF-8").mkString)

    if (compareResult) {
      val fields = new util.ArrayList[StructField]()
      for (elem <- df.schema) {
        fields.add(
          StructField
            .apply(elem.name + fields.size().toString, elem.dataType, elem.nullable, elem.metadata))
      }

      var expectedAnswer: Seq[Row] = null
      withSQLConf(vanillaSparkConfs(): _*) {
        expectedAnswer = spark.read
          .option("delimiter", "|-|")
          .option("nullValue", "null")
          .schema(StructType.apply(fields))
          .csv(queriesResults + "/" + queryNum + ".out")
          .toDF()
          .collect()
      }
      checkAnswer(df, expectedAnswer)
    }
    customCheck(df)
  }
}
