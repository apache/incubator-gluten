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
package org.apache.gluten.integration.h

import org.apache.gluten.integration.{DataGen, QuerySet, Suite, TableCreator}
import org.apache.gluten.integration.action.Action
import org.apache.gluten.integration.metrics.MetricMapper

import org.apache.spark.SparkConf

import org.apache.log4j.Level

import java.io.File

class TpchSuite(
    val masterUrl: String,
    val actions: Array[Action],
    val testConf: SparkConf,
    val baselineConf: SparkConf,
    val extraSparkConf: Map[String, String],
    val logLevel: Level,
    val errorOnMemLeak: Boolean,
    val dataSource: String,
    val dataDir: String,
    val dataScale: Double,
    val genPartitionedData: Boolean,
    val enableUi: Boolean,
    val enableHsUi: Boolean,
    val hsUiPort: Int,
    val disableAqe: Boolean,
    val disableBhj: Boolean,
    val disableWscg: Boolean,
    val shufflePartitions: Int,
    val scanPartitions: Int,
    val decimalAsDouble: Boolean,
    val baselineMetricMapper: MetricMapper,
    val testMetricMapper: MetricMapper)
  extends Suite(
    masterUrl,
    actions,
    testConf,
    baselineConf,
    extraSparkConf,
    logLevel,
    errorOnMemLeak,
    enableUi,
    enableHsUi,
    hsUiPort,
    disableAqe,
    disableBhj,
    disableWscg,
    shufflePartitions,
    scanPartitions,
    decimalAsDouble,
    baselineMetricMapper,
    testMetricMapper
  ) {
  import TpchSuite._

  override protected def historyWritePath(): String = HISTORY_WRITE_PATH

  override private[integration] def dataWritePath(): String =
    new File(dataDir).toPath
      .resolve(s"$TPCH_WRITE_RELATIVE_PATH-$dataScale-$dataSource")
      .toFile
      .getAbsolutePath

  override private[integration] def createDataGen(): DataGen = {
    checkDataGenArgs(dataSource, dataScale, genPartitionedData)
    new TpchDataGen(
      sessionSwitcher.spark(),
      dataScale,
      shufflePartitions,
      dataSource,
      dataWritePath(),
      typeModifiers())
  }

  override private[integration] def allQueries(): QuerySet = {
    QuerySet.readFromResource("/tpch-queries", TpchSuite.ALL_QUERY_IDS)
  }

  override private[integration] def desc(): String = "TPC-H"

  override def tableCreator(): TableCreator = TableCreator.discoverSchema()
}

object TpchSuite {
  private val TPCH_WRITE_RELATIVE_PATH = "tpch-generated"
  private val ALL_QUERY_IDS = Array(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21",
    "q22")
  private val HISTORY_WRITE_PATH = "/tmp/tpch-history"

  private def checkDataGenArgs(
      dataSource: String,
      scale: Double,
      genPartitionedData: Boolean): Unit = {
    require(
      Set("parquet", "delta").contains(dataSource),
      s"Data source type $dataSource is not supported by TPC-H suite")
    require(!genPartitionedData, "TPC-H suite doesn't support generating partitioned data")
  }
}
