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
package org.apache.gluten.integration.clickbench

import org.apache.gluten.integration.{DataGen, Suite, TableCreator}
import org.apache.gluten.integration.action.Action
import org.apache.gluten.integration.metrics.MetricMapper

import org.apache.spark.SparkConf

import org.apache.log4j.Level

import java.io.File

/**
 * ClickBench: a Benchmark For Analytical Databases
 *
 * See the project: https://github.com/ClickHouse/ClickBench Site: https://benchmark.clickhouse.com/
 */
class ClickBenchSuite(
    val masterUrl: String,
    val actions: Array[Action],
    val testConf: SparkConf,
    val baselineConf: SparkConf,
    val extraSparkConf: Map[String, String],
    val logLevel: Level,
    val errorOnMemLeak: Boolean,
    val dataDir: String,
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
  import ClickBenchSuite._

  override protected def historyWritePath(): String = HISTORY_WRITE_PATH

  override private[integration] def dataWritePath(
      scale: Double,
      genPartitionedData: Boolean): String = {
    checkDataGenArgs(scale, genPartitionedData)
    new File(dataDir).toPath.resolve(DATA_WRITE_RELATIVE_PATH + s"-$scale").toFile.getAbsolutePath
  }

  override private[integration] def createDataGen(
      scale: Double,
      genPartitionedData: Boolean): DataGen = {
    new ClickBenchDataGen(sessionSwitcher.spark(), dataWritePath(scale, genPartitionedData))
  }

  override private[integration] def queryResource(): String = "/clickbench-queries"

  override private[integration] def allQueryIds(): Array[String] = ALL_QUERY_IDS

  override private[integration] def desc(): String = "ClickBench"

  override def tableCreator(): TableCreator = ClickBenchTableCreator
}

private object ClickBenchSuite {
  private val DATA_WRITE_RELATIVE_PATH = "clickbench-generated"
  private val HISTORY_WRITE_PATH = "/tmp/clickbench-history"
  private val ALL_QUERY_IDS = (1 to 43).map(i => s"q$i").toArray

  private def checkDataGenArgs(scale: Double, genPartitionedData: Boolean): Unit = {
    assert(scale == 1.0d, "ClickBench suite doesn't support scale factor other than 1")
    assert(!genPartitionedData, "ClickBench suite doesn't support generating partitioned data")
  }
}
