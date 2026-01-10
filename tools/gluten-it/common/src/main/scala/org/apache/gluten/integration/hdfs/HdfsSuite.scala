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
package org.apache.gluten.integration.hdfs

import org.apache.gluten.integration.{DataGen, QuerySet, Suite, TableCreator}
import org.apache.gluten.integration.action.Action
import org.apache.gluten.integration.metrics.MetricMapper

import org.apache.spark.SparkConf

import org.apache.log4j.Level

import java.io.File

class HdfsSuite(
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
    val dataGenFeatures: Seq[String],
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
  import HdfsSuite._

  override protected def historyWritePath(): String = HISTORY_WRITE_PATH

  override private[integration] def dataWritePath(): String = {
    if (dataDir.startsWith("hdfs://")) {
      s"$dataDir/hdfs-test-data-$dataScale-$dataSource"
    } else {
      new File(dataDir).toPath
        .resolve(s"$HDFS_WRITE_RELATIVE_PATH-$dataScale-$dataSource")
        .toFile
        .getAbsolutePath
    }
  }

  override private[integration] def createDataGen(): DataGen = {
    checkDataGenArgs(dataSource, dataScale, genPartitionedData)
    new HdfsDataGen(sessionSwitcher.spark(), shufflePartitions, dataSource, dataWritePath())
  }

  override private[integration] def allQueries(): QuerySet = {
    QuerySet.readFromResource("/hdfs-queries", HdfsSuite.ALL_QUERY_IDS)
  }

  override private[integration] def desc(): String = "HDFS Integration"

  override def tableCreator(): TableCreator = HdfsTableCreator
}
object HdfsSuite {
  private val HDFS_WRITE_RELATIVE_PATH = "hdfs-generated"
  private val ALL_QUERY_IDS = Array("q1")
  private val HISTORY_WRITE_PATH = "/tmp/hdfs-history"

  private def checkDataGenArgs(
      dataSource: String,
      scale: Double,
      genPartitionedData: Boolean): Unit = {
    require(
      Set("parquet", "delta").contains(dataSource),
      s"Data source type $dataSource is not supported by HDFS suite")
    require(!genPartitionedData, "HDFS suite doesn't support generating partitioned data")
  }
}
