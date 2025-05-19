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
package org.apache.gluten.integration

import org.apache.gluten.integration.Constants.TYPE_MODIFIER_DECIMAL_AS_DOUBLE
import org.apache.gluten.integration.action.Action
import org.apache.gluten.integration.metrics.MetricMapper
import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerHelper
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.ConfUtils.ConfImplicits._
import org.apache.spark.sql.SparkSessionSwitcher
import org.apache.log4j.{Level, LogManager}

import java.io.File
import java.util.Scanner

abstract class Suite(
    private val masterUrl: String,
    private val actions: Array[Action],
    private val testConf: SparkConf,
    private val baselineConf: SparkConf,
    private val extraSparkConf: Map[String, String],
    private val logLevel: Level,
    private val errorOnMemLeak: Boolean,
    private val enableUi: Boolean,
    private val enableHsUi: Boolean,
    private val hsUiPort: Int,
    private val disableAqe: Boolean,
    private val disableBhj: Boolean,
    private val disableWscg: Boolean,
    private val shufflePartitions: Int,
    private val scanPartitions: Int,
    private val decimalAsDouble: Boolean,
    private val baselineMetricMapper: MetricMapper,
    private val testMetricMapper: MetricMapper) {

  resetLogLevel()

  private var hsUiBoundPort: Int = -1

  private[integration] val sessionSwitcher: SparkSessionSwitcher =
    new SparkSessionSwitcher(masterUrl, logLevel.toString)

  // define initial configs
  sessionSwitcher.addDefaultConf("spark.sql.sources.useV1SourceList", "")
  sessionSwitcher.addDefaultConf("spark.sql.shuffle.partitions", s"$shufflePartitions")
  sessionSwitcher.addDefaultConf("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
  sessionSwitcher.addDefaultConf("spark.executor.heartbeatInterval", "10s")
  sessionSwitcher.addDefaultConf("spark.worker.timeout", "3600")
  sessionSwitcher.addDefaultConf("spark.executor.metrics.pollingInterval", "0")
  sessionSwitcher.addDefaultConf("spark.network.timeout", "3601s")
  sessionSwitcher.addDefaultConf("spark.sql.broadcastTimeout", "1800")
  sessionSwitcher.addDefaultConf("spark.network.io.preferDirectBufs", "false")
  sessionSwitcher.addDefaultConf("spark.unsafe.exceptionOnMemoryLeak", s"$errorOnMemLeak")

  if (!enableUi) {
    sessionSwitcher.addDefaultConf("spark.ui.enabled", "false")
  }

  if (enableHsUi) {
    if (!new File(historyWritePath()).exists() && !new File(historyWritePath()).mkdirs()) {
      throw new RuntimeException(
        "Unable to create history directory: " +
          historyWritePath())
    }
    sessionSwitcher.addDefaultConf("spark.eventLog.enabled", "true")
    sessionSwitcher.addDefaultConf("spark.eventLog.dir", historyWritePath())
  }

  if (disableAqe) {
    sessionSwitcher.addDefaultConf("spark.sql.adaptive.enabled", "false")
  }

  if (disableBhj) {
    sessionSwitcher.addDefaultConf("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  if (disableWscg) {
    sessionSwitcher.addDefaultConf("spark.sql.codegen.wholeStage", "false")
  }

  if (scanPartitions != -1) {
    // Scan partition number.
    sessionSwitcher.addDefaultConf("spark.sql.files.maxPartitionBytes", s"${ByteUnit.PiB.toBytes(1L)}")
    sessionSwitcher.addDefaultConf("spark.sql.files.openCostInBytes", "0")
    sessionSwitcher.addDefaultConf("spark.sql.files.minPartitionNum", s"${(scanPartitions - 1).max(1)}")
  }

  // register sessions
  sessionSwitcher.registerSession("test", testConf)
  sessionSwitcher.registerSession("baseline", baselineConf)

  extraSparkConf.toStream.foreach {
    kv => sessionSwitcher.addExtraConf(kv._1, kv._2)
  }

  private def startHistoryServer(): Int = {
    val hsConf = new SparkConf(false)
    hsConf.setWarningOnOverriding("spark.history.ui.port", s"$hsUiPort")
    hsConf.setWarningOnOverriding("spark.history.fs.logDirectory", historyWritePath())
    HistoryServerHelper.startHistoryServer(hsConf)
  }

  // boot up history server
  if (enableHsUi) {
    hsUiBoundPort = startHistoryServer()
  }

  def run(): Boolean = {
    val succeed = actions.forall {
      action =>
        resetLogLevel() // to prevent log level from being set by unknown external codes
        action.execute(this)
    }
    succeed
  }

  def close(): Unit = {
    sessionSwitcher.close()
    // wait for input, if history server was started
    if (enableHsUi) {
      printf("History server was running at port %d. Press enter to exit... \n", hsUiBoundPort)
      print("> ")
      new Scanner(System.in).nextLine
    }
  }

  def tableCreator(): TableCreator

  private def resetLogLevel(): Unit = {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel.toString)
    LogManager.getRootLogger.setLevel(logLevel)
  }

  private[integration] def getBaselineConf(): SparkConf = {
    baselineConf.clone()
  }

  private[integration] def getTestConf(): SparkConf = {
    testConf.clone()
  }

  private[integration] def getBaselineMetricMapper(): MetricMapper = {
    baselineMetricMapper
  }

  private[integration] def getTestMetricMapper(): MetricMapper = {
    testMetricMapper
  }

  private[integration] def typeModifiers(): List[TypeModifier] = {
    if (decimalAsDouble) List(TYPE_MODIFIER_DECIMAL_AS_DOUBLE) else List()
  }

  protected def historyWritePath(): String

  private[integration] def dataWritePath(scale: Double, genPartitionedData: Boolean): String

  private[integration] def createDataGen(scale: Double, genPartitionedData: Boolean): DataGen

  private[integration] def queryResource(): String

  private[integration] def allQueryIds(): Array[String]

  private[integration] def desc(): String
}

object Suite {}
