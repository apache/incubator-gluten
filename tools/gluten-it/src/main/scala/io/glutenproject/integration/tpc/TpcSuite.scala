package io.glutenproject.integration.tpc

import io.glutenproject.integration.tpc.action.Action

import java.io.File
import java.util.Scanner
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerHelper
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.GlutenSparkSessionSwitcher

abstract class TpcSuite(
  private val actions: Array[Action],
  private val testConf: SparkConf,
  private val baselineConf: SparkConf,
  private val fixedWidthAsDouble: Boolean,
  private val logLevel: Level,
  private val errorOnMemLeak: Boolean,
  private val enableUi: Boolean,
  private val enableHsUi: Boolean,
  private val hsUiPort: Int,
  private val enableCeleborn: Boolean,
  private val cpus: Int,
  private val offHeapSize: String,
  private val disableAqe: Boolean,
  private val disableBhj: Boolean,
  private val disableWscg: Boolean,
  private val shufflePartitions: Int,
  private val minimumScanPartitions: Boolean) {

  System.setProperty("spark.testing", "true")
  resetLogLevel()

  private[tpc] val sessionSwitcher: GlutenSparkSessionSwitcher = new GlutenSparkSessionSwitcher(cpus, logLevel.toString)

  // define initial configs
  sessionSwitcher.defaultConf().set("spark.sql.sources.useV1SourceList", "")
  sessionSwitcher.defaultConf().set("spark.sql.shuffle.partitions", s"$shufflePartitions")
  sessionSwitcher.defaultConf().set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
  sessionSwitcher.defaultConf().set("spark.executor.heartbeatInterval", "3600000")
  sessionSwitcher.defaultConf().set("spark.network.timeout", "3601s")
  sessionSwitcher.defaultConf().set("spark.sql.broadcastTimeout", "1800")
  sessionSwitcher.defaultConf().set("spark.network.io.preferDirectBufs", "false")
  sessionSwitcher.defaultConf().set("spark.unsafe.exceptionOnMemoryLeak", s"$errorOnMemLeak")
  sessionSwitcher.defaultConf().set("spark.memory.offHeap.enabled", "true")
  sessionSwitcher.defaultConf().set("spark.memory.offHeap.size", offHeapSize)

  if (!enableUi) {
    sessionSwitcher.defaultConf().set("spark.ui.enabled", "false")
  }

  if (enableHsUi) {
    if (!new File(historyWritePath()).exists() && !new File(historyWritePath()).mkdirs()) {
      throw new RuntimeException("Unable to create history directory: " +
        historyWritePath())
    }
    sessionSwitcher.defaultConf().set("spark.eventLog.enabled", "true")
    sessionSwitcher.defaultConf().set("spark.eventLog.dir", historyWritePath())
  }

  if (disableAqe) {
    sessionSwitcher.defaultConf().set("spark.sql.adaptive.enabled", "false")
  }

  if (disableBhj) {
    sessionSwitcher.defaultConf().set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  if (disableWscg) {
    sessionSwitcher.defaultConf().set("spark.sql.codegen.wholeStage", "false")
  }

  if (minimumScanPartitions) {
    sessionSwitcher.defaultConf().set("spark.sql.files.maxPartitionBytes", s"${ByteUnit.PiB.toBytes(1L)}")
    sessionSwitcher.defaultConf().set("spark.sql.files.openCostInBytes", s"${ByteUnit.PiB.toBytes(1L)}")
    sessionSwitcher.defaultConf().set("spark.default.parallelism", "1")
  }

  if (enableCeleborn) {
    testConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.CelebornShuffleManager")
    testConf.set("spark.celeborn.shuffle.writer", "hash")
    testConf.set("spark.celeborn.push.replicate.enabled", "false")
    testConf.set("spark.shuffle.service.enabled", "false")
    testConf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    testConf.set("spark.dynamicAllocation.enabled", "false")
  }

  // register sessions
  sessionSwitcher.registerSession("test", testConf)
  sessionSwitcher.registerSession("baseline", baselineConf)

  def startHistoryServer(): Unit = {
    val conf = new SparkConf()
    conf.remove("spark.testing")
    conf.set("spark.history.ui.port", s"$hsUiPort")
    conf.set("spark.history.fs.logDirectory", historyWritePath())
    HistoryServerHelper.startHistoryServer(conf)
  }

  // boot up history server
  if (enableHsUi) {
    startHistoryServer()
  }

  def run(): Boolean = {
    val succeed = actions.forall { action =>
      resetLogLevel() // to prevent log level from being set by unknown external codes
      action.execute(this)
    }
    succeed
  }

  def close(): Unit = {
    sessionSwitcher.close()
    // wait for input, if history server was started
    if (enableHsUi) {
      printf("History server was running at port %d. Press enter to exit... \n", hsUiPort)
      print("> ")
      new Scanner(System.in).nextLine
    }
  }

  private def resetLogLevel(): Unit = {
    LogManager.getRootLogger.setLevel(logLevel)
  }

  protected def historyWritePath(): String

  private[tpc] def dataWritePath(scale: Double): String

  private[tpc] def createDataGen(scale: Double, genPartitionedData: Boolean): DataGen

  private[tpc] def queryResource(): String

  protected def typeModifiers(): List[TypeModifier]

  private[tpc] def allQueryIds(): Array[String]

  private[tpc] def desc(): String

}

