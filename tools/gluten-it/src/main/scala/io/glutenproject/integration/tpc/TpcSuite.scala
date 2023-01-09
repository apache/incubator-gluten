package io.glutenproject.integration.tpc

import java.io.File
import java.util.Scanner

import io.glutenproject.integration.stat.RamStat
import io.glutenproject.integration.tpc.TpcSuite.TestResultLine
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.{Level, LogManager}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerHelper
import org.apache.spark.sql.{GlutenSparkSessionSwitcher, GlutenTestUtils}

abstract class TpcSuite(
  private val testConf: SparkConf,
  private val baselineConf: SparkConf,
  private val scale: Double,
  private val fixedWidthAsDouble: Boolean,
  private val queryIds: Array[String],
  private val logLevel: Level,
  private val explain: Boolean,
  private val errorOnMemLeak: Boolean,
  private val enableHsUi: Boolean,
  private val hsUiPort: Int,
  private val cpus: Int,
  private val offHeapSize: String,
  private val iterations: Int,
  private val disableAqe: Boolean,
  private val disableBhj: Boolean,
  private val disableWscg: Boolean,
  private val useExistingData: Boolean) {

  System.setProperty("spark.testing", "true")
  resetLogLevel()

  protected val sessionSwitcher: GlutenSparkSessionSwitcher = new GlutenSparkSessionSwitcher(cpus, logLevel.toString)
  private val runner: TpcRunner = new TpcRunner(queryResource(), dataWritePath())

  // define initial configs
  sessionSwitcher.defaultConf().set("spark.sql.sources.useV1SourceList", "")
  sessionSwitcher.defaultConf().set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
  sessionSwitcher.defaultConf().set("spark.executor.heartbeatInterval", "3600000")
  sessionSwitcher.defaultConf().set("spark.network.timeout", "3601s")
  sessionSwitcher.defaultConf().set("spark.sql.broadcastTimeout", "1800")
  sessionSwitcher.defaultConf().set("spark.network.io.preferDirectBufs", "false")
  sessionSwitcher.defaultConf().set("spark.unsafe.exceptionOnMemoryLeak", s"$errorOnMemLeak")
  sessionSwitcher.defaultConf().set("spark.ui.enabled", "false")
  sessionSwitcher.defaultConf().set("spark.memory.offHeap.enabled", "true")
  sessionSwitcher.defaultConf().set("spark.memory.offHeap.size", offHeapSize)

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

  // register sessions
  sessionSwitcher.registerSession("test", testConf)
  sessionSwitcher.registerSession("baseline", baselineConf)

  def startHistoryServer(): Unit = {
    if (!enableHsUi) {
      return
    }
    val conf = new SparkConf()
    conf.remove("spark.testing")
    conf.set("spark.history.ui.port", s"$hsUiPort")
    conf.set("spark.history.fs.logDirectory", historyWritePath())
    HistoryServerHelper.startHistoryServer(conf)
  }

  def run(): Boolean = {
    // boot up history server
    startHistoryServer()

    // use vanilla spark to generate data
    resetLogLevel() // to prevent log level from being set by unknown external codes
    sessionSwitcher.useSession("baseline", "Data Gen")
    if (useExistingData) {
      println("Use existing data in " + dataWritePath())
    } else {
      val dataGen = createDataGen()
      dataGen.gen()
    }

    // run tests
    resetLogLevel() // to prevent log level from being set by unknown external codes

    val allQueries = allQueryIds()
    val results = (0 until iterations).flatMap { iteration =>
      println(s"Running tests (iteration $iteration)...")
      val runQueryIds = queryIds match {
        case Array("__all__") =>
          allQueries
        case _ =>
          queryIds
      }
      val allQueriesSet = allQueries.toSet
      runQueryIds.map { queryId =>
        if (!allQueriesSet.contains(queryId)) {
          throw new IllegalArgumentException(s"Query ID doesn't exist: $queryId")
        }
        runTpcQuery(queryId)
      }
    }.toList
    sessionSwitcher.close()
    val passedCount = results.count(l => l.testPassed)
    val count = results.count(_ => true)

    // RAM stats
    println("Performing GC to collect RAM statistics... ")
    System.gc()
    System.gc()
    printf("RAM statistics: JVM Heap size: %d KiB (total %d KiB), Process RSS: %d KiB\n",
      RamStat.getJvmHeapUsed(), RamStat.getJvmHeapTotal(), RamStat.getProcessRamUsed())

    println("")
    println("Test report: ")
    println("")
    printf("Summary: %d out of %d queries passed. \n", passedCount, count)
    println("")
    val succeed = results.filter(_.testPassed)
    printResults(succeed)
    println("")

    if (passedCount == count) {
      println("No failed queries. ")
      println("")
    } else {
      println("Failed queries (a failed query with correct row count indicates value mismatches): ")
      println("")
      printResults(results.filter(!_.testPassed))
      println("")
    }

    var all = aggregate(results, "all")

    if (passedCount != count) {
      all = aggregate(succeed, "all succeed") ::: all
    }

    println("Overall: ")
    println("")
    printResults(all)
    println("")

    // wait for input, if history server was started
    if (enableHsUi) {
      printf("History server was running at port %d. Press enter to exit... \n", hsUiPort)
      print("> ")
      new Scanner(System.in).nextLine
    }

    if (passedCount != count) {
      return false
    }
    true
  }

  private def aggregate(succeed: List[TestResultLine], name: String): List[TestResultLine] = {
    if (succeed.isEmpty) {
      return Nil
    }
    List(succeed.reduce((r1, r2) => TestResultLine(name, testPassed = true,
      if (r1.expectedRowCount.nonEmpty && r2.expectedRowCount.nonEmpty)
        Some(r1.expectedRowCount.get + r2.expectedRowCount.get)
      else None,
      if (r1.actualRowCount.nonEmpty && r2.actualRowCount.nonEmpty)
        Some(r1.actualRowCount.get + r2.actualRowCount.get)
      else None,
      if (r1.expectedExecutionTimeMillis.nonEmpty && r2.expectedExecutionTimeMillis.nonEmpty)
        Some(r1.expectedExecutionTimeMillis.get + r2.expectedExecutionTimeMillis.get)
      else None,
      if (r1.actualExecutionTimeMillis.nonEmpty && r2.actualExecutionTimeMillis.nonEmpty)
        Some(r1.actualExecutionTimeMillis.get + r2.actualExecutionTimeMillis.get)
      else None, None)))
  }

  private def resetLogLevel(): Unit = {
    LogManager.getRootLogger.setLevel(logLevel)
  }

  private def printResults(results: List[TestResultLine]): Unit = {
    printf("|%15s|%15s|%30s|%30s|%30s|%30s|%30s|\n", "Query ID", "Was Passed", "Expected Row Count",
      "Actual Row Count", "Baseline Query Time (Millis)", "Query Time (Millis)", "Query Time Variation")
    results.foreach { line =>
      val timeVariation =
        if (line.expectedExecutionTimeMillis.nonEmpty && line.actualExecutionTimeMillis.nonEmpty) {
          Some(((line.expectedExecutionTimeMillis.get - line.actualExecutionTimeMillis.get).toDouble
            / line.actualExecutionTimeMillis.get.toDouble) * 100)
        } else None
      printf("|%15s|%15s|%30s|%30s|%30s|%30s|%30s|\n", line.queryId, line.testPassed,
        line.expectedRowCount.getOrElse("N/A"),
        line.actualRowCount.getOrElse("N/A"),
        line.expectedExecutionTimeMillis.getOrElse("N/A"),
        line.actualExecutionTimeMillis.getOrElse("N/A"),
        timeVariation.map("%15.2f%%".format(_)).getOrElse("N/A"))
    }
  }

  private def runTpcQuery(id: String): TestResultLine = {
    println(s"Running query: $id...")
    try {
      val baseLineDesc = "Vanilla Spark %s %s".format(desc(), id)
      sessionSwitcher.useSession("baseline", baseLineDesc)
      runner.createTables(sessionSwitcher.spark())
      val expected = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = explain,
        baseLineDesc)
      val expectedRows = expected.rows
      val testDesc = "Gluten Spark %s %s".format(desc(), id)
      sessionSwitcher.useSession("test", testDesc)
      runner.createTables(sessionSwitcher.spark())
      val result = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = explain,
        testDesc)
      val resultRows = result.rows
      val error = GlutenTestUtils.compareAnswers(resultRows, expectedRows, sort = true)
      if (error.isEmpty) {
        println(s"Successfully ran query $id, result check was passed. " +
          s"Returned row count: ${resultRows.length}, expected: ${expectedRows.length}")
        return TestResultLine(id, testPassed = true, Some(expectedRows.length), Some(resultRows.length),
          Some(expected.executionTimeMillis), Some(result.executionTimeMillis), None)
      }
      println(s"Error running query $id, result check was not passed. " +
        s"Returned row count: ${resultRows.length}, expected: ${expectedRows.length}, error: ${error.get}")
      TestResultLine(id, testPassed = false, Some(expectedRows.length), Some(resultRows.length),
        Some(expected.executionTimeMillis), Some(result.executionTimeMillis), error)
    } catch {
      case e: Exception =>
        val error = Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}")
        println(s"Error running query $id. " +
          s" Error: ${error.get}")
        TestResultLine(id, testPassed = false, None, None, None, None, error)
    }
  }

  protected def dataWritePath(): String

  protected def historyWritePath(): String

  protected def createDataGen(): DataGen

  protected def allQueryIds(): Array[String]

  protected def queryResource(): String

  protected def typeModifiers(): List[TypeModifier]

  protected def desc(): String

}

object TpcSuite {
  case class TestResultLine(
    queryId: String,
    testPassed: Boolean,
    expectedRowCount: Option[Long],
    actualRowCount: Option[Long],
    expectedExecutionTimeMillis: Option[Long],
    actualExecutionTimeMillis: Option[Long],
    errorMessage: Option[String])
}

