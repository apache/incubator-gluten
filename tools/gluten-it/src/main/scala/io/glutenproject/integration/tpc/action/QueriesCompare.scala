package io.glutenproject.integration.tpc.action

import io.glutenproject.integration.stat.RamStat
import io.glutenproject.integration.tpc.{TpcRunner, TpcSuite}

import org.apache.spark.sql.{GlutenSparkSessionSwitcher, GlutenTestUtils}

import org.apache.commons.lang3.exception.ExceptionUtils

case class QueriesCompare(
    scale: Double,
    queryIds: Array[String],
    explain: Boolean,
    iterations: Int)
  extends Action {

  override def execute(tpcSuite: TpcSuite): Boolean = {
    val runner: TpcRunner = new TpcRunner(tpcSuite.queryResource(), tpcSuite.dataWritePath(scale))
    val allQueries = tpcSuite.allQueryIds()
    val results = (0 until iterations).flatMap {
      iteration =>
        println(s"Running tests (iteration $iteration)...")
        val runQueryIds = queryIds match {
          case Array("__all__") =>
            allQueries
          case _ =>
            queryIds
        }
        val allQueriesSet = allQueries.toSet
        runQueryIds.map {
          queryId =>
            if (!allQueriesSet.contains(queryId)) {
              throw new IllegalArgumentException(s"Query ID doesn't exist: $queryId")
            }
            QueriesCompare.runTpcQuery(
              queryId,
              explain,
              tpcSuite.desc(),
              tpcSuite.sessionSwitcher,
              runner)
        }
    }.toList

    val passedCount = results.count(l => l.testPassed)
    val count = results.count(_ => true)

    // RAM stats
    println("Performing GC to collect RAM statistics... ")
    System.gc()
    System.gc()
    printf(
      "RAM statistics: JVM Heap size: %d KiB (total %d KiB), Process RSS: %d KiB\n",
      RamStat.getJvmHeapUsed(),
      RamStat.getJvmHeapTotal(),
      RamStat.getProcessRamUsed()
    )

    println("")
    println("Test report: ")
    println("")
    printf("Summary: %d out of %d queries passed. \n", passedCount, count)
    println("")
    val succeed = results.filter(_.testPassed)
    QueriesCompare.printResults(succeed)
    println("")

    if (passedCount == count) {
      println("No failed queries. ")
      println("")
    } else {
      println("Failed queries (a failed query with correct row count indicates value mismatches): ")
      println("")
      QueriesCompare.printResults(results.filter(!_.testPassed))
      println("")
    }

    var all = QueriesCompare.aggregate(results, "all")

    if (passedCount != count) {
      all = QueriesCompare.aggregate(succeed, "all succeed") ::: all
    }

    println("Overall: ")
    println("")
    QueriesCompare.printResults(all)
    println("")

    if (passedCount != count) {
      return false
    }
    true
  }
}

object QueriesCompare {
  case class TestResultLine(
      queryId: String,
      testPassed: Boolean,
      expectedRowCount: Option[Long],
      actualRowCount: Option[Long],
      expectedExecutionTimeMillis: Option[Long],
      actualExecutionTimeMillis: Option[Long],
      errorMessage: Option[String])

  private def printResults(results: List[TestResultLine]): Unit = {
    printf(
      "|%15s|%15s|%30s|%30s|%30s|%30s|%30s|\n",
      "Query ID",
      "Was Passed",
      "Expected Row Count",
      "Actual Row Count",
      "Baseline Query Time (Millis)",
      "Query Time (Millis)",
      "Query Time Variation"
    )
    results.foreach {
      line =>
        val timeVariation =
          if (
            line.expectedExecutionTimeMillis.nonEmpty && line.actualExecutionTimeMillis.nonEmpty
          ) {
            Some(
              ((line.expectedExecutionTimeMillis.get - line.actualExecutionTimeMillis.get).toDouble
                / line.actualExecutionTimeMillis.get.toDouble) * 100)
          } else None
        printf(
          "|%15s|%15s|%30s|%30s|%30s|%30s|%30s|\n",
          line.queryId,
          line.testPassed,
          line.expectedRowCount.getOrElse("N/A"),
          line.actualRowCount.getOrElse("N/A"),
          line.expectedExecutionTimeMillis.getOrElse("N/A"),
          line.actualExecutionTimeMillis.getOrElse("N/A"),
          timeVariation.map("%15.2f%%".format(_)).getOrElse("N/A")
        )
    }
  }

  private def aggregate(succeed: List[TestResultLine], name: String): List[TestResultLine] = {
    if (succeed.isEmpty) {
      return Nil
    }
    List(
      succeed.reduce(
        (r1, r2) =>
          TestResultLine(
            name,
            testPassed = true,
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
            else None,
            None
          )))
  }

  private[tpc] def runTpcQuery(
      id: String,
      explain: Boolean,
      desc: String,
      sessionSwitcher: GlutenSparkSessionSwitcher,
      runner: TpcRunner): TestResultLine = {
    println(s"Running query: $id...")
    try {
      val baseLineDesc = "Vanilla Spark %s %s".format(desc, id)
      sessionSwitcher.useSession("baseline", baseLineDesc)
      runner.createTables(sessionSwitcher.spark())
      val expected =
        runner.runTpcQuery(sessionSwitcher.spark(), id, explain = explain, baseLineDesc)
      val expectedRows = expected.rows
      val testDesc = "Gluten Spark %s %s".format(desc, id)
      sessionSwitcher.useSession("test", testDesc)
      runner.createTables(sessionSwitcher.spark())
      val result = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = explain, testDesc)
      val resultRows = result.rows
      val error = GlutenTestUtils.compareAnswers(resultRows, expectedRows, sort = true)
      if (error.isEmpty) {
        println(
          s"Successfully ran query $id, result check was passed. " +
            s"Returned row count: ${resultRows.length}, expected: ${expectedRows.length}")
        return TestResultLine(
          id,
          testPassed = true,
          Some(expectedRows.length),
          Some(resultRows.length),
          Some(expected.executionTimeMillis),
          Some(result.executionTimeMillis),
          None)
      }
      println(s"Error running query $id, result check was not passed. " +
        s"Returned row count: ${resultRows.length}, expected: ${expectedRows.length}, error: ${error.get}")
      TestResultLine(
        id,
        testPassed = false,
        Some(expectedRows.length),
        Some(resultRows.length),
        Some(expected.executionTimeMillis),
        Some(result.executionTimeMillis),
        error)
    } catch {
      case e: Exception =>
        val error = Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}")
        println(
          s"Error running query $id. " +
            s" Error: ${error.get}")
        TestResultLine(id, testPassed = false, None, None, None, None, error)
    }
  }
}
