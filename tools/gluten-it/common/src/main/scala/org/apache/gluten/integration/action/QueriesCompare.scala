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
package org.apache.gluten.integration.action

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.gluten.integration.QueryRunner.QueryResult
import org.apache.gluten.integration.action.Actions.QuerySelector
import org.apache.gluten.integration.action.QueriesCompare.TestResultLine
import org.apache.gluten.integration.action.TableRender.RowParser.FieldAppender.RowAppender
import org.apache.gluten.integration.stat.RamStat
import org.apache.gluten.integration.{QueryRunner, Suite, TableCreator}
import org.apache.spark.sql.{RunResult, SparkSession, SparkSessionSwitcher, TestUtils}

case class QueriesCompare(
    scale: Double,
    genPartitionedData: Boolean,
    queries: QuerySelector,
    explain: Boolean,
    iterations: Int)
    extends Action {

  override def execute(suite: Suite): Boolean = {
    val runner: QueryRunner =
      new QueryRunner(suite.queryResource(), suite.dataWritePath(scale, genPartitionedData))
    val runQueryIds = queries.select(suite)
    val sessionSwitcher = suite.sessionSwitcher

    sessionSwitcher.useSession("baseline", "Run Baseline Queries")
    runner.createTables(suite.tableCreator(), sessionSwitcher.spark())
    val baselineResults = (0 until iterations).flatMap { iteration =>
      runQueryIds.map { queryId =>
        println(s"Running baseline query $queryId (iteration $iteration)...")
        QueriesCompare.runBaselineQuery(runner, sessionSwitcher.spark(), suite.desc(), queryId, explain)
      }
    }.toList

    sessionSwitcher.useSession("test", "Run Test Queries")
    runner.createTables(suite.tableCreator(), sessionSwitcher.spark())
    val testResults = (0 until iterations).flatMap { iteration =>
      runQueryIds.map { queryId =>
        println(s"Running test query $queryId (iteration $iteration)...")
        QueriesCompare.runTestQuery(runner, sessionSwitcher.spark(), suite.desc(), queryId, explain)
      }
    }.toList

    assert(baselineResults.size == testResults.size)

    val results: Seq[TestResultLine] = baselineResults.zip(testResults).map { case (b, t) =>
      assert(b.caseId() == t.caseId())
      TestResultLine(b.caseId(), b, t)
    }

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
      RamStat.getProcessRamUsed())

    println("")
    println("Test report: ")
    println("")
    printf("Summary: %d out of %d queries passed. \n", passedCount, count)
    println("")
    val succeeded = results.filter(_.testPassed)
    val all = succeeded match {
      case Nil => None
      case several =>
        val allExpected = several.map(_.expected).asSuccesses().agg("all expected").get
        val allActual = several.map(_.actual).asSuccesses().agg("all actual").get
        Some(TestResultLine("all", allExpected, allActual))
    }
    QueriesCompare.printResults(succeeded ++ all)
    println("")

    if (passedCount == count) {
      println("No failed queries. ")
      println("")
    } else {
      println(
        "Failed queries (a failed query with correct row count indicates value mismatches): ")
      println("")
      QueriesCompare.printResults(results.filter(!_.testPassed))
      println("")
    }

    if (passedCount != count) {
      return false
    }
    true
  }
}

object QueriesCompare {
  case class TestResultLine(queryId: String, expected: QueryResult, actual: QueryResult) {
    val testPassed: Boolean = {
      expected.succeeded() && actual.succeeded() &&
      TestUtils
        .compareAnswers(
          expected.asSuccess().runResult.rows,
          actual.asSuccess().runResult.rows,
          sort = true)
        .isEmpty
    }
  }

  object TestResultLine {
    implicit object Parser extends TableRender.RowParser[TestResultLine] {
      override def parse(rowAppender: RowAppender, line: TestResultLine): Unit = {
        val inc = rowAppender.incremental()
        inc.next().write(line.queryId)
        inc.next().write(line.testPassed)
        inc.next().write(line.expected.asSuccessOption().map(_.runResult.rows.size))
        inc.next().write(line.actual.asSuccessOption().map(_.runResult.rows.size))
        inc.next().write(line.expected.asSuccessOption().map(_.runResult.planningTimeMillis))
        inc.next().write(line.actual.asSuccessOption().map(_.runResult.planningTimeMillis))
        inc.next().write(line.expected.asSuccessOption().map(_.runResult.executionTimeMillis))
        inc.next().write(line.actual.asSuccessOption().map(_.runResult.executionTimeMillis))

        val speedUp =
          if (line.expected.succeeded() && line.actual.succeeded()) {
            Some(
              ((line.expected.asSuccess().runResult.executionTimeMillis - line.actual
                .asSuccess()
                .runResult
                .executionTimeMillis).toDouble
                / line.actual.asSuccess().runResult.executionTimeMillis) * 100)
          } else None
        inc.next().write(speedUp.map("%.2f%%".format(_)))
      }
    }
  }

  private def printResults(results: Seq[TestResultLine]): Unit = {
    import org.apache.gluten.integration.action.TableRender.Field._

    val render = TableRender.create[TestResultLine](
      Leaf("Query ID"),
      Leaf("Passed"),
      Branch("Row Count", List(Leaf("Vanilla"), Leaf("Gluten"))),
      Branch("Planning Time (Millis)", List(Leaf("Vanilla"), Leaf("Gluten"))),
      Branch("Query Time (Millis)", List(Leaf("Vanilla"), Leaf("Gluten"))),
      Leaf("Speedup"))

    results.foreach { line =>
      render.appendRow(line)
    }

    render.print(System.out)
  }

  private def runBaselineQuery(
      runner: QueryRunner,
      session: SparkSession,
      desc: String,
      id: String,
      explain: Boolean): QueryResult = {
    val testDesc = "Baseline %s [%s]".format(desc, id)
    val result = runner.runQuery(session, testDesc, id, explain = explain)
    result
  }

  private def runTestQuery(
      runner: QueryRunner,
      session: SparkSession,
      desc: String,
      id: String,
      explain: Boolean): QueryResult = {
    val testDesc = "Query %s [%s]".format(desc, id)
    val result = runner.runQuery(session, testDesc, id, explain = explain)
    result
  }
}
