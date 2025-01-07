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

import org.apache.gluten.integration.{QueryRunner, Suite, TableCreator}
import org.apache.gluten.integration.QueryRunner.QueryResult
import org.apache.gluten.integration.action.Actions.QuerySelector
import org.apache.gluten.integration.action.TableRender.RowParser.FieldAppender.RowAppender
import org.apache.gluten.integration.metrics.{MetricMapper, PlanMetric}
import org.apache.gluten.integration.stat.RamStat

import org.apache.spark.sql.SparkSession

case class Queries(
    scale: Double,
    genPartitionedData: Boolean,
    queries: QuerySelector,
    explain: Boolean,
    iterations: Int,
    randomKillTasks: Boolean,
    noSessionReuse: Boolean,
    metricsReporters: Seq[PlanMetric.Reporter])
  extends Action {
  import Queries._

  override def execute(suite: Suite): Boolean = {
    val runQueryIds = queries.select(suite)
    val runner: QueryRunner =
      new QueryRunner(suite.queryResource(), suite.dataWritePath(scale, genPartitionedData))
    val sessionSwitcher = suite.sessionSwitcher
    sessionSwitcher.useSession("test", "Run Queries")
    runner.createTables(suite.tableCreator(), sessionSwitcher.spark())
    val results = (0 until iterations).flatMap { iteration =>
      println(s"Running tests (iteration $iteration)...")
      runQueryIds.map { queryId =>
        try {
          Queries.runQuery(
            runner,
            suite.tableCreator(),
            sessionSwitcher.spark(),
            queryId,
            suite.desc(),
            explain,
            suite.getTestMetricMapper(),
            randomKillTasks)
        } finally {
          if (noSessionReuse) {
            sessionSwitcher.renewSession()
            runner.createTables(suite.tableCreator(), sessionSwitcher.spark())
          }
        }
      }
    }.toList

    val passedCount = results.count(l => l.queryResult.succeeded())
    val count = results.count(_ => true)
    val succeeded = results.filter(_.queryResult.succeeded())

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

    val sqlMetrics = succeeded.flatMap(_.queryResult.asSuccess().runResult.sqlMetrics)
    metricsReporters.foreach {
      r =>
        val report = r.toString(sqlMetrics)
        println(report)
        println("")
    }

    println("Test report: ")
    println("")
    printf("Summary: %d out of %d queries passed. \n", passedCount, count)
    println("")
    val all = succeeded.map(_.queryResult).asSuccesses().agg("all").map(s => TestResultLine(s))
    Queries.printResults(succeeded ++ all)
    println("")

    if (passedCount == count) {
      println("No failed queries. ")
      println("")
    } else {
      println("Failed queries: ")
      println("")
      Queries.printResults(results.filter(!_.queryResult.succeeded()))
      println("")
    }

    if (passedCount != count) {
      return false
    }
    true
  }
}

object Queries {
  case class TestResultLine(queryResult: QueryResult)

  object TestResultLine {
    implicit object Parser extends TableRender.RowParser[TestResultLine] {
      override def parse(rowAppender: RowAppender, line: TestResultLine): Unit = {
        val inc = rowAppender.incremental()
        inc.next().write(line.queryResult.caseId())
        inc.next().write(line.queryResult.succeeded())
        line.queryResult match {
          case QueryRunner.Success(_, runResult) =>
            inc.next().write(runResult.rows.size)
            inc.next().write(runResult.planningTimeMillis)
            inc.next().write(runResult.executionTimeMillis)
          case QueryRunner.Failure(_, error) =>
            inc.next().write(None)
            inc.next().write(None)
            inc.next().write(None)
        }
      }
    }
  }

  private def printResults(results: Seq[TestResultLine]): Unit = {
    val render = TableRender.plain[TestResultLine](
      "Query ID",
      "Was Passed",
      "Row Count",
      "Plan Time (Millis)",
      "Query Time (Millis)")

    results.foreach(line => render.appendRow(line))

    render.print(System.out)
  }

  private def runQuery(
      runner: QueryRunner,
      creator: TableCreator,
      session: SparkSession,
      id: String,
      desc: String,
      explain: Boolean,
      metricMapper: MetricMapper,
      randomKillTasks: Boolean): TestResultLine = {
    println(s"Running query: $id...")
    val testDesc = "Query %s [%s]".format(desc, id)
    val result =
      runner.runQuery(
        session,
        testDesc,
        id,
        explain = explain,
        sqlMetricMapper = metricMapper,
        randomKillTasks = randomKillTasks)
    TestResultLine(result)
  }
}
