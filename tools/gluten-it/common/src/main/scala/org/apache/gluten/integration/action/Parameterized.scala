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
import org.apache.gluten.integration.action.Actions.QuerySelector
import org.apache.gluten.integration.action.TableRender.RowParser.FieldAppender.RowAppender
import org.apache.gluten.integration.stat.RamStat
import org.apache.gluten.integration.{QueryRunner, Suite, TableCreator}
import org.apache.spark.sql.ConfUtils.ConfImplicits._
import org.apache.spark.sql.SparkSessionSwitcher

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Parameterized(
    scale: Double,
    genPartitionedData: Boolean,
    queries: QuerySelector,
    explain: Boolean,
    iterations: Int,
    warmupIterations: Int,
    configDimensions: Seq[Dim],
    excludedCombinations: Seq[Set[DimKv]],
    metrics: Array[String])
    extends Action {

  private def validateDims(configDimensions: Seq[Dim]): Unit = {
    if (configDimensions
          .map(dim => {
            dim.name
          })
          .toSet
          .size != configDimensions.size) {
      throw new IllegalStateException("Duplicated dimension name found")
    }

    configDimensions.foreach { dim =>
      if (dim.dimValues.map(dimValue => dimValue.name).toSet.size != dim.dimValues.size) {
        throw new IllegalStateException("Duplicated dimension value found")
      }
    }
  }

  private val coordinates: mutable.LinkedHashMap[Coordinate, Seq[(String, String)]] = {
    validateDims(configDimensions)
    val dimCount = configDimensions.size
    val coordinateMap = mutable.LinkedHashMap[Coordinate, Seq[(String, String)]]()

    def fillCoordinates(
        dimOffset: Int,
        intermediateCoordinates: Map[String, String],
        intermediateConf: Seq[(String, String)]): Unit = {
      if (dimOffset == dimCount) {
        // we got one coordinate
        excludedCombinations.foreach { ec: Set[DimKv] =>
          if (ec.forall { kv =>
                intermediateCoordinates.contains(kv.k) && intermediateCoordinates(kv.k) == kv.v
              }) {
            println(s"Coordinate ${Coordinate(intermediateCoordinates)} excluded by $ec.")
            return
          }
        }
        coordinateMap(Coordinate(intermediateCoordinates)) = intermediateConf
        return
      }
      val dim = configDimensions(dimOffset)
      dim.dimValues.foreach { dimValue =>
        fillCoordinates(
          dimOffset + 1,
          intermediateCoordinates + (dim.name -> dimValue.name),
          intermediateConf ++ dimValue.conf)
      }
    }

    fillCoordinates(0, Map(), Seq())

    coordinateMap
  }

  override def execute(suite: Suite): Boolean = {
    val runner: QueryRunner =
      new QueryRunner(suite.queryResource(), suite.dataWritePath(scale, genPartitionedData))
    val allQueries = suite.allQueryIds()

    val sessionSwitcher = suite.sessionSwitcher
    val testConf = suite.getTestConf()

    println("Prepared coordinates: ")
    coordinates.toList.map(_._1).zipWithIndex.foreach {
      case (c, idx) =>
        println(s"  $idx: $c")
    }
    coordinates.foreach { entry =>
      // register one session per coordinate
      val coordinate = entry._1
      val coordinateConf = entry._2
      val conf = testConf.clone()
      conf.setAllWarningOnOverriding(coordinateConf)
      sessionSwitcher.registerSession(coordinate.toString, conf)
    }

    val runQueryIds = queries.select(suite)

    // warm up
    (0 until warmupIterations).foreach { _ =>
      runQueryIds.foreach { queryId =>
        Parameterized.warmUp(suite.tableCreator(), queryId, suite.desc(), sessionSwitcher, runner)
      }
    }

    val results = coordinates.flatMap { entry =>
      val coordinate = entry._1
      val coordinateResults = (0 until iterations).flatMap { iteration =>
        println(s"Running tests (iteration $iteration) with coordinate $coordinate...")
        runQueryIds.map { queryId =>
          Parameterized.runQuery(
            runner,
            suite.tableCreator(),
            sessionSwitcher,
            queryId,
            coordinate,
            suite.desc(),
            explain,
            metrics)
        }
      }.toList
      coordinateResults
    }

    val dimNames = configDimensions.map(dim => dim.name)

    val passedCount = results.count(l => l.succeed)
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
    TestResultLines(dimNames, metrics, results.filter(_.succeed)).print()
    println("")

    if (passedCount == count) {
      println("No failed queries. ")
      println("")
    } else {
      println("Failed queries: ")
      println("")
      TestResultLines(dimNames, metrics, results.filter(!_.succeed)).print()
      println("")
    }

    if (passedCount != count) {
      return false
    }
    true
  }
}

case class DimKv(k: String, v: String)
case class Dim(name: String, dimValues: Seq[DimValue])
case class DimValue(name: String, conf: Seq[(String, String)])
case class Coordinate(coordinate: Map[String, String]) // [dim, dim value]

case class TestResultLine(
    queryId: String,
    succeed: Boolean,
    coordinate: Coordinate,
    rowCount: Option[Long],
    planningTimeMillis: Option[Long],
    executionTimeMillis: Option[Long],
    metrics: Map[String, Long],
    errorMessage: Option[String])

object TestResultLine {
  class Parser(dimNames: Seq[String], metricNames: Seq[String])
      extends TableRender.RowParser[TestResultLine] {
    override def parse(rowAppender: RowAppender, line: TestResultLine): Unit = {
      val inc = rowAppender.incremental()
      inc.next().write(line.queryId)
      inc.next().write(line.succeed)
      dimNames.foreach { dimName =>
        val coordinate = line.coordinate.coordinate
        if (!coordinate.contains(dimName)) {
          throw new IllegalStateException("Dimension name not found" + dimName)
        }
        inc.next().write(coordinate(dimName))
      }
      metricNames.foreach { metricName =>
        val metrics = line.metrics
        inc.next().write(metrics.getOrElse(metricName, "N/A"))
      }
      inc.next().write(line.rowCount.getOrElse("N/A"))
      inc.next().write(line.planningTimeMillis.getOrElse("N/A"))
      inc.next().write(line.executionTimeMillis.getOrElse("N/A"))
    }
  }
}

case class TestResultLines(
    dimNames: Seq[String],
    metricNames: Seq[String],
    lines: Iterable[TestResultLine]) {
  def print(): Unit = {
    val fields = ListBuffer[String]("Query ID", "Succeeded")
    dimNames.foreach(dimName => fields.append(dimName))
    metricNames.foreach(metricName => fields.append(metricName))
    fields.append("Row Count")
    fields.append("Planning Time (Millis)")
    fields.append("Query Time (Millis)")
    val render = TableRender.plain[TestResultLine](fields: _*)(
      new TestResultLine.Parser(dimNames, metricNames))

    lines.foreach { line =>
      render.appendRow(line)
    }

    render.print(System.out)
  }
}

object Parameterized {
  private def runQuery(
      runner: QueryRunner,
      creator: TableCreator,
      sessionSwitcher: SparkSessionSwitcher,
      id: String,
      coordinate: Coordinate,
      desc: String,
      explain: Boolean,
      metrics: Array[String]) = {
    println(s"Running query: $id...")
    try {
      val testDesc = "Gluten Spark %s %s %s".format(desc, id, coordinate)
      sessionSwitcher.useSession(coordinate.toString, testDesc)
      runner.createTables(creator, sessionSwitcher.spark())
      val result =
        runner.runQuery(sessionSwitcher.spark(), testDesc, id, explain, metrics)
      val resultRows = result.rows
      println(
        s"Successfully ran query $id. " +
          s"Returned row count: ${resultRows.length}")
      TestResultLine(
        id,
        succeed = true,
        coordinate,
        Some(resultRows.length),
        Some(result.planningTimeMillis),
        Some(result.executionTimeMillis),
        result.metrics,
        None)
    } catch {
      case e: Exception =>
        val error = Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}")
        println(
          s"Error running query $id. " +
            s" Error: ${error.get}")
        TestResultLine(id, succeed = false, coordinate, None, None, None, Map.empty, error)
    }
  }

  private[integration] def warmUp(
      creator: TableCreator,
      id: String,
      desc: String,
      sessionSwitcher: SparkSessionSwitcher,
      runner: QueryRunner): Unit = {
    println(s"Warming up: Running query: $id...")
    try {
      val testDesc = "Gluten Spark %s %s warm up".format(desc, id)
      sessionSwitcher.useSession("test", testDesc)
      runner.createTables(creator, sessionSwitcher.spark())
      val result = runner.runQuery(sessionSwitcher.spark(), testDesc, id, explain = false)
      val resultRows = result.rows
      println(
        s"Warming up: Successfully ran query $id. " +
          s"Returned row count: ${resultRows.length}")
    } catch {
      case e: Exception =>
        val error = Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}")
        println(
          s"Warming up: Error running query $id. " +
            s" Error: ${error.get}")
    }
  }
}
