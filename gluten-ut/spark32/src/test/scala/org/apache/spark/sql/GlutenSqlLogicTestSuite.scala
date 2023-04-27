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

package org.apache.spark.sql

import java.io.File
import java.util.Locale

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class GlutenSqlLogicTestSuite extends QueryTest with SharedSparkSession with SQLHelper
  with SQLQueryTestHelper {

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"
  val baseResource = Thread.currentThread().getContextClassLoader
    .getResource("SqlLogicTest")
  val baseResourcePath = new File(baseResource.toURI).getAbsolutePath
  protected val inputFilePath = new File(baseResourcePath, "query").getAbsolutePath
  protected val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  protected val validFileExtensions = ".sql"

  protected override def sparkConf: SparkConf = {
    val conf = super.sparkConf
      // Fewer shuffle partitions to speed up testing.
      .set(SQLConf.SHUFFLE_PARTITIONS, 4)
      // use Java 8 time API to handle negative years properly
      .set(SQLConf.DATETIME_JAVA8API_ENABLED, true)
      .setAppName("Gluten-UT")
      .set("spark.driver.memory", "1G")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "134217728")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")

    conf.set("spark.unsafe.exceptionOnMemoryLeak", "true")
    conf
  }

  protected def ignoreList: Set[String] = Set(
    "ignored.sql" // Do NOT remove this one. It is here to test the ignore functionality.
  )
  // Create all the test cases.
  listTestCases.foreach(createScalaTestCase)

  /** A single SQL query's output. */
  protected case class QueryOutput(sql: String, schema: String, output: String) {
    override def toString: String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query\n" +
        sql + "\n" +
        s"-- !query schema\n" +
        schema + "\n" +
        s"-- !query output\n" +
        output
    }
  }

  /** A test case. */
  protected trait TestCase {
    val name: String
    val inputFile: String
    val resultFile: String
  }

  /** A regular test case. */
  protected case class RegularTestCase(
                                        name: String, inputFile: String, resultFile: String) extends TestCase

  /** Returns all the files (not directories) in a directory, recursively. */
  protected def listFilesRecursively(path: File): Seq[File] = {
    val (dirs, files) = path.listFiles().partition(_.isDirectory)
    // Filter out test files with invalid extensions such as temp files created
    // by vi (.swp), Mac (.DS_Store) etc.
    val filteredFiles = files.filter(_.getName.endsWith(validFileExtensions))
    filteredFiles ++ dirs.flatMap(listFilesRecursively)
  }

  protected lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)

      RegularTestCase(testCaseName, absPath, resultFile) :: Nil
    }
  }

  protected def createScalaTestCase(testCase: TestCase): Unit = {
    if (ignoreList.exists(t =>
      testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT)))) {
      // Create a test case to ignore this case.
      ignore(testCase.name) {
        /* Do nothing */
      }
    } else testCase match {
      case _ =>
        // Create a test case to run this case.
        test(testCase.name) {
          runTest(testCase)
        }
    }
  }

  protected def runTest(testCase: TestCase): Unit = {
    def splitWithSemicolon(seq: Seq[String]) = {
      seq.mkString("\n").split("(?<=[^\\\\]);")
    }

    def splitCommentsAndCodes(input: String) = input.split("\n").partition { line =>
      val newLine = line.trim
      newLine.startsWith("--") && !newLine.startsWith("--QUERY-DELIMITER")
    }

    val input = fileToString(new File(testCase.inputFile))

    val (comments, code) = splitCommentsAndCodes(input)

    // If `--IMPORT` found, load code from another test case file, then insert them
    // into the head in this test.
    val importedTestCaseName = comments.filter(_.startsWith("--IMPORT ")).map(_.substring(9))
    val importedCode = importedTestCaseName.flatMap { testCaseName =>
      listTestCases.find(_.name == testCaseName).map { testCase =>
        val input = fileToString(new File(testCase.inputFile))
        val (_, code) = splitCommentsAndCodes(input)
        code
      }
    }.flatten

    val allCode = importedCode ++ code
    val tempQueries = if (allCode.exists(_.trim.startsWith("--QUERY-DELIMITER"))) {
      // Although the loop is heavy, only used for bracketed comments test.
      val queries = new ArrayBuffer[String]
      val otherCodes = new ArrayBuffer[String]
      var tempStr = ""
      var start = false
      for (c <- allCode) {
        if (c.trim.startsWith("--QUERY-DELIMITER-START")) {
          start = true
          queries ++= splitWithSemicolon(otherCodes.toSeq)
          otherCodes.clear()
        } else if (c.trim.startsWith("--QUERY-DELIMITER-END")) {
          start = false
          queries += s"\n${tempStr.stripSuffix(";")}"
          tempStr = ""
        } else if (start) {
          tempStr += s"\n$c"
        } else {
          otherCodes += c
        }
      }
      if (otherCodes.nonEmpty) {
        queries ++= splitWithSemicolon(otherCodes.toSeq)
      }
      queries.toSeq
    } else {
      splitWithSemicolon(allCode).toSeq
    }

    // List of SQL queries to run
    val queries = tempQueries.map(_.trim).filter(_ != "").toSeq
      // Fix misplacement when comment is at the end of the query.
      .map(_.split("\n").filterNot(_.startsWith("--")).mkString("\n")).map(_.trim).filter(_ != "")

    val settingLines = comments.filter(_.startsWith("--SET ")).map(_.substring(6))
    val settings = settingLines.flatMap(_.split(",").map { kv =>
      val (conf, value) = kv.span(_ != '=')
      conf.trim -> value.substring(1).trim
    })

    if (regenerateGoldenFiles) {
      runQueries(queries, testCase, settings)
    } else {
      // A config dimension has multiple config sets, and a config set has multiple configs.
      // - config dim:     Seq[Seq[(String, String)]]
      //   - config set:   Seq[(String, String)]
      //     - config:     (String, String))
      // We need to do cartesian product for all the config dimensions, to get a list of
      // config sets, and run the query once for each config set.
      val configDimLines = comments.filter(_.startsWith("--CONFIG_DIM")).map(_.substring(12))
      val configDims = configDimLines.groupBy(_.takeWhile(_ != ' ')).mapValues { lines =>
        lines.map(_.dropWhile(_ != ' ').substring(1)).map(_.split(",").map { kv =>
          val (conf, value) = kv.span(_ != '=')
          conf.trim -> value.substring(1).trim
        }.toSeq).toSeq
      }

      val configSets = configDims.values.foldLeft(Seq(Seq[(String, String)]())) { (res, dim) =>
        dim.flatMap { configSet => res.map(_ ++ configSet) }
      }

      configSets.foreach { configSet =>
        try {
          runQueries(queries, testCase, settings ++ configSet)
        } catch {
          case e: Throwable =>
            val configs = configSet.map {
              case (k, v) => s"$k=$v"
            }
            logError(s"Error using configs: ${configs.mkString(",")}")
            throw e
        }
      }
    }
  }

  protected def runQueries(
                            queries: Seq[String],
                            testCase: TestCase,
                            configSet: Seq[(String, String)]): Unit = {
    // Create a local SparkSession to have stronger isolation between different test cases.
    // This does not isolate catalog changes.
    val localSparkSession = spark.newSession()

    testCase match {
      case _ =>
    }

    if (configSet.nonEmpty) {
      // Execute the list of set operation in order to add the desired configs
      val setOperations = configSet.map { case (key, value) => s"set $key=$value" }
      logInfo(s"Setting configs: ${setOperations.mkString(", ")}")
      setOperations.foreach(localSparkSession.sql)
    }

    // Run the SQL queries preparing them for comparison.
    val outputs: Seq[QueryOutput] = queries.map { sql =>
      val (schema, output) = handleExceptions(getNormalizedResult(localSparkSession, sql))
      // We might need to do some query canonicalization in the future.
      QueryOutput(
        sql = sql,
        schema = schema,
        output = output.mkString("\n").replaceAll("\\s+$", ""))
    }

    if (regenerateGoldenFiles) {
      // Again, we are explicitly not using multi-line string due to stripMargin removing "|".
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n" +
          s"-- Number of queries: ${outputs.size}\n\n\n" +
          outputs.mkString("\n\n\n") + "\n"
      }
      val resultFile = new File(testCase.resultFile)
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(resultFile, goldenOutput)
    }

    val clue = s"${testCase.name}${System.lineSeparator()}"

    withClue(clue) {
      // Read back the golden file.
      val expectedOutputs: Seq[QueryOutput] = {
        val goldenOutput = fileToString(new File(testCase.resultFile))
        val segments = goldenOutput.split("-- !query.*\n")

        // each query has 3 segments, plus the header
        assert(segments.size == outputs.size * 3 + 1,
          s"Expected ${outputs.size * 3 + 1} blocks in result file but got ${segments.size}. " +
            s"Try regenerate the result files.")
        Seq.tabulate(outputs.size) { i =>
          QueryOutput(
            sql = segments(i * 3 + 1).trim,
            schema = segments(i * 3 + 2).trim,
            output = segments(i * 3 + 3).replaceAll("\\s+$", "")
          )
        }
      }

      // Compare results.
      assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
        outputs.size
      }

      outputs.zip(expectedOutputs).zipWithIndex.foreach { case ((output, expected), i) =>
        assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
          output.sql
        }
        assertResult(expected.schema,
          s"Schema did not match for query #$i\n${expected.sql}: $output") {
          output.schema
        }
        assertResult(expected.output, s"Result did not match" +
          s" for query #$i\n${expected.sql}") {
          output.output
        }
      }
    }
  }
}