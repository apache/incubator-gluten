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

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.utils.{BackendTestSettings, BackendTestUtils}

import org.apache.spark.{SparkConf, SparkException, SparkThrowable, TestUtils}
import org.apache.spark.ErrorMessageFormat.MINIMAL
import org.apache.spark.SparkThrowableHelper.getMessage
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.{fileToString, stringToFile}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.TimestampTypes
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.Utils

import java.io.File
import java.net.URI
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * Originated from org.apache.spark.sql.SQLQueryTestSuite, with the following modifications:
 *   - Overwrite the generated golden files to remove failed queries.
 *   - Overwrite the generated golden files to update expected results for exception message
 *     mismatches, result order mismatches in non-order-sensitive queries, and minor precision scale
 *     mismatches.
 *   - Remove the AnalyzerTest as it's not within the scope of the Gluten project.
 *
 * NOTE: DO NOT simply copy-paste this file for supporting new Spark versions. SQLQueryTestSuite is
 * actively modified in Spark, so compare the difference and apply the necessary changes.
 */
@ExtendedSQLTest
class GlutenSQLQueryTestSuite
  extends QueryTest
  with SharedSparkSession
  with SQLHelper
  with SQLQueryTestHelper {

  import IntegratedUDFTestUtils._

  // ==== Start of modifications for Gluten. ====
  protected val resourcesPath = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toFile
  }
  protected val testDataPath = new File(resourcesPath, "test-data").getAbsolutePath

  protected val overwriteInputFilePath = new File(
    BackendTestSettings.instance.getSQLQueryTestSettings.getResourceFilePath,
    "inputs").getAbsolutePath

  protected val overwriteGoldenFilePath = new File(
    BackendTestSettings.instance.getSQLQueryTestSettings.getResourceFilePath,
    "results").getAbsolutePath

  private val isCHBackend = BackendTestUtils.isCHBackendLoaded()

  // List of supported cases to run with a certain backend, in lower case.
  private val supportedList: Set[String] =
    BackendTestSettings.instance.getSQLQueryTestSettings.getSupportedSQLQueryTests ++
      BackendTestSettings.instance.getSQLQueryTestSettings.getOverwriteSQLQueryTests

  private val normalizeRegex = "#\\d+L?".r
  private val nodeNumberRegex = "[\\^*]\\(\\d+\\)".r
  private def normalizeIds(plan: String): String = {
    val normalizedPlan = nodeNumberRegex.replaceAllIn(plan, "")
    val map = new mutable.HashMap[String, String]()
    normalizeRegex
      .findAllMatchIn(normalizedPlan)
      .map(_.toString)
      .foreach(map.getOrElseUpdate(_, (map.size + 1).toString))
    normalizeRegex.replaceAllIn(normalizedPlan, regexMatch => s"#${map(regexMatch.toString)}")
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      // Fewer shuffle partitions to speed up testing.
      .set(SQLConf.SHUFFLE_PARTITIONS, 4)
      // use Java 8 time API to handle negative years properly
      .set(SQLConf.DATETIME_JAVA8API_ENABLED, true)
      // SPARK-39564: don't print out serde to avoid introducing complicated and error-prone
      // regex magic.
      .set("spark.test.noSerdeInExplain", "true")
      .setAppName("Gluten-UT")
      .set("spark.driver.memory", "1G")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "134217728")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")

    if (isCHBackend) {
      conf
        .set("spark.io.compression.codec", "LZ4")
        .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
        .set("spark.gluten.sql.enable.native.validation", "false")
        .set("spark.sql.files.openCostInBytes", "134217728")
        .set("spark.unsafe.exceptionOnMemoryLeak", "true")
    } else {
      conf
        .set("spark.unsafe.exceptionOnMemoryLeak", "true")
        // Avoid static evaluation for literal input by spark catalyst.
        .set(
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
          ConvertToLocalRelation.ruleName +
            "," + ConstantFolding.ruleName + "," + NullPropagation.ruleName)
    }
    conf
  }

  /** List of test cases to ignore, in lower cases. */
  protected def ignoreList: Set[String] = Set(
    "ignored.sql", // Do NOT remove this one. It is here to test the ignore functionality.
    "explain-aqe.sql", // Explain is different in Gluten.
    "explain-cbo.sql", // Explain is different in Gluten.
    "explain.sql" // Explain is different in Gluten.
  ) ++ otherIgnoreList ++
    BackendTestSettings.instance.getSQLQueryTestSettings.getIgnoredSQLQueryTests

  /**
   * This method handles exceptions occurred during query execution as they may need special care to
   * become comparable to the expected output.
   *
   * Modified for Gluten by truncating exception output to only include the exception class and
   * message.
   *
   * @param result
   *   a function that returns a pair of schema and output
   */
  override protected def handleExceptions(
      result: => (String, Seq[String])): (String, Seq[String]) = {
    val format = MINIMAL
    try {
      result
    } catch {
      case e: SparkThrowable with Throwable if e.getErrorClass != null =>
        (emptySchema, Seq(e.getClass.getName, getMessage(e, format)))
      case a: AnalysisException =>
        // Do not output the logical plan tree which contains expression IDs.
        // Also implement a crude way of masking expression IDs in the error message
        // with a generic pattern "###".
        (emptySchema, Seq(a.getClass.getName, a.getSimpleMessage.replaceAll("#\\d+", "#x")))
      case s: SparkException if s.getCause != null =>
        // For a runtime exception, it is hard to match because its message contains
        // information of stage, task ID, etc.
        // To make result matching simpler, here we match the cause of the exception if it exists.
        s.getCause match {
          case e: SparkThrowable with Throwable if e.getErrorClass != null =>
            (emptySchema, Seq(e.getClass.getName, getMessage(e, format)))
          case e: GlutenException =>
            val reasonPattern = "Reason: (.*)".r
            val reason = reasonPattern.findFirstMatchIn(e.getMessage).map(_.group(1))

            reason match {
              case Some(r) =>
                (emptySchema, Seq(e.getClass.getName, r))
              case None => (emptySchema, Seq())
            }

          case cause =>
            (emptySchema, Seq(cause.getClass.getName, cause.getMessage))
        }
      case NonFatal(e) =>
        // If there is an exception, put the exception class followed by the message.
        (emptySchema, Seq(e.getClass.getName, e.getMessage))
    }
  }

  protected lazy val listTestCases: Seq[TestCase] = {
    val createTestCase = (file: File, parentDir: String, resultPath: String) => {
      val resultFile = file.getAbsolutePath.replace(parentDir, resultPath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(parentDir).stripPrefix(File.separator)

      if (
        file.getAbsolutePath.startsWith(
          s"$parentDir${File.separator}udf${File.separator}postgreSQL")
      ) {
        Seq(TestScalaUDF("udf"), TestPythonUDF("udf"), TestScalarPandasUDF("udf")).map {
          udf => UDFPgSQLTestCase(s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
        }
      } else if (file.getAbsolutePath.startsWith(s"$parentDir${File.separator}udf")) {
        Seq(TestScalaUDF("udf"), TestPythonUDF("udf"), TestScalarPandasUDF("udf")).map {
          udf => UDFTestCase(s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
        }
      } else if (file.getAbsolutePath.startsWith(s"$parentDir${File.separator}udaf")) {
        Seq(TestGroupedAggPandasUDF("udaf")).map {
          udf => UDAFTestCase(s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
        }
      } else if (file.getAbsolutePath.startsWith(s"$parentDir${File.separator}udtf")) {
        Seq(TestPythonUDTF("udtf")).map {
          udtf =>
            UDTFTestCase(
              s"$testCaseName - ${udtf.prettyName}",
              absPath,
              resultFile,
              udtf
            )
        }
      } else if (file.getAbsolutePath.startsWith(s"$parentDir${File.separator}postgreSQL")) {
        PgSQLTestCase(testCaseName, absPath, resultFile) :: Nil
      } else if (file.getAbsolutePath.startsWith(s"$parentDir${File.separator}ansi")) {
        AnsiTestCase(testCaseName, absPath, resultFile) :: Nil
      } else if (file.getAbsolutePath.startsWith(s"$parentDir${File.separator}timestampNTZ")) {
        TimestampNTZTestCase(testCaseName, absPath, resultFile) :: Nil
      } else if (file.getAbsolutePath.startsWith(s"$parentDir${File.separator}cte.sql")) {
        CTETestCase(testCaseName, absPath, resultFile) :: Nil
      } else {
        RegularTestCase(testCaseName, absPath, resultFile) :: Nil
      }
    }
    val overwriteTestCases = listFilesRecursively(new File(overwriteInputFilePath))
      .flatMap(createTestCase(_, overwriteInputFilePath, overwriteGoldenFilePath))
    val overwriteTestCaseNames = overwriteTestCases.map(_.name)
    (listFilesRecursively(new File(inputFilePath))
      .flatMap(createTestCase(_, inputFilePath, goldenFilePath))
      .filterNot(testCase => overwriteTestCaseNames.contains(testCase.name)) ++ overwriteTestCases)
      .sortBy(_.name)
  }
  // ==== End of modifications for Gluten. ====

  protected val baseResourcePath = {
    // We use a path based on Spark home for 2 reasons:
    //   1. Maven can't get correct resource directory when resources in other jars.
    //   2. We test subclasses in the hive-thriftserver module.
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "sql-tests").toFile
  }

  protected val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  protected val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  // SPARK-32106 Since we add SQL test 'transform.sql' will use `cat` command,
  // here we need to ignore it.
  private val otherIgnoreList =
    if (TestUtils.testCommandAvailable("/bin/bash")) Nil else Set("transform.sql")

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
    def asAnalyzerTest(newName: String, newResultFile: String): TestCase
  }

  /**
   * traits that indicate UDF or PgSQL to trigger the code path specific to each. For instance,
   * PgSQL tests require to register some UDF functions.
   */
  protected trait PgSQLTest

  /** Trait that indicates ANSI-related tests with the ANSI mode enabled. */
  protected trait AnsiTest

  /** Trait that indicates an analyzer test that shows the analyzed plan string as output. */
  protected trait AnalyzerTest extends TestCase {
    override def asAnalyzerTest(newName: String, newResultFile: String): AnalyzerTest = this
  }

  /** Trait that indicates the default timestamp type is TimestampNTZType. */
  protected trait TimestampNTZTest

  /** Trait that indicates CTE test cases need their create view versions */
  protected trait CTETest

  protected trait UDFTest {
    val udf: TestUDF
  }

  protected trait UDTFTest {
    val udtf: TestUDTF
  }

  /** A regular test case. */
  protected case class RegularTestCase(name: String, inputFile: String, resultFile: String)
    extends TestCase {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      RegularAnalyzerTestCase(newName, inputFile, newResultFile)
  }

  /** An ANSI-related test case. */
  protected case class AnsiTestCase(name: String, inputFile: String, resultFile: String)
    extends TestCase
    with AnsiTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      AnsiAnalyzerTestCase(newName, inputFile, newResultFile)
  }

  /** An analyzer test that shows the analyzed plan string as output. */
  protected case class AnalyzerTestCase(name: String, inputFile: String, resultFile: String)
    extends TestCase
    with AnalyzerTest

  /** A PostgreSQL test case. */
  protected case class PgSQLTestCase(name: String, inputFile: String, resultFile: String)
    extends TestCase
    with PgSQLTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      PgSQLAnalyzerTestCase(newName, inputFile, newResultFile)
  }

  /** A UDF test case. */
  protected case class UDFTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF)
    extends TestCase
    with UDFTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      UDFAnalyzerTestCase(newName, inputFile, newResultFile, udf)
  }

  protected case class UDTFTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udtf: TestUDTF)
    extends TestCase
    with UDTFTest {

    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      UDTFAnalyzerTestCase(newName, inputFile, newResultFile, udtf)
  }

  /** A UDAF test case. */
  protected case class UDAFTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF)
    extends TestCase
    with UDFTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      UDAFAnalyzerTestCase(newName, inputFile, newResultFile, udf)
  }

  /** A UDF PostgreSQL test case. */
  protected case class UDFPgSQLTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF)
    extends TestCase
    with UDFTest
    with PgSQLTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      UDFPgSQLAnalyzerTestCase(newName, inputFile, newResultFile, udf)
  }

  /** An date time test case with default timestamp as TimestampNTZType */
  protected case class TimestampNTZTestCase(name: String, inputFile: String, resultFile: String)
    extends TestCase
    with TimestampNTZTest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      TimestampNTZAnalyzerTestCase(newName, inputFile, newResultFile)
  }

  /** A CTE test case with special handling */
  protected case class CTETestCase(name: String, inputFile: String, resultFile: String)
    extends TestCase
    with CTETest {
    override def asAnalyzerTest(newName: String, newResultFile: String): TestCase =
      CTEAnalyzerTestCase(newName, inputFile, newResultFile)
  }

  /** These are versions of the above test cases, but only exercising analysis. */
  protected case class RegularAnalyzerTestCase(name: String, inputFile: String, resultFile: String)
    extends AnalyzerTest
  protected case class AnsiAnalyzerTestCase(name: String, inputFile: String, resultFile: String)
    extends AnalyzerTest
    with AnsiTest
  protected case class PgSQLAnalyzerTestCase(name: String, inputFile: String, resultFile: String)
    extends AnalyzerTest
    with PgSQLTest
  protected case class UDFAnalyzerTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF)
    extends AnalyzerTest
    with UDFTest
  protected case class UDTFAnalyzerTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udtf: TestUDTF)
    extends AnalyzerTest
    with UDTFTest
  protected case class UDAFAnalyzerTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF)
    extends AnalyzerTest
    with UDFTest
  protected case class UDFPgSQLAnalyzerTestCase(
      name: String,
      inputFile: String,
      resultFile: String,
      udf: TestUDF)
    extends AnalyzerTest
    with UDFTest
    with PgSQLTest
  protected case class TimestampNTZAnalyzerTestCase(
      name: String,
      inputFile: String,
      resultFile: String)
    extends AnalyzerTest
    with TimestampNTZTest
  protected case class CTEAnalyzerTestCase(name: String, inputFile: String, resultFile: String)
    extends AnalyzerTest
    with CTETest

  protected def createScalaTestCase(testCase: TestCase): Unit = {
    if (
      // Modified for Gluten to use exact name matching.
      !supportedList.exists(
        t => testCase.name.toLowerCase(Locale.ROOT) == t.toLowerCase(Locale.ROOT)) ||
      ignoreList.exists(t => testCase.name.toLowerCase(Locale.ROOT) == t.toLowerCase(Locale.ROOT))
    ) {
      // Create a test case to ignore this case.
      ignore(testCase.name) { /* Do nothing */ }
    } else {
      testCase match {
        case udfTestCase: UDFTest
            if udfTestCase.udf.isInstanceOf[TestPythonUDF] && !shouldTestPythonUDFs =>
          ignore(
            s"${testCase.name} is skipped because " +
              s"[$pythonExec] and/or pyspark were not available.") {
            /* Do nothing */
          }
        case udfTestCase: UDFTest
            if udfTestCase.udf.isInstanceOf[TestScalarPandasUDF] && !shouldTestPandasUDFs =>
          ignore(
            s"${testCase.name} is skipped because pyspark," +
              s"pandas and/or pyarrow were not available in [$pythonExec].") {
            /* Do nothing */
          }
        case udfTestCase: UDFTest
            if udfTestCase.udf.isInstanceOf[TestGroupedAggPandasUDF] &&
              !shouldTestPandasUDFs =>
          ignore(
            s"${testCase.name} is skipped because pyspark," +
              s"pandas and/or pyarrow were not available in [$pythonExec].") {
            /* Do nothing */
          }
        case _ =>
          // Create a test case to run this case.
          test(testCase.name) {
            runSqlTestCase(testCase, listTestCases)
          }
      }
    }
  }

  /** Run a test case. */
  protected def runSqlTestCase(testCase: TestCase, listTestCases: Seq[TestCase]): Unit = {
    def splitWithSemicolon(seq: Seq[String]) = {
      seq.mkString("\n").split("(?<=[^\\\\]);")
    }

    def splitCommentsAndCodes(input: String) = input.split("\n").partition {
      line =>
        val newLine = line.trim
        newLine.startsWith("--") && !newLine.startsWith("--QUERY-DELIMITER")
    }

    val input = fileToString(new File(testCase.inputFile))

    val (comments, code) = splitCommentsAndCodes(input)

    // If `--IMPORT` found, load code from another test case file, then insert them
    // into the head in this test.
    val importedTestCaseName = comments.filter(_.startsWith("--IMPORT ")).map(_.substring(9))
    val importedCode = importedTestCaseName.flatMap {
      testCaseName =>
        listTestCases.find(_.name == testCaseName).map {
          testCase =>
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
    val queries = tempQueries
      .map(_.trim)
      .filter(_ != "")
      .toSeq
      // Fix misplacement when comment is at the end of the query.
      .map(_.split("\n").filterNot(_.startsWith("--")).mkString("\n"))
      .map(_.trim)
      .filter(_ != "")

    val settingLines = comments.filter(_.startsWith("--SET ")).map(_.substring(6))
    val settings = settingLines.flatMap(_.split(",").map {
      kv =>
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
      val configDims = configDimLines.groupBy(_.takeWhile(_ != ' ')).mapValues {
        lines =>
          lines
            .map(_.dropWhile(_ != ' ').substring(1))
            .map(_.split(",")
              .map {
                kv =>
                  val (conf, value) = kv.span(_ != '=')
                  conf.trim -> value.substring(1).trim
              }
              .toSeq)
            .toSeq
      }

      val configSets = configDims.values.foldLeft(Seq(Seq[(String, String)]())) {
        (res, dim) => dim.flatMap(configSet => res.map(_ ++ configSet))
      }

      configSets.foreach {
        configSet =>
          try {
            runQueries(queries, testCase, settings ++ configSet)
          } catch {
            case e: Throwable =>
              val configs = configSet.map { case (k, v) => s"$k=$v" }
              logError(s"Error using configs: ${configs.mkString(",")}")
              throw e
          }
      }
    }
  }

  def hasNoDuplicateColumns(schema: String): Boolean = {
    val columnAndTypes = schema.replaceFirst("^struct<", "").stripSuffix(">").split(",")
    columnAndTypes.size == columnAndTypes.distinct.size
  }

  def expandCTEQueryAndCompareResult(
      session: SparkSession,
      query: String,
      output: ExecutionOutput): Unit = {
    val triggerCreateViewTest =
      try {
        val logicalPlan: LogicalPlan = session.sessionState.sqlParser.parsePlan(query)
        !logicalPlan.isInstanceOf[Command] &&
        output.schema.get != emptySchema &&
        hasNoDuplicateColumns(output.schema.get)
      } catch {
        case _: ParseException => return
      }

    // For non-command query with CTE, compare the results of selecting from view created on the
    // original query.
    if (triggerCreateViewTest) {
      val createView = s"CREATE temporary VIEW cte_view AS $query"
      val selectFromView = "SELECT * FROM cte_view"
      val dropViewIfExists = "DROP VIEW IF EXISTS cte_view"
      session.sql(createView)
      val (selectViewSchema, selectViewOutput) =
        handleExceptions(getNormalizedQueryExecutionResult(session, selectFromView))
      // Compare results.
      assertResult(
        output.schema.get,
        s"Schema did not match for CTE query and select from its view: \n$output") {
        selectViewSchema
      }
      assertResult(
        output.output,
        s"Result did not match for CTE query and select from its view: \n${output.sql}") {
        selectViewOutput.mkString("\n").replaceAll("\\s+$", "")
      }
      // Drop view.
      session.sql(dropViewIfExists)
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
      case udfTestCase: UDFTest =>
        registerTestUDF(udfTestCase.udf, localSparkSession)
      case udtfTestCase: UDTFTest =>
        registerTestUDTF(udtfTestCase.udtf, localSparkSession)
      case _ =>
    }

    testCase match {
      case _: PgSQLTest =>
        // booleq/boolne used by boolean.sql
        localSparkSession.udf.register("booleq", (b1: Boolean, b2: Boolean) => b1 == b2)
        localSparkSession.udf.register("boolne", (b1: Boolean, b2: Boolean) => b1 != b2)
        // vol used by boolean.sql and case.sql.
        localSparkSession.udf.register("vol", (s: String) => s)
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, true)
        localSparkSession.conf.set(SQLConf.LEGACY_INTERVAL_ENABLED.key, true)
      case _: AnsiTest =>
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, true)
      case _: TimestampNTZTest =>
        localSparkSession.conf.set(
          SQLConf.TIMESTAMP_TYPE.key,
          TimestampTypes.TIMESTAMP_NTZ.toString)
      case _ =>
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, false)
    }

    if (configSet.nonEmpty) {
      // Execute the list of set operation in order to add the desired configs
      val setOperations = configSet.map { case (key, value) => s"set $key=$value" }
      logInfo(s"Setting configs: ${setOperations.mkString(", ")}")
      setOperations.foreach(localSparkSession.sql)
    }

    // Run the SQL queries preparing them for comparison.
    val outputs: Seq[QueryTestOutput] = queries.map {
      sql =>
        testCase match {
          case _: AnalyzerTest =>
            val (_, output) =
              handleExceptions(getNormalizedQueryAnalysisResult(localSparkSession, sql))
            // We might need to do some query canonicalization in the future.
            AnalyzerOutput(
              sql = sql,
              schema = None,
              output = output.mkString("\n").replaceAll("\\s+$", ""))
          case _ =>
            val (schema, output) =
              handleExceptions(getNormalizedQueryExecutionResult(localSparkSession, sql))
            // We might need to do some query canonicalization in the future.
            val executionOutput = ExecutionOutput(
              sql = sql,
              schema = Some(schema),
              // GLUTEN-3559: Overwrite scalar-subquery-select.sql test
              output = normalizeIds(output.mkString("\n").replaceAll("\\s+$", ""))
            )
            if (testCase.isInstanceOf[CTETest]) {
              expandCTEQueryAndCompareResult(localSparkSession, sql, executionOutput)
            }
            executionOutput
        }
    }

    if (regenerateGoldenFiles) {
      // Again, we are explicitly not using multi-line string due to stripMargin removing "|".
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n" +
          outputs.mkString("\n\n\n") + "\n"
      }
      val resultFile = new File(testCase.resultFile)
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(resultFile, goldenOutput)
    }

    // This is a temporary workaround for SPARK-28894. The test names are truncated after
    // the last dot due to a bug in SBT. This makes easier to debug via Jenkins test result
    // report. See SPARK-28894.
    // See also SPARK-29127. It is difficult to see the version information in the failed test
    // cases so the version information related to Python was also added.
    val clue = testCase match {
      case udfTestCase: UDFTest
          if udfTestCase.udf.isInstanceOf[TestPythonUDF] && shouldTestPythonUDFs =>
        s"${testCase.name}${System.lineSeparator()}Python: $pythonVer${System.lineSeparator()}"
      case udfTestCase: UDFTest
          if udfTestCase.udf.isInstanceOf[TestScalarPandasUDF] && shouldTestPandasUDFs =>
        s"${testCase.name}${System.lineSeparator()}" +
          s"Python: $pythonVer Pandas: $pandasVer PyArrow: $pyarrowVer${System.lineSeparator()}"
      case udfTestCase: UDFTest
          if udfTestCase.udf.isInstanceOf[TestGroupedAggPandasUDF] &&
            shouldTestPandasUDFs =>
        s"${testCase.name}${System.lineSeparator()}" +
          s"Python: $pythonVer Pandas: $pandasVer PyArrow: $pyarrowVer${System.lineSeparator()}"
      case udtfTestCase: UDTFTest
          if udtfTestCase.udtf.isInstanceOf[TestPythonUDTF] && shouldTestPythonUDFs =>
        s"${testCase.name}${System.lineSeparator()}Python: $pythonVer${System.lineSeparator()}"
      case _ =>
        s"${testCase.name}${System.lineSeparator()}"
    }

    withClue(clue) {
      testCase match {
        case _: AnalyzerTest =>
          readGoldenFileAndCompareResults(testCase.resultFile, outputs, AnalyzerOutput)
        case _ =>
          readGoldenFileAndCompareResults(testCase.resultFile, outputs, ExecutionOutput)
      }
    }
  }

  // ==== Start of modifications for Gluten. ====
  // ===- End of modifications for Gluten. ====

  /** Returns all the files (not directories) in a directory, recursively. */
  protected def listFilesRecursively(path: File): Seq[File] = {
    val (dirs, files) = path.listFiles().partition(_.isDirectory)
    // Filter out test files with invalid extensions such as temp files created
    // by vi (.swp), Mac (.DS_Store) etc.
    val filteredFiles = files.filter(_.getName.endsWith(validFileExtensions))
    filteredFiles ++ dirs.flatMap(listFilesRecursively)
  }

  /** Load built-in test tables into the SparkSession. */
  private def createTestTables(session: SparkSession): Unit = {
    import session.implicits._

    // Before creating test tables, deletes orphan directories in warehouse dir
    Seq("testdata", "arraydata", "mapdata", "aggtest", "onek", "tenk1").foreach {
      dirName =>
        val f = new File(new URI(s"${conf.warehousePath}/$dirName"))
        if (f.exists()) {
          Utils.deleteRecursively(f)
        }
    }

    (1 to 100)
      .map(i => (i, i.toString))
      .toDF("key", "value")
      .repartition(1)
      .write
      .format("parquet")
      .saveAsTable("testdata")

    ((Seq(1, 2, 3), Seq(Seq(1, 2, 3))) :: (Seq(2, 3, 4), Seq(Seq(2, 3, 4))) :: Nil)
      .toDF("arraycol", "nestedarraycol")
      .write
      .format("parquet")
      .saveAsTable("arraydata")

    (Tuple1(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
      Tuple1(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
      Tuple1(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
      Tuple1(Map(1 -> "a4", 2 -> "b4")) ::
      Tuple1(Map(1 -> "a5")) :: Nil)
      .toDF("mapcol")
      .write
      .format("parquet")
      .saveAsTable("mapdata")

    session.read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema("a int, b float")
      .load(testDataPath + "/postgresql/agg.data")
      .write
      .format("parquet")
      .saveAsTable("aggtest")

    session.read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema("""
                |unique1 int,
                |unique2 int,
                |two int,
                |four int,
                |ten int,
                |twenty int,
                |hundred int,
                |thousand int,
                |twothousand int,
                |fivethous int,
                |tenthous int,
                |odd int,
                |even int,
                |stringu1 string,
                |stringu2 string,
                |string4 string
        """.stripMargin)
      .load(testDataPath + "/postgresql/onek.data")
      .write
      .format("parquet")
      .saveAsTable("onek")

    session.read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema("""
                |unique1 int,
                |unique2 int,
                |two int,
                |four int,
                |ten int,
                |twenty int,
                |hundred int,
                |thousand int,
                |twothousand int,
                |fivethous int,
                |tenthous int,
                |odd int,
                |even int,
                |stringu1 string,
                |stringu2 string,
                |string4 string
        """.stripMargin)
      .load(testDataPath + "/postgresql/tenk.data")
      .write
      .format("parquet")
      .saveAsTable("tenk1")
  }

  protected def removeTestTables(session: SparkSession): Unit = {
    session.sql("DROP TABLE IF EXISTS testdata")
    session.sql("DROP TABLE IF EXISTS arraydata")
    session.sql("DROP TABLE IF EXISTS mapdata")
    session.sql("DROP TABLE IF EXISTS aggtest")
    session.sql("DROP TABLE IF EXISTS onek")
    session.sql("DROP TABLE IF EXISTS tenk1")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTestTables(spark)
    RuleExecutor.resetMetrics()
    CodeGenerator.resetCompileTime()
    WholeStageCodegenExec.resetCodeGenTime()
  }

  override def afterAll(): Unit = {
    try {
      removeTestTables(spark)

      // For debugging dump some statistics about how much time was spent in various optimizer rules
      logWarning(RuleExecutor.dumpTimeSpent())

      val codeGenTime = WholeStageCodegenExec.codeGenTime.toDouble / NANOS_PER_SECOND
      val compileTime = CodeGenerator.compileTime.toDouble / NANOS_PER_SECOND
      val codegenInfo =
        s"""
           |=== Metrics of Whole-stage Codegen ===
           |Total code generation time: $codeGenTime seconds
           |Total compile time: $compileTime seconds
         """.stripMargin
      logWarning(codegenInfo)
    } finally {
      super.afterAll()
    }
  }

  /**
   * Consumes contents from a single golden file and compares the expected results against the
   * output of running a query.
   */
  def readGoldenFileAndCompareResults(
      resultFile: String,
      outputs: Seq[QueryTestOutput],
      makeOutput: (String, Option[String], String) => QueryTestOutput): Unit = {
    // Read back the golden file.
    val expectedOutputs: Seq[QueryTestOutput] = {
      val goldenOutput = fileToString(new File(resultFile))
      val segments = goldenOutput.split("-- !query.*\n")

      val numSegments = outputs.map(_.numSegments).sum + 1
      assert(
        segments.size == numSegments,
        s"Expected $numSegments blocks in result file but got " +
          s"${segments.size}. Try regenerate the result files.")
      var curSegment = 0
      outputs.map {
        output =>
          val result = if (output.numSegments == 3) {
            makeOutput(
              segments(curSegment + 1).trim, // SQL
              Some(segments(curSegment + 2).trim), // Schema
              segments(curSegment + 3).replaceAll("\\s+$", "")
            ) // Output
          } else {
            makeOutput(
              segments(curSegment + 1).trim, // SQL
              None, // Schema
              segments(curSegment + 2).replaceAll("\\s+$", "")
            ) // Output
          }
          curSegment += output.numSegments
          result
      }
    }

    // Compare results.
    assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
      outputs.size
    }

    outputs.zip(expectedOutputs).zipWithIndex.foreach {
      case ((output, expected), i) =>
        assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
          output.sql
        }
        assertResult(
          expected.schema,
          s"Schema did not match for query #$i\n${expected.sql}: $output") {
          output.schema
        }
        assertResult(
          expected.output,
          s"Result did not match" +
            s" for query #$i\n${expected.sql}") {
          output.output
        }
    }
  }

  /** A single SQL query's output. */
  trait QueryTestOutput {
    def sql: String
    def schema: Option[String]
    def output: String
    def numSegments: Int
  }

  /** A single SQL query's execution output. */
  case class ExecutionOutput(sql: String, schema: Option[String], output: String)
    extends QueryTestOutput {
    override def toString: String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query\n" +
        sql + "\n" +
        s"-- !query schema\n" +
        schema.get + "\n" +
        s"-- !query output\n" +
        output
    }
    override def numSegments: Int = 3
  }

  /** A single SQL query's analysis results. */
  case class AnalyzerOutput(sql: String, schema: Option[String], output: String)
    extends QueryTestOutput {
    override def toString: String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query\n" +
        sql + "\n" +
        s"-- !query analysis\n" +
        output
    }
    override def numSegments: Int = 2
  }
}
