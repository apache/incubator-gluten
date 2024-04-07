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
package org.apache.gluten.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, TestUtils}
import org.apache.spark.sql.execution.FormattedMode

import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.charset.StandardCharsets

abstract class VeloxTPCHTableSupport extends VeloxWholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  // TODO: the tpch query was changed a bit. Because date was converted into string in the test
  //  dataset, the queries were changed accordingly.
  protected val veloxTPCHQueries: String = rootPath + "/tpch-queries-velox"

  // TODO: result comparison is not supported currently.
  protected val queriesResults: String = rootPath + "queries-output"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    // TODO Should enable this after fix the issue of native plan detail occasional disappearance
    // .set("spark.gluten.sql.injectNativePlanStringToExplain", "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }
}

abstract class VeloxTPCHSuite extends VeloxTPCHTableSupport {
  // here we only get spark major version
  private lazy val formatSparkVersion: String = spark.version.replace(".", "").substring(0, 2)

  private def formatMaterializedPlan(plan: String): String = {
    plan
      .replaceAll("#[0-9]*L*", "#X")
      .replaceAll("plan_id=[0-9]*", "plan_id=X")
      .replaceAll("Statistics[(A-Za-z0-9=. ,+)]*", "Statistics(X)")
      .replaceAll("WholeStageCodegenTransformer[0-9 ()]*", "WholeStageCodegenTransformer (X)")
      .replaceAll("\\[file:[.\\-/a-zA-z0-9= ,_%]*]", "[*]")
      // for unexpected blank
      .replaceAll("Scan parquet ", "Scan parquet")
      // Spark QueryStageExec will take it's id as argument, replace it with X
      .replaceAll("Arguments: [0-9]+", "Arguments: X")
      // mask PullOutPostProject and PullOutPreProject id
      .replaceAll("_pre_[0-9]*", "_pre_X")
      .replaceAll("_post_[0-9]*", "_post_X")
      .trim
  }

  private def getGoldenFile(path: String): String = {
    FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8).trim
  }

  def subType(): String = ""
  def shouldCheckGoldenFiles(): Boolean = {
    Seq("v1", "v1-bhj").contains(subType()) && (
      formatSparkVersion match {
        case "32" => true
        case "33" => true
        case "34" => true
        case _ => false
      }
    )
  }

  private def checkGoldenFile(df: DataFrame, id: Int): Unit = {
    // skip checking golden file for non-ready subtype and spark version
    if (!shouldCheckGoldenFiles) {
      return
    }
    val file = s"tpch-approved-plan/${subType()}/spark$formatSparkVersion/$id.txt"
    val actual = formatMaterializedPlan(df.queryExecution.explainString(FormattedMode))
    val path = s"$rootPath$file"
    // due to assert throw too much info
    // let's check and print diff manually
    if (!getGoldenFile(path).equals(actual)) {
      val actualFile = new File(FileUtils.getTempDirectory, file)
      new File(actualFile.getParent).mkdirs()
      FileUtils.writeStringToFile(actualFile, actual, StandardCharsets.UTF_8)
      org.scalatest.Assertions.fail(
        s"Mismatch for query $id\n" +
          s"Actual Plan path: ${actualFile.getAbsolutePath}\n" +
          s"Golden Plan path: $path")
    }
  }

  test("TPC-H q1") {
    runTPCHQuery(1, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 1)
    }
  }

  test("TPC-H q2") {
    runTPCHQuery(2, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ => // due to tpc-h q2 will generate multiple plans, skip checking golden file for now
    }
  }

  test("TPC-H q3") {
    runTPCHQuery(3, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 3)
    }
  }

  test("TPC-H q4") {
    runTPCHQuery(4, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 4)
    }
  }

  test("TPC-H q5") {
    runTPCHQuery(5, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 5)
    }
  }

  test("TPC-H q6") {
    runTPCHQuery(6, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 6)
    }
  }

  test("TPC-H q7") {
    runTPCHQuery(7, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 7)
    }
  }

  test("TPC-H q8") {
    runTPCHQuery(8, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 8)
    }
  }

  test("TPC-H q9") {
    runTPCHQuery(9, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 9)
    }
  }

  test("TPC-H q10") {
    runTPCHQuery(10, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 10)
    }
  }

  test("TPC-H q11") {
    runTPCHQuery(11, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 11)
    }
  }

  test("TPC-H q12") {
    runTPCHQuery(12, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 12)
    }
  }

  test("TPC-H q13") {
    runTPCHQuery(13, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 13)
    }
  }

  test("TPC-H q14") {
    runTPCHQuery(14, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 14)
    }
  }

  test("TPC-H q15") {
    runTPCHQuery(15, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 15)
    }
  }

  test("TPC-H q16") {
    runTPCHQuery(16, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 16)
    }
  }

  test("TPC-H q17") {
    runTPCHQuery(17, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 17)
    }
  }

  test("TPC-H q18") {
    runTPCHQuery(18, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 18)
    }
  }

  test("TPC-H q19") {
    runTPCHQuery(19, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 19)
    }
  }

  test("TPC-H q20") {
    runTPCHQuery(20, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 20)
    }
  }

  test("TPC-H q21") {
    runTPCHQuery(21, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 21)
    }
  }

  test("TPC-H q22") {
    runTPCHQuery(22, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      checkGoldenFile(_, 22)
    }
  }

  test("test 'order by limit'") {
    val df = spark.sql(
      """
        |select n_nationkey from nation order by n_nationkey limit 5
        |""".stripMargin
    )
    val sortExec = df.queryExecution.executedPlan.collect {
      case sortExec: TakeOrderedAndProjectExecTransformer => sortExec
    }
    assert(sortExec.size == 1)
    val result = df.collect()
    val expectedResult = Seq(Row(0), Row(1), Row(2), Row(3), Row(4))
    TestUtils.compareAnswers(result, expectedResult)
  }
}

class VeloxTPCHDistinctSpill extends VeloxTPCHTableSupport {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "50m")
      .set("spark.gluten.memory.overAcquiredMemoryRatio", "0.9") // to trigger distinct spill early
  }

  test("distinct spill") {
    val df = spark.sql("select count(distinct *) from lineitem limit 1")
    TestUtils.compareAnswers(df.collect(), Seq(Row(60175)))
  }
}

class VeloxTPCHV1Suite extends VeloxTPCHSuite {
  override def subType(): String = "v1"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
}

class VeloxTPCHV1BhjSuite extends VeloxTPCHSuite {
  override def subType(): String = "v1-bhj"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
  }
}

class VeloxTPCHV2Suite extends VeloxTPCHSuite {
  override def subType(): String = "v2"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
}

class VeloxTPCHV2BhjSuite extends VeloxTPCHSuite {
  override def subType(): String = "v2-bhj"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
  }
}

class VeloxPartitionedTableTPCHSuite extends VeloxTPCHSuite {
  override def subType(): String = "partitioned"

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.memory.offHeap.size", "4g")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    TPCHTableDataFrames = TPCHTables.map {
      table =>
        val tableDir = getClass.getResource(resourcePath).getFile
        val tablePath = new File(tableDir, table.name).getAbsolutePath
        val tableDF = spark.read.format(fileFormat).load(tablePath)

        tableDF.write
          .format(fileFormat)
          .partitionBy(table.partitionColumns: _*)
          .mode("append")
          .saveAsTable(table.name)
        (table.name, tableDF)
    }.toMap
  }

  override protected def afterAll(): Unit = {
    if (TPCHTableDataFrames != null) {
      TPCHTableDataFrames.keys.foreach(table => spark.sql(s"DROP TABLE IF EXISTS $table"))
    }
    super.afterAll()
  }
}

class VeloxTPCHV1GlutenBhjVanillaBeSuite extends VeloxTPCHSuite {
  override def subType(): String = "gluten-bhj-vanilla-be"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
      .set("spark.gluten.sql.columnar.broadcastJoin", "true")
      .set("spark.gluten.sql.columnar.broadcastExchange", "false")
  }
}

class VeloxTPCHV1VanillaBhjGlutenBeSuite extends VeloxTPCHSuite {
  override def subType(): String = "vanilla-bhj-gluten-be"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
      .set("spark.gluten.sql.columnar.broadcastJoin", "false")
      .set("spark.gluten.sql.columnar.broadcastExchange", "true")
  }
}
