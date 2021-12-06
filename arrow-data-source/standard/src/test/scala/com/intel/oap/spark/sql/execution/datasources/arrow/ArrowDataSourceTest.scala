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

package com.intel.oap.spark.sql.execution.datasources.arrow

import java.io.File
import java.lang.management.ManagementFactory

import com.intel.oap.spark.sql.ArrowWriteExtension
import com.intel.oap.spark.sql.DataFrameReaderImplicits._
import com.intel.oap.spark.sql.DataFrameWriterImplicits._
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.ArrowOptions
import com.sun.management.UnixOperatingSystemMXBean
import org.apache.commons.io.FileUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class ArrowDataSourceTest extends QueryTest with SharedSparkSession {
  private val parquetFile1 = "parquet-1.parquet"
  private val parquetFile2 = "parquet-2.parquet"
  private val parquetFile3 = "parquet-3.parquet"
  private val parquetFile4 = "parquet-4.parquet"
  private val parquetFile5 = "parquet-5.parquet"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(10 * 1024 * 1024))
    conf.set("spark.unsafe.exceptionOnMemoryLeak", "false")
    conf.set(SPARK_SESSION_EXTENSIONS.key, classOf[ArrowWriteExtension].getCanonicalName)
    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    import testImplicits._
    spark.read
      .json(Seq("{\"col\": -1}", "{\"col\": 0}", "{\"col\": 1}", "{\"col\": 2}", "{\"col\": null}")
        .toDS())
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile1))

    spark.read
      .json(Seq("{\"col\": \"a\"}", "{\"col\": \"b\"}")
        .toDS())
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile2))

    spark.read
      .json(Seq("{\"col1\": \"apple\", \"col2\": 100}", "{\"col1\": \"pear\", \"col2\": 200}",
        "{\"col1\": \"apple\", \"col2\": 300}")
        .toDS())
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile3))

    spark.range(1000)
      .select(col("id"), col("id").as("k"))
      .write
      .partitionBy("k")
      .format("parquet")
      .mode("overwrite")
      .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile4))

    spark.range(100)
      .select(col("id"), col("id").as("k"))
      .write
      .partitionBy("k")
      .format("parquet")
      .mode("overwrite")
      .parquet(ArrowDataSourceTest.locateResourcePath(parquetFile5))

  }

  override def afterAll(): Unit = {
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile1))
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile2))
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile3))
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile4))
    delete(ArrowDataSourceTest.locateResourcePath(parquetFile5))
    super.afterAll()
  }

  test("read parquet file") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    verifyFrame(
      spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(path), 5, 1)
  }

  ignore("simple sql query on s3") {
    val path = "s3a://mlp-spark-dataset-bucket/test_arrowds_s3_small"
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("stab")
    assert(spark.sql("select id from stab").count() === 1000)
  }

  test("create catalog table") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    spark.catalog.createTable("ptab", path, "arrow")
    val sql = "select * from ptab"
    spark.sql(sql).explain()
    verifyFrame(spark.sql(sql), 5, 1)
  }

  test("create table statement") {
    spark.sql("drop table if exists ptab")
    spark.sql("create table ptab (col1 varchar(14), col2 bigint, col3 bigint) " +
      "using arrow " +
      "partitioned by (col1)")
    spark.sql("select * from ptab")
  }

  test("simple SQL query on parquet file - 1") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    verifyFrame(spark.sql("select * from ptab"), 5, 1)
    verifyFrame(spark.sql("select col from ptab"), 5, 1)
    verifyFrame(spark.sql("select col from ptab where col is not null or col is null"),
      5, 1)
  }

  test("simple SQL query on parquet file - 2") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile3)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    val sqlFrame = spark.sql("select * from ptab")
    assert(
      sqlFrame.schema ===
        StructType(Seq(StructField("col1", StringType), StructField("col2", LongType))))
    val rows = sqlFrame.collect()
    assert(rows(0).get(0) == "apple")
    assert(rows(0).get(1) == 100)
    assert(rows(1).get(0) == "pear")
    assert(rows(1).get(1) == 200)
    assert(rows(2).get(0) == "apple")
    assert(rows(2).get(1) == 300)
    assert(rows.length === 3)
  }

  test("simple parquet write") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile3)
    val frame = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(path)
    frame.createOrReplaceTempView("ptab")
    val sqlFrame = spark.sql("select * from ptab")

    val writtenPath = FileUtils.getTempDirectory + File.separator + "written.parquet"
    sqlFrame.write.mode(SaveMode.Overwrite)
        .option(ArrowOptions.KEY_TARGET_FORMAT, "parquet")
        .arrow(writtenPath)

    val frame2 = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(writtenPath)
    frame2.createOrReplaceTempView("ptab2")
    val sqlFrame2 = spark.sql("select * from ptab2")

    verifyFrame(sqlFrame2, 3, 2)
  }

  test("simple SQL query on parquet file with pushed filters") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    spark.sql("select col from ptab where col = 1").explain(true)
    val result = spark.sql("select col from ptab where col = 1") // fixme rowcount == 2?
    assert(
      result.schema ===
        StructType(Seq(StructField("col", LongType))))
    assert(result.collect().length === 1)
  }

  test("ignore unrecognizable types when pushing down filters") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile2)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")
    val rows = spark.sql("select * from ptab where col = 'b'").collect()
    assert(rows.length === 1)
  }

  ignore("dynamic partition pruning") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
      SQLConf.USE_V1_SOURCE_LIST.key -> "arrow",
      SQLConf.CBO_ENABLED.key -> "true") {

      var path: String = null
      path = ArrowDataSourceTest.locateResourcePath(parquetFile4)
      spark.catalog.createTable("df1", path, "arrow")
      path = ArrowDataSourceTest.locateResourcePath(parquetFile5)
      spark.catalog.createTable("df2", path, "arrow")

      sql("ALTER TABLE df1 RECOVER PARTITIONS")
      sql("ALTER TABLE df2 RECOVER PARTITIONS")

      sql("ANALYZE TABLE df1 COMPUTE STATISTICS FOR COLUMNS id")
      sql("ANALYZE TABLE df2 COMPUTE STATISTICS FOR COLUMNS id")

      val df = sql("SELECT df1.id, df2.k FROM df1 " +
        "JOIN df2 ON df1.k = df2.k AND df2.id < 2")
      assert(df.queryExecution.executedPlan.toString().contains("dynamicpruningexpression"))
      checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
    }
  }

  test("count(*) without group by v2") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(path)
    frame.createOrReplaceTempView("ptab")
    val df = sql("SELECT COUNT(*) FROM ptab")
    checkAnswer(df, Row(5) :: Nil)

  }

  test("file descriptor leak") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .arrow(path)
    frame.createOrReplaceTempView("ptab")

    def getFdCount: Long = {
      ManagementFactory.getOperatingSystemMXBean
        .asInstanceOf[UnixOperatingSystemMXBean]
        .getOpenFileDescriptorCount
    }

    val initialFdCount = getFdCount
    for (_ <- 0 until 100) {
      verifyFrame(spark.sql("select * from ptab"), 5, 1)
    }
    val fdGrowth = getFdCount - initialFdCount
    assert(fdGrowth < 100)
  }

  test("file descriptor leak - v1") {
    val path = ArrowDataSourceTest.locateResourcePath(parquetFile1)
    val frame = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .arrow(path)
    frame.createOrReplaceTempView("ptab2")

    def getFdCount: Long = {
      ManagementFactory.getOperatingSystemMXBean
          .asInstanceOf[UnixOperatingSystemMXBean]
          .getOpenFileDescriptorCount
    }

    val initialFdCount = getFdCount
    for (_ <- 0 until 100) {
      verifyFrame(spark.sql("select * from ptab2"), 5, 1)
    }
    val fdGrowth = getFdCount - initialFdCount
    assert(fdGrowth < 100)
  }

  private val csvFile1 = "people.csv"
  private val csvFile2 = "example.csv"
  private val csvFile3 = "example-tab.csv"

  ignore("read csv file without specifying original format") {
    // not implemented
    verifyFrame(spark.read.format("arrow")
        .load(ArrowDataSourceTest.locateResourcePath(csvFile1)), 1, 2)
  }

  test("read csv file") {
    val path = ArrowDataSourceTest.locateResourcePath(csvFile1)
    verifyFrame(
      spark.read
        .format("arrow")
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "csv")
        .load(path), 2, 3)
  }

  test("read csv file 2") {
    val path = ArrowDataSourceTest.locateResourcePath(csvFile2)
    verifyFrame(
      spark.read
          .format("arrow")
          .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "csv")
          .load(path), 34, 9)
  }

  test("read csv file 3 - tab separated") {
    val path = ArrowDataSourceTest.locateResourcePath(csvFile3)
    verifyFrame(
      spark.read
          .format("arrow")
          .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "csv")
          .option("delimiter", "\t")
          .load(path), 34, 9)
  }

  test("read csv file - programmatic API ") {
    val path = ArrowDataSourceTest.locateResourcePath(csvFile1)
    verifyFrame(
      spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "csv")
        .arrow(path), 2, 3)
  }

  def verifyFrame(frame: DataFrame, rowCount: Int, columnCount: Int): Unit = {
    assert(frame.schema.length === columnCount)
    assert(frame.collect().length === rowCount)
  }

  def verifyCsv(frame: DataFrame): Unit = {
    // todo assert something
  }

  def verifyParquet(frame: DataFrame): Unit = {
    verifyFrame(frame, 5, 1)
  }

  def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }

  def closeAllocators(): Unit = {
    SparkMemoryUtils.contextAllocator().close()
  }
}

object ArrowDataSourceTest {
  private def locateResourcePath(resource: String): String = {
    classOf[ArrowDataSourceTest].getClassLoader.getResource("")
      .getPath.concat(File.separator).concat(resource)
  }
}
