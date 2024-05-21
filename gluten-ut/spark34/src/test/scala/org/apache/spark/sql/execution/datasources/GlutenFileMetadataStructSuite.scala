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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.execution.{FileSourceScanExecTransformer, FilterExecTransformer}
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.io.File
import java.sql.Timestamp

import scala.reflect.ClassTag

class GlutenFileMetadataStructSuite extends FileMetadataStructSuite with GlutenSQLTestsBaseTrait {

  val schemaWithFilePathField: StructType = new StructType()
    .add(StructField("file_path", StringType))
    .add(StructField("age", IntegerType))
    .add(
      StructField(
        "info",
        new StructType()
          .add(StructField("id", LongType))
          .add(StructField("university", StringType))))

  private val METADATA_FILE_PATH = "_metadata.file_path"
  private val METADATA_FILE_NAME = "_metadata.file_name"
  private val METADATA_FILE_SIZE = "_metadata.file_size"
  private val METADATA_FILE_BLOCK_START = "_metadata.file_block_start"
  private val METADATA_FILE_BLOCK_LENGTH = "_metadata.file_block_length"
  private val METADATA_FILE_MODIFICATION_TIME = "_metadata.file_modification_time"
  private val METADATA_ROW_INDEX = "_metadata.row_index"
  private val FILE_FORMAT = "fileFormat"

  private def getMetadataRow(f: Map[String, Any]): Row = f(FILE_FORMAT) match {
    case "parquet" =>
      Row(
        f(METADATA_FILE_PATH),
        f(METADATA_FILE_NAME),
        f(METADATA_FILE_SIZE),
        f(METADATA_FILE_BLOCK_START),
        f(METADATA_FILE_BLOCK_LENGTH),
        f(METADATA_FILE_MODIFICATION_TIME),
        f(METADATA_ROW_INDEX)
      )
    case _ =>
      Row(
        f(METADATA_FILE_PATH),
        f(METADATA_FILE_NAME),
        f(METADATA_FILE_SIZE),
        f(METADATA_FILE_BLOCK_START),
        f(METADATA_FILE_BLOCK_LENGTH),
        f(METADATA_FILE_MODIFICATION_TIME)
      )
  }

  private def getMetadataForFile(f: File): Map[String, Any] = {
    Map(
      METADATA_FILE_PATH -> f.toURI.toString,
      METADATA_FILE_NAME -> f.getName,
      METADATA_FILE_SIZE -> f.length(),
      // test file is small enough so we would not do splitting files,
      // then the file block start is always 0 and file block length is same with file size
      METADATA_FILE_BLOCK_START -> 0,
      METADATA_FILE_BLOCK_LENGTH -> f.length(),
      METADATA_FILE_MODIFICATION_TIME -> new Timestamp(f.lastModified()),
      METADATA_ROW_INDEX -> 0,
      FILE_FORMAT -> f.getName.split("\\.").last
    )
  }

  private def metadataColumnsNativeTest(testName: String, fileSchema: StructType)(
      f: (DataFrame, Map[String, Any], Map[String, Any]) => Unit): Unit = {
    Seq("parquet").foreach {
      testFileFormat =>
        test(s"$GLUTEN_TEST metadata struct ($testFileFormat): " + testName) {
          withTempDir {
            dir =>
              import scala.collection.JavaConverters._

              // 1. create df0 and df1 and save under /data/f0 and /data/f1
              val df0 = spark.createDataFrame(data0.asJava, fileSchema)
              val f0 = new File(dir, "data/f0").getCanonicalPath
              df0.coalesce(1).write.format(testFileFormat).save(f0)

              val df1 = spark.createDataFrame(data1.asJava, fileSchema)
              val f1 = new File(dir, "data/f1 gluten").getCanonicalPath
              df1.coalesce(1).write.format(testFileFormat).save(f1)

              // 2. read both f0 and f1
              val df = spark.read
                .format(testFileFormat)
                .schema(fileSchema)
                .load(new File(dir, "data").getCanonicalPath + "/*")
              val realF0 = new File(dir, "data/f0")
                .listFiles()
                .filter(_.getName.endsWith(s".$testFileFormat"))
                .head
              val realF1 = new File(dir, "data/f1 gluten")
                .listFiles()
                .filter(_.getName.endsWith(s".$testFileFormat"))
                .head
              f(df, getMetadataForFile(realF0), getMetadataForFile(realF1))
          }
        }
    }
  }

  def checkOperatorMatch[T](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => plan.getClass == tag.runtimeClass))
  }

  metadataColumnsNativeTest(
    "plan check with metadata and user data select",
    schemaWithFilePathField) {
    (df, f0, f1) =>
      var dfWithMetadata = df.select(
        METADATA_FILE_NAME,
        METADATA_FILE_PATH,
        METADATA_FILE_SIZE,
        METADATA_FILE_MODIFICATION_TIME,
        "age")
      dfWithMetadata.collect
      if (BackendTestUtils.isVeloxBackendLoaded()) {
        checkOperatorMatch[FileSourceScanExecTransformer](dfWithMetadata)
      } else {
        checkOperatorMatch[FileSourceScanExec](dfWithMetadata)
      }

      // would fallback
      dfWithMetadata = df.select(METADATA_FILE_PATH, "file_path")
      checkAnswer(
        dfWithMetadata,
        Seq(
          Row(f0(METADATA_FILE_PATH), "jack"),
          Row(f1(METADATA_FILE_PATH), "lily")
        )
      )
      checkOperatorMatch[FileSourceScanExec](dfWithMetadata)
  }

  metadataColumnsNativeTest("plan check with metadata filter", schemaWithFilePathField) {
    (df, f0, f1) =>
      var filterDF = df
        .select("file_path", "age", METADATA_FILE_NAME)
        .where(Column(METADATA_FILE_NAME) === f0((METADATA_FILE_NAME)))
      val ret = filterDF.collect
      assert(ret.size == 1)
      if (BackendTestUtils.isVeloxBackendLoaded()) {
        checkOperatorMatch[FileSourceScanExecTransformer](filterDF)
      } else {
        checkOperatorMatch[FileSourceScanExec](filterDF)
      }
      checkOperatorMatch[FilterExecTransformer](filterDF)

      // case to check if file_path is URI string
      filterDF =
        df.select(METADATA_FILE_PATH).where(Column(METADATA_FILE_NAME) === f1((METADATA_FILE_NAME)))
      checkAnswer(
        filterDF,
        Seq(
          Row(f1(METADATA_FILE_PATH))
        )
      )
      if (BackendTestUtils.isVeloxBackendLoaded()) {
        checkOperatorMatch[FileSourceScanExecTransformer](filterDF)
      } else {
        checkOperatorMatch[FileSourceScanExec](filterDF)
      }
      checkOperatorMatch[FilterExecTransformer](filterDF)
  }

  metadataColumnsNativeTest("select only metadata", schema) {
    (df, f0, f1) =>
      checkAnswer(
        df.select(
          METADATA_FILE_NAME,
          METADATA_FILE_PATH,
          METADATA_FILE_SIZE,
          METADATA_FILE_BLOCK_START,
          METADATA_FILE_BLOCK_LENGTH,
          METADATA_FILE_MODIFICATION_TIME),
        Seq(
          Row(
            f0(METADATA_FILE_NAME),
            f0(METADATA_FILE_PATH),
            f0(METADATA_FILE_SIZE),
            f0(METADATA_FILE_BLOCK_START),
            f0(METADATA_FILE_BLOCK_LENGTH),
            f0(METADATA_FILE_MODIFICATION_TIME)
          ),
          Row(
            f1(METADATA_FILE_NAME),
            f1(METADATA_FILE_PATH),
            f1(METADATA_FILE_SIZE),
            f1(METADATA_FILE_BLOCK_START),
            f1(METADATA_FILE_BLOCK_LENGTH),
            f1(METADATA_FILE_MODIFICATION_TIME)
          )
        )
      )
      checkAnswer(
        df.select("name", "_metadata"),
        Seq(
          Row("jack", getMetadataRow(f0)),
          Row("lily", getMetadataRow(f1))
        )
      )
  }

  testGluten("SPARK-41896: Filter on constant and generated metadata attributes at the same time") {
    withTempPath {
      dir =>
        val idColumnName = "id"
        val partitionColumnName = "partition"
        val numFiles = 4
        val totalNumRows = 40

        spark
          .range(end = totalNumRows)
          .toDF(idColumnName)
          .withColumn(partitionColumnName, col(idColumnName).mod(lit(numFiles)))
          .write
          .partitionBy(partitionColumnName)
          .format("parquet")
          .save(dir.getAbsolutePath)

        // Get one file path.
        val randomTableFilePath = spark.read
          .load(dir.getAbsolutePath)
          .select(METADATA_FILE_PATH)
          .collect()
          .head
          .getString(0)

        // Select half the rows from one file.
        val halfTheNumberOfRowsPerFile = totalNumRows / (numFiles * 2)
        val collectedRows = spark.read
          .load(dir.getAbsolutePath)
          .select("id", METADATA_FILE_PATH, METADATA_ROW_INDEX)
          .where(col(METADATA_FILE_PATH).equalTo(lit(randomTableFilePath)))
          .where(col(METADATA_ROW_INDEX).leq(lit(halfTheNumberOfRowsPerFile)))
          .collect()

        // Assert we only select rows from one file.
        assert(collectedRows.map(_.getString(1)).distinct.length === 1)
        // Assert we filtered by row index.
        assert(collectedRows.forall(row => row.getLong(2) < halfTheNumberOfRowsPerFile))
        assert(collectedRows.length === halfTheNumberOfRowsPerFile)
    }
  }

  testGluten("SPARK-43450: Filter on full _metadata column struct") {
    withTempPath {
      dir =>
        val numRows = 10
        spark
          .range(end = numRows)
          .toDF()
          .write
          .format("parquet")
          .save(dir.getAbsolutePath)

        // Get the metadata of a random row. The metadata is unique per row because of row_index.
        val metadataColumnRow = spark.read
          .load(dir.getAbsolutePath)
          .select("id", "_metadata")
          .collect()
          .head
          .getStruct(1)

        // Transform the result into a literal that can be used in an expression.
        val metadataColumnFields = metadataColumnRow.schema.fields
          .map(field => lit(metadataColumnRow.getAs[Any](field.name)).as(field.name))
        val metadataColumnStruct = struct(metadataColumnFields: _*)

        val selectSingleRowDf = spark.read
          .load(dir.getAbsolutePath)
          .where(col("_metadata").equalTo(lit(metadataColumnStruct)))

        assert(selectSingleRowDf.collect().size === 1)
    }
  }

  testGluten("SPARK-43450: Filter on aliased _metadata.row_index") {
    withTempPath {
      dir =>
        val numRows = 10
        spark
          .range(start = 0, end = numRows, step = 1, numPartitions = 1)
          .toDF()
          .write
          .format("parquet")
          .save(dir.getAbsolutePath)

        // There is only one file, so row_index is unique.
        val selectSingleRowDf = spark.read
          .load(dir.getAbsolutePath)
          .select(col("id"), col("_metadata"), col("_metadata.row_index").as("renamed_row_index"))
          .where(col("renamed_row_index").equalTo(lit(0)))

        assert(selectSingleRowDf.collect().size === 1)
    }
  }
}
