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

import io.glutenproject.execution.FilterExecTransformerBase

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.io.File
import java.sql.Timestamp

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
  private val METADATA_FILE_MODIFICATION_TIME = "_metadata.file_modification_time"

  private def getMetadataForFile(f: File): Map[String, Any] = {
    Map(
      METADATA_FILE_PATH -> f.toURI.toString,
      METADATA_FILE_NAME -> f.getName,
      METADATA_FILE_SIZE -> f.length(),
      METADATA_FILE_MODIFICATION_TIME -> new Timestamp(f.lastModified())
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
      var fileScan = dfWithMetadata.queryExecution.executedPlan.collect {
        case f: FileSourceScanExec => f
      }
      assert(fileScan.size == 1)
      assert(fileScan(0).nodeNamePrefix == "NativeFile")

      // would fallback
      dfWithMetadata = df.select(METADATA_FILE_PATH, "file_path")
      checkAnswer(
        dfWithMetadata,
        Seq(
          Row(f0(METADATA_FILE_PATH), "jack"),
          Row(f1(METADATA_FILE_PATH), "lily")
        )
      )
      fileScan = dfWithMetadata.queryExecution.executedPlan.collect {
        case f: FileSourceScanExec => f
      }
      assert(fileScan(0).nodeNamePrefix == "File")
  }

  metadataColumnsNativeTest("plan check with metadata filter", schemaWithFilePathField) {
    (df, f0, f1) =>
      var filterDF = df
        .select("file_path", "age", METADATA_FILE_NAME)
        .where(Column(METADATA_FILE_NAME) === f0((METADATA_FILE_NAME)))
      val ret = filterDF.collect
      assert(ret.size == 1)
      var fileScan = filterDF.queryExecution.executedPlan.collect {
        case f: FileSourceScanExec => f
      }
      assert(fileScan.size == 1)
      assert(fileScan(0).nodeNamePrefix == "NativeFile")
      var filterExecs = filterDF.queryExecution.executedPlan.collect {
        case filter: FilterExecTransformerBase => filter
      }
      assert(filterExecs.size == 1)

      // case to check if file_path is URI string
      filterDF =
        df.select(METADATA_FILE_PATH).where(Column(METADATA_FILE_NAME) === f1((METADATA_FILE_NAME)))
      checkAnswer(
        filterDF,
        Seq(
          Row(f1(METADATA_FILE_PATH))
        )
      )
      fileScan = filterDF.queryExecution.executedPlan.collect { case f: FileSourceScanExec => f }
      assert(fileScan(0).nodeNamePrefix == "NativeFile")
      filterExecs = filterDF.queryExecution.executedPlan.collect {
        case filter: FilterExecTransformerBase => filter
      }
      assert(filterExecs.size == 1)
  }
}
