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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.GlutenSQLTestsBaseTrait

import org.apache.hadoop.fs.Path

import java.io.File

import scala.collection.JavaConverters._

class GlutenParquetCompressionCodecPrecedenceSuite
  extends ParquetCompressionCodecPrecedenceSuite
  with GlutenSQLTestsBaseTrait {

  private def getTableCompressionCodec(path: String): Seq[String] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val codecs = for {
      footer <- readAllFootersWithoutSummaryFiles(new Path(path), hadoopConf)
      block <- footer.getParquetMetadata.getBlocks.asScala
      column <- block.getColumns.asScala
    } yield column.getCodec.name()
    codecs.distinct
  }

  private def createTableWithCompression(
      tableName: String,
      isPartitioned: Boolean,
      compressionCodec: String,
      rootDir: File): Unit = {
    val options =
      s"""
         |OPTIONS('path'='${rootDir.toURI.toString.stripSuffix("/")}/$tableName',
         |'parquet.compression'='$compressionCodec')
       """.stripMargin
    val partitionCreate = if (isPartitioned) "PARTITIONED BY (p)" else ""
    sql(s"""
           |CREATE TABLE $tableName USING Parquet $options $partitionCreate
           |AS SELECT 1 AS col1, 2 AS p
       """.stripMargin)
  }
  private def checkCompressionCodec(compressionCodec: String, isPartitioned: Boolean): Unit = {
    withTempDir {
      tmpDir =>
        val tempTableName = "TempParquetTable"
        withTable(tempTableName) {
          createTableWithCompression(tempTableName, isPartitioned, compressionCodec, tmpDir)
          val partitionPath = if (isPartitioned) "p=2" else ""
          val path = s"${tmpDir.getPath.stripSuffix("/")}/$tempTableName/$partitionPath"
          val realCompressionCodecs = getTableCompressionCodec(path)
          // Native parquet write currently not support LZ4_RAW
          // reference here: https://github.com/facebookincubator/velox/blob/d796cfc8c2a3cc045f
          // 1b33880c5839fec21a6b3b/velox/dwio/parquet/writer/Writer.cpp#L107C1-L120C17
          if (compressionCodec == "LZ4_RAW" || compressionCodec == "LZ4RAW") {
            assert(realCompressionCodecs.forall(_ == "SNAPPY"))
          } else {
            assert(realCompressionCodecs.forall(_ == compressionCodec))
          }
        }
    }
  }

  testGluten("Create parquet table with compression") {
    Seq(true, false).foreach {
      isPartitioned =>
        val codecs = Seq("UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD", "LZ4", "LZ4RAW", "LZ4_RAW")
        codecs.foreach {
          compressionCodec => checkCompressionCodec(compressionCodec, isPartitioned)
        }
    }
  }
}
