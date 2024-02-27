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

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.GlutenTestConstants.GLUTEN_TEST
import org.apache.spark.sql.functions.{asc, col, lit}
import org.apache.spark.sql.types.{StructField, StructType}
class GlutenFileMetadataStructRowIndexSuite
  extends FileMetadataStructRowIndexSuite
  with GlutenSQLTestsBaseTrait {
  import testImplicits._
  override def withReadDataFrame(
      format: String,
      partitionCol: String = null,
      extraCol: String = "ec",
      extraSchemaFields: Seq[StructField] = Seq.empty)(f: DataFrame => Unit): Unit = {
    withTempPath {
      path =>
        val baseDf = spark
          .range(0, NUM_ROWS, 1, 1)
          .toDF("id")
          .withColumn(extraCol, $"id" + lit(1000 * 1000))
          .withColumn(EXPECTED_EXTRA_COL, col(extraCol))
        val writeSchema: StructType = if (partitionCol != null) {
          val writeDf = baseDf
            .withColumn(partitionCol, ($"id" / 10).cast("int") + lit(1000))
            .withColumn(EXPECTED_PARTITION_COL, col(partitionCol))
            .withColumn(EXPECTED_ROW_ID_COL, $"id" % 10)
            .orderBy(col(partitionCol), asc("id"))
          writeDf.write.format(format).partitionBy(partitionCol).save(path.getAbsolutePath)
          writeDf.schema
        } else {
          val writeDf = baseDf
            .withColumn(EXPECTED_ROW_ID_COL, $"id")
          writeDf.write.format(format).save(path.getAbsolutePath)
          writeDf.schema
        }
        val readSchema: StructType = new StructType(writeSchema.fields ++ extraSchemaFields)
        val readDf = spark.read.format(format).schema(readSchema).load(path.getAbsolutePath)
        f(readDf)
    }
  }

  test(s"$GLUTEN_TEST reading ${FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME} - present in a table") {
    withReadDataFrame("parquet", extraCol = FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME) {
      df =>
        // The UT make sure we fix:
        // (SPARK-40059): Allow users to include columns named
        //                    FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME in their schemas.
        // in current native implement.
        assert(
          df
            .where(col(EXPECTED_EXTRA_COL) === col(FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME))
            .count == NUM_ROWS)

        // Column cannot be read in combination with _metadata.row_index.
        intercept[AnalysisException](df.select("*", FileFormat.METADATA_NAME).collect())
        intercept[AnalysisException](
          df
            .select("*", s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}")
            .collect())
    }
  }
}
