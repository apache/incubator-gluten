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
package org.apache.spark.gluten.delta

import org.apache.gluten.execution.CreateMergeTreeSuite

import org.apache.spark.SparkConf

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenDeltaMergeTreeDeletionVectorSuite extends CreateMergeTreeSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
  }

  test("Gluten-9334: column `_tmp_metadata_row_index` and `file_path` not found") {
    val tableName = "delta_metadata_column"
    withTable(tableName) {
      withTempDir {
        dirName =>
          val s = createTableBuilder(tableName, "delta", s"$dirName/$tableName")
            .withProps(Map("delta.enableDeletionVectors" -> "'true'"))
            .withTableKey("lineitem")
            .build()
          spark.sql(s)

          spark.sql(s"insert into table $tableName select * from lineitem ")

          val df = sql(s"""
                          | select
                          |   _metadata.file_path,
                          |   _metadata.row_index
                          | from $tableName
                          | limit 1
                          |""".stripMargin)

          checkFallbackOperators(df, 0)
      }
    }
  }
}
// scalastyle:off line.size.limit
