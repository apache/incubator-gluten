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

abstract class PaimonSuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/data-paimon")
  }

  test("paimon transformer exists") {
    Seq("parquet", "orc").foreach {
      format =>
        withTable(s"paimon_${format}_tbl") {
          sql(s"DROP TABLE IF EXISTS paimon_${format}_tbl")
          sql(
            s"CREATE TABLE paimon_${format}_tbl (id INT, name STRING) USING PAIMON " +
              s"TBLPROPERTIES ('file.format'='$format')")
          sql(s"INSERT INTO paimon_${format}_tbl VALUES (1, 'Bob'), (2, 'Blue'), (3, 'Mike')")
          runQueryAndCompare(s"""
                                |SELECT * FROM paimon_${format}_tbl;
                                |""".stripMargin) {
            checkGlutenPlan[PaimonScanTransformer]
          }
        }
    }
  }

  test("paimon partitioned table") {
    withTable("paimon_tbl") {
      sql("DROP TABLE IF EXISTS paimon_tbl")
      sql(
        "CREATE TABLE paimon_tbl (id INT, p1 STRING, p2 STRING) USING paimon PARTITIONED BY (p1, p2)")
      sql("INSERT INTO paimon_tbl VALUES (1, '1', '1'), (2, '1', '2')")
      runQueryAndCompare("""
                           |SELECT p1 FROM paimon_tbl WHERE p1 = '1' ORDER BY id;
                           |""".stripMargin) {
        checkGlutenPlan[PaimonScanTransformer]
      }
      runQueryAndCompare("""
                           |SELECT p2 FROM paimon_tbl WHERE p1 = '1' ORDER BY id;
                           |""".stripMargin) {
        checkGlutenPlan[PaimonScanTransformer]
      }
      runQueryAndCompare("""
                           |SELECT p1 FROM paimon_tbl WHERE p2 = '1';
                           |""".stripMargin) {
        checkGlutenPlan[PaimonScanTransformer]
      }
      runQueryAndCompare("""
                           |SELECT p2 FROM paimon_tbl WHERE p2 = '1';
                           |""".stripMargin) {
        checkGlutenPlan[PaimonScanTransformer]
      }
      runQueryAndCompare("""
                           |SELECT id, p2 FROM paimon_tbl WHERE p1 = '1' and p2 = '2';
                           |""".stripMargin) {
        checkGlutenPlan[PaimonScanTransformer]
      }
      runQueryAndCompare("""
                           |SELECT id FROM paimon_tbl ORDER BY id;
                           |""".stripMargin) {
        checkGlutenPlan[PaimonScanTransformer]
      }
    }
  }

  test("paimon transformer exists with bucket table") {
    withTable(s"paimon_tbl") {
      sql(s"""
             |CREATE TABLE paimon_tbl (id INT, name STRING)
             |USING paimon
             |TBLPROPERTIES (
             | 'bucket' = '1',
             | 'bucket-key' = 'id'
             |)
             |""".stripMargin)
      sql(s"INSERT INTO paimon_tbl VALUES (1, 'Bob'), (2, 'Blue'), (3, 'Mike')")
      runQueryAndCompare("SELECT * FROM paimon_tbl") {
        checkGlutenPlan[PaimonScanTransformer]
      }
    }
  }
}
