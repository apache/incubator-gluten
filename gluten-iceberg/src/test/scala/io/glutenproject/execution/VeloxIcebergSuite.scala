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
package io.glutenproject.execution

import io.glutenproject.GlutenConfig

import org.apache.spark.SparkConf

class VeloxIcebergSuite extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
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
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hadoop")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/tpch-data-iceberg-velox")
  }

  test("iceberg transformer exists") {
    spark.sql("""
                |create table iceberg_tb using iceberg as
                |(select 1 as col1, 2 as col2, 3 as col3)
                |""".stripMargin)

    runQueryAndCompare("""
                         |select * from iceberg_tb;
                         |""".stripMargin) {
      checkOperatorMatch[IcebergScanTransformer]
    }
  }

  test("iceberg partitioned table") {
    withTable("p_str_tb", "p_int_tb") {
      // Partition key of string type.
      withSQLConf(GlutenConfig.GLUTEN_ENABLE_KEY -> "false") {
        // Gluten does not support write iceberg table.
        spark.sql(
          """
            |create table p_str_tb(id int, name string, p string) using iceberg partitioned by (p);
            |""".stripMargin)
        spark.sql(
          """
            |insert into table p_str_tb values(1, 'a1', 'p1'), (2, 'a2', 'p1'), (3, 'a3', 'p2');
            |""".stripMargin
        )
      }
      runQueryAndCompare("""
                           |select * from p_str_tb;
                           |""".stripMargin) {
        checkOperatorMatch[IcebergScanTransformer]
      }

      // Partition key of integer type.
      withSQLConf(GlutenConfig.GLUTEN_ENABLE_KEY -> "false") {
        // Gluten does not support write iceberg table.
        spark.sql(
          """
            |create table p_int_tb(id int, name string, p int) using iceberg partitioned by (p);
            |""".stripMargin)
        spark.sql(
          """
            |insert into table p_int_tb values(1, 'a1', 1), (2, 'a2', 1), (3, 'a3', 2);
            |""".stripMargin
        )
      }
      runQueryAndCompare("""
                           |select * from p_int_tb;
                           |""".stripMargin) {
        checkOperatorMatch[IcebergScanTransformer]
      }
    }
  }
}
