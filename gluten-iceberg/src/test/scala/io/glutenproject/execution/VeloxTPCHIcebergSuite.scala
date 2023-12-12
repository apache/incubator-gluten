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

import org.apache.spark.SparkConf

import org.apache.iceberg.spark.SparkWriteOptions

import java.io.File

class VeloxTPCHIcebergSuite extends VeloxTPCHSuite {

  protected val tpchBasePath: String = new File(
    "../backends-velox/src/test/resources").getAbsolutePath

  override protected val resourcePath: String =
    new File(tpchBasePath, "tpch-data-parquet-velox").getCanonicalPath

  override protected val veloxTPCHQueries: String =
    new File(tpchBasePath, "tpch-queries-velox").getCanonicalPath

  override protected val queriesResults: String =
    new File(tpchBasePath, "queries-output").getCanonicalPath

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hadoop")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/tpch-data-iceberg-velox")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    TPCHTables = TPCHTable
      .map(_.name)
      .map {
        table =>
          val tablePath = new File(resourcePath, table).getAbsolutePath
          val tableDF = spark.read.format(fileFormat).load(tablePath)
          tableDF.write.format("iceberg").mode("append").saveAsTable(table)
          (table, tableDF)
      }
      .toMap
  }

  test("iceberg transformer exists") {
    runQueryAndCompare("""
                         |SELECT
                         |  l_orderkey,
                         |  o_orderdate
                         |FROM
                         |  orders,
                         |  lineitem
                         |WHERE
                         |  l_orderkey = o_orderkey
                         |ORDER BY
                         |  l_orderkey,
                         |  o_orderdate
                         |LIMIT
                         |  10;
                         |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).count(
              plan => {
                plan.isInstanceOf[IcebergScanTransformer]
              }) == 2)
        }
    }
  }

  test("iceberg partition table") {
    withTable("lineitem_p") {
      val tablePath = new File(resourcePath, "lineitem").getAbsolutePath
      val tableDF = spark.read.format(fileFormat).load(tablePath)
      tableDF.write
        .format("iceberg")
        .option(SparkWriteOptions.FANOUT_ENABLED, "true")
        .mode("append")
        .saveAsTable("lineitem_p")
      runQueryAndCompare("""
                           |SELECT
                           |  sum(l_extendedprice * l_discount) AS revenue
                           |FROM
                           |  lineitem_p
                           |WHERE
                           |  l_shipdate >= '1994-01-01'
                           |  AND l_shipdate < '1995-01-01'
                           |  AND l_discount BETWEEN.06 - 0.01
                           |  AND.06 + 0.01
                           |  AND l_quantity < 24;
                           |""".stripMargin) {
        df =>
          {
            assert(
              getExecutedPlan(df).count(
                plan => {
                  plan.isInstanceOf[IcebergScanTransformer]
                }) == 1)
          }
      }
    }
  }
}
