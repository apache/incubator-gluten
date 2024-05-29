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

import org.apache.gluten.benchmarks.RandomParquetDataGenerator

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.types._

class VeloxScanSuite extends VeloxWholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  protected val veloxTPCHQueries: String = rootPath + "/tpch-queries-velox"
  protected val queriesResults: String = rootPath + "queries-output"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.adaptive.enabled", "false")

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("tpch q22 subquery filter pushdown - v1") {
    createTPCHNotNullTables()
    runTPCHQuery(22, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      df =>
        val plan = df.queryExecution.executedPlan
        val exist = plan.collect { case scan: FileSourceScanExecTransformer => scan }.exists {
          scan =>
            scan.filterExprs().exists {
              case _ @GreaterThan(_, _: ScalarSubquery) => true
              case _ => false
            }
        }
        assert(exist)
    }
  }

  test("tpch q22 subquery filter pushdown - v2") {
    withSQLConf("spark.sql.sources.useV1SourceList" -> "") {
      // Tables must be created here, otherwise v2 scan will not be used.
      createTPCHNotNullTables()
      runTPCHQuery(
        22,
        veloxTPCHQueries,
        queriesResults,
        compareResult = false,
        noFallBack = false) {
        df =>
          val plan = df.queryExecution.executedPlan
          val exist = plan.collect { case scan: BatchScanExecTransformer => scan }.exists {
            scan =>
              scan.filterExprs().exists {
                case _ @GreaterThan(_, _: ScalarSubquery) => true
                case _ => false
              }
          }
          assert(exist)
      }
    }
  }

  test("unsupported data type scan filter pushdown") {
    withTempView("t") {
      withTempDir {
        dir =>
          val path = dir.getAbsolutePath
          val schema = StructType(
            Array(
              StructField("short_decimal_field", DecimalType(5, 2), true),
              StructField("long_decimal_field", DecimalType(32, 8), true),
              StructField("binary_field", BinaryType, true),
              StructField("timestamp_field", TimestampType, true)
            ))
          RandomParquetDataGenerator(0).generateRandomData(spark, schema, 10, Some(path))
          spark.catalog.createTable("t", path, "parquet")
          runQueryAndCompare(
            """select * from t where long_decimal_field = 3.14)""".stripMargin
          )(checkGlutenOperatorMatch[FileSourceScanExecTransformer])
          runQueryAndCompare(
            """select * from t where short_decimal_field = 3.14""".stripMargin
          )(checkGlutenOperatorMatch[FileSourceScanExecTransformer])
          runQueryAndCompare(
            """select * from t where binary_field = '3.14'""".stripMargin
          )(checkGlutenOperatorMatch[FileSourceScanExecTransformer])
          runQueryAndCompare(
            """select * from t where timestamp_field = current_timestamp()""".stripMargin
          )(checkGlutenOperatorMatch[FileSourceScanExecTransformer])
      }
    }
  }
}
