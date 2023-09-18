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
package org.apache.spark.sql.execution

import io.glutenproject.execution.WholeStageTransformerSuite
import io.glutenproject.utils.FallbackUtil

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.lit

import org.junit.Assert

class VeloxParquetWriteSuite extends WholeStageTransformerSuite {
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.gluten.sql.native.writer.enabled", "true")
  }

  test("test write parquet with compression codec") {
    // compression codec details see `VeloxParquetDatasource.cc`
    Seq("snappy", "gzip", "zstd", "none", "uncompressed")
      .foreach {
        codec =>
          val extension = codec match {
            case "none" | "uncompressed" => ""
            case "gzip" => "gz"
            case _ => codec
          }

          TPCHTables.foreach {
            case (_, df) =>
              withTempPath {
                f =>
                  df.write
                    .format("parquet")
                    .option("compression", codec)
                    .save(f.getCanonicalPath)
                  val files = f.list()
                  assert(files.nonEmpty, extension)
                  assert(files.exists(_.contains(extension)), extension)

                  val parquetDf = spark.read
                    .format("parquet")
                    .load(f.getCanonicalPath)
                  assert(df.schema.equals(parquetDf.schema))
                  checkAnswer(parquetDf, df)
              }
          }
      }
  }

  test("test ctas") {
    withTable("velox_ctas") {
      spark
        .range(100)
        .toDF("id")
        .createOrReplaceTempView("ctas_temp")
      val df = spark.sql("CREATE TABLE velox_ctas USING PARQUET AS SELECT * FROM ctas_temp")
      Assert.assertTrue(FallbackUtil.isFallback(df.queryExecution.executedPlan))
    }
  }

  test("test parquet dynamic partition write") {
    withTempPath {
      f =>
        val path = f.getCanonicalPath
        spark
          .range(100)
          .selectExpr("id as c1", "id % 7 as p")
          .createOrReplaceTempView("temp")
        val df = spark.sql(s"INSERT OVERWRITE DIRECTORY '$path' USING PARQUET SELECT * FROM temp")
        Assert.assertTrue(FallbackUtil.isFallback(df.queryExecution.executedPlan))
    }
  }

  test("test parquet bucket write") {
    withTable("bucket") {
      spark
        .range(100)
        .selectExpr("id as c1", "id % 7 as p")
        .createOrReplaceTempView("bucket_temp")
      val df = spark.sql(
        "CREATE TABLE bucket USING PARQUET CLUSTERED BY (p) INTO 7 BUCKETS " +
          "AS SELECT * FROM bucket_temp")
      Assert.assertTrue(FallbackUtil.isFallback(df.queryExecution.executedPlan))
    }
  }

  ignore("parquet write with empty dataframe") {
    withTempPath {
      f =>
        val df = spark.emptyDataFrame.select(lit(1).as("i"))
        df.write.format("parquet").save(f.getCanonicalPath)
        val res = spark.read.parquet(f.getCanonicalPath)
        checkAnswer(res, Nil)
        assert(res.schema.asNullable == df.schema.asNullable)
    }
  }
}
