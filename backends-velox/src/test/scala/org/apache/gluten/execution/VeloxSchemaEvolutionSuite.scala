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
import org.apache.spark.sql.Row

class VeloxSchemaEvolutionSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.gluten.sql.columnar.backend.velox.parquetUseColumnNames", "false")
    .set("spark.gluten.sql.columnar.backend.velox.orcUseColumnNames", "false")

  test("parquet change column names") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        spark
          .range(2)
          .selectExpr("id as a", "cast(id + 10 as string) as b")
          .write
          .mode("overwrite")
          .parquet(path)

        withTable("test") {
          sql("create table test (c long, d string) using parquet options (path '" + path + "')")
          val df = sql("select c, d from test")
          checkAnswer(df, Seq(Row(0L, "10"), Row(1L, "11")))
        }
    }
  }

  test("ORC change column names") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        spark
          .range(2)
          .selectExpr("id as a", "cast(id + 10 as string) as b")
          .write
          .mode("overwrite")
          .orc(path)

        withTable("test") {
          sql("create table test (c long, d string) using orc options (path '" + path + "')")
          val df = sql("select c, d from test")
          checkAnswer(df, Seq(Row(0L, "10"), Row(1L, "11")))
        }
    }
  }
}
