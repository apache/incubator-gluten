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
package io.glutenproject.expression

import io.glutenproject.execution.WholeStageTransformerSuite

import org.apache.spark.SparkConf

class VeloxUdfSuite extends WholeStageTransformerSuite {
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  private val udfExampleLibrary =
    this.getClass.getResource("/").getPath + "../../../../cpp/build/velox/udf/examples/libmyudf.so"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.unsafe.exceptionOnMemoryLeak", "true")
    .set(
      "spark.gluten.sql.columnar.backend.velox.udfLibraryPaths",
      System.getProperty("velox.udf.lib.path", udfExampleLibrary))

  test("test udf") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.sql.adaptive.enabled", "true"),
      ("spark.gluten.sql.columnar.forceShuffledHashJoin", "true"),
      ("spark.sql.sources.useV1SourceList", "avro")
    ) {
      createTPCHNotNullTables()

      val df = spark.sql("""select myudf1(1), myudf2(100L)""")
      df.collect().sameElements(Array(6, 105))
    }
  }
}
