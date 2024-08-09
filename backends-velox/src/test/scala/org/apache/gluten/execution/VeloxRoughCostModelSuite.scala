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

import org.apache.gluten.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.internal.SQLConf

class VeloxRoughCostModelSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark
      .range(100)
      .selectExpr("cast(id % 3 as int) as c1", "id as c2")
      .write
      .format("parquet")
      .saveAsTable("tmp1")
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(GlutenConfig.RAS_ENABLED.key, "true")
    .set(GlutenConfig.RAS_COST_MODEL.key, "rough")
    .set(SQLConf.USE_V1_SOURCE_LIST.key, "parquet")

  test("fallback trivial project if its neighbor nodes fell back") {
    withSQLConf(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
      runQueryAndCompare("select c1 as c3 from tmp1") {
        checkSparkOperatorMatch[ProjectExec]
      }
    }
  }
}
