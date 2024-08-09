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

class VeloxRoughCostModelSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(GlutenConfig.RAS_ENABLED.key, "true")
    .set(GlutenConfig.RAS_COST_MODEL.key, "rough")

  import testImplicits._
  test("fallback trivial project if its neighbor nodes fell back") {
    withTempView("t1") {
      Seq(1, 1).toDF("c1").createOrReplaceTempView("t1")
      runQueryAndCompare("select c1 as c3 from t1") {
        checkSparkOperatorMatch[ProjectExec]
      }
    }
  }
}
