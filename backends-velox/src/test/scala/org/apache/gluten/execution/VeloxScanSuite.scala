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
import org.apache.gluten.backendsapi.velox.VeloxBackendSettings
import org.apache.gluten.utils.VeloxFileSystemValidationJniWrapper

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.execution.ScalarSubquery

class VeloxScanSuite extends VeloxWholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  protected val tpchQueries: String =
    rootPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries"
  protected val queriesResults: String = rootPath + "queries-output"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.adaptive.enabled", "false")

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("tpch q22 subquery filter pushdown - v1") {
    createTPCHNotNullTables()
    runTPCHQuery(22, tpchQueries, queriesResults, compareResult = false, noFallBack = false) {
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
      runTPCHQuery(22, tpchQueries, queriesResults, compareResult = false, noFallBack = false) {
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

  test("Test file scheme validation") {
    withTempPath {
      path =>
        withSQLConf(GlutenConfig.NATIVE_WRITER_ENABLED.key -> "false") {
          spark
            .range(100)
            .selectExpr("cast(id % 9 as int) as c1")
            .write
            .format("parquet")
            .save(path.getCanonicalPath)
          runQueryAndCompare(s"SELECT count(*) FROM `parquet`.`${path.getCanonicalPath}`") {
            df =>
              val plan = df.queryExecution.executedPlan
              val fileScan = collect(plan) { case s: FileSourceScanExecTransformer => s }
              assert(fileScan.size == 1)
              val rootPaths = fileScan(0).getRootPathsInternal
              assert(rootPaths.length == 1)
              assert(rootPaths(0).startsWith("file:/"))
              assert(
                VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
                  rootPaths.toArray))
          }
        }
    }
    val filteredRootPath =
      VeloxBackendSettings.distinctRootPaths(
        Seq("file:/test_path/", "test://test/s", "test://test1/s"))
    assert(filteredRootPath.length == 1)
    assert(filteredRootPath(0).startsWith("test://"))
    assert(
      VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
        Array("file:/test_path/")))
    assert(
      !VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
        Array("unsupported://test_path")))
    assert(
      !VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
        Array("file:/test_path/", "unsupported://test_path")))
  }
}
