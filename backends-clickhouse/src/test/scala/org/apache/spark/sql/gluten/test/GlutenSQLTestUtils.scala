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
package org.apache.spark.sql.gluten.test

import io.glutenproject.GlutenConfig
import io.glutenproject.utils.UTSystemParameters

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.commons.io.FileUtils

import java.io.File

trait GlutenSQLTestUtils extends SparkFunSuite with SharedSparkSession {
  protected val rootPath: String = getClass.getResource("/").getPath
  protected val basePath: String = rootPath + "unit-tests-working-home"
  protected val tablesPath: String = basePath + "/unit-tests-data"

  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.getClickHouseLibPath())
      .set(
        "spark.gluten.sql.columnar.backend.ch.use.v2",
        ClickHouseConfig.DEFAULT_USE_DATASOURCE_V2)
      .set(GlutenConfig.NATIVE_VALIDATION_ENABLED, false)
      .set(StaticSQLConf.WAREHOUSE_PATH, warehouse)

  override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    DeltaLog.clearCache()
    super.afterAll()
    // init GlutenConfig in the next beforeAll
    GlutenConfig.ins = null
  }
}
