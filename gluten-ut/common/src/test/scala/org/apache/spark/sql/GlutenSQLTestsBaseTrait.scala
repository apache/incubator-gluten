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

package org.apache.spark.sql

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.utils.SystemParameters

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Basic trait for Gluten SQL test cases.
 */
trait GlutenSQLTestsBaseTrait extends SharedSparkSession with GlutenTestsBaseTrait {

  override protected def test(testName: String,
                              testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(testFun)
    } else {
      logWarning(s"Ignore test case: ${testName}")
    }
  }

  override def sparkConf: SparkConf = {
    // Native SQL configs
    val conf = super.sparkConf
      .setAppName("Gluten-UT")
      .set("spark.driver.memory", "1G")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.files.maxPartitionBytes", "134217728")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.warehouse.dir", warehouse)
      // Avoid static evaluation by spark catalyst. But there are some UT issues
      // coming from spark, e.g., expecting SparkException is thrown, but the wrapped
      // exception is thrown.
      // .set("spark.sql.optimizer.excludedRules", ConstantFolding.ruleName + "," +
      //     NullPropagation.ruleName)

    if (BackendsApiManager.getBackendName.equalsIgnoreCase(
      GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)) {
      conf
        .set("spark.io.compression.codec", "LZ4")
        .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
        .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
        .set("spark.gluten.sql.enable.native.validation", "false")
        .set(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getClickHouseLibPath)
        .set("spark.sql.files.openCostInBytes", "134217728")
        .set("spark.unsafe.exceptionOnMemoryLeak", "true")
    } else {
      conf.set("spark.unsafe.exceptionOnMemoryLeak", "false")
    }

    conf
  }
}
