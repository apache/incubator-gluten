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
package org.apache.spark.sql.statistics

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.utils.SystemParameters
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{GlutenTestConstants, GlutenTestsTrait, QueryTest, SQLQueryTestHelper, SparkSession}

class SparkFunctionStatistics extends QueryTest with SharedSparkSession with SQLHelper
    with SQLQueryTestHelper {

  protected var _spark: SparkSession = null

  override protected def initializeSession(): Unit = {
    if (_spark == null) {
      val sparkBuilder = SparkSession
          .builder()
          .appName("Gluten-UT")
          .master(s"local[2]")
          .config(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
          .config("spark.driver.memory", "1G")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.shuffle.partitions", "1")
          .config("spark.sql.files.maxPartitionBytes", "134217728")
          .config("spark.memory.offHeap.enabled", "true")
          .config("spark.memory.offHeap.size", "1024MB")
          .config("spark.plugins", "io.glutenproject.GlutenPlugin")
          .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
//          .config("spark.sql.warehouse.dir", warehouse)
          // Avoid static evaluation for literal input by spark catalyst.
          .config("spark.sql.optimizer.excludedRules", ConstantFolding.ruleName + "," +
              NullPropagation.ruleName)
          // Avoid the code size overflow error in Spark code generation.
          .config("spark.sql.codegen.wholeStage", "false")

      _spark = if (BackendsApiManager.getBackendName.equalsIgnoreCase(
        GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)) {
        sparkBuilder
            .config("spark.io.compression.codec", "LZ4")
            .config("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
            .config("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
            .config("spark.gluten.sql.enable.native.validation", "false")
            .config("spark.sql.files.openCostInBytes", "134217728")
            .config(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getClickHouseLibPath)
            .config("spark.unsafe.exceptionOnMemoryLeak", "true")
            .getOrCreate()
      } else {
        sparkBuilder
            .config("spark.unsafe.exceptionOnMemoryLeak", "true")
            .getOrCreate()
      }
    }
  }

//  def main(args: Array[String]): Unit = {
    test(GlutenTestConstants.GLUTEN_TEST + "Run spark function statistics: ") {
//      initializeSession
      val functionRegistry = _spark.sessionState.functionRegistry
      print(functionRegistry.lookupFunction(functionRegistry.listFunction().head).get.getExamples)
      val sparkBuiltInFunctions = functionRegistry.listFunction()
      for (func <- sparkBuiltInFunctions) {
        val exprInfo = functionRegistry.lookupFunction(func).get
        exprInfo.getName
      }
    }
}
