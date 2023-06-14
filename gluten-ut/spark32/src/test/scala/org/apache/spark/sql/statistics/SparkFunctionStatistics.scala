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
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.utils.SystemParameters
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{GlutenTestConstants, QueryTest, SparkSession}

import scala.util.control.Breaks.{break, breakable}

class SparkFunctionStatistics extends QueryTest {

  var spark: SparkSession = null

  protected def initializeSession(): Unit = {
    if (spark == null) {
      val sparkBuilder = SparkSession
          .builder()
          .appName("Gluten-UT")
          .master(s"local[2]")
          // Avoid static evaluation for literal input by spark catalyst.
          .config(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName +
              "," + ConstantFolding.ruleName + "," + NullPropagation.ruleName)
          .config("spark.driver.memory", "1G")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.shuffle.partitions", "1")
          .config("spark.sql.files.maxPartitionBytes", "134217728")
          .config("spark.memory.offHeap.enabled", "true")
          .config("spark.memory.offHeap.size", "1024MB")
          .config("spark.plugins", "io.glutenproject.GlutenPlugin")
          .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
          // Avoid the code size overflow error in Spark code generation.
          .config("spark.sql.codegen.wholeStage", "false")

      spark = if (BackendsApiManager.getBackendName.equalsIgnoreCase(
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

  def extractQuery(examples: String): Seq[String] = {
    examples.split("\n").map(_.trim).filter(!_.isEmpty).filter(_.startsWith(">"))
        .map(_.replace(">", ""))
  }

  test(GlutenTestConstants.GLUTEN_TEST + "Run spark function statistics: ") {
    initializeSession
    val functionRegistry = spark.sessionState.functionRegistry
    val sparkBuiltInFunctions = functionRegistry.listFunction()
    // According to expressionsForTimestampNTZSupport in FunctionRegistry.scala,
    // these functions are registered only for testing, not available for end users.
    val ignoreFunctions = FunctionRegistry.expressionsForTimestampNTZSupport.keySet
    val supportedFunctions = new java.util.ArrayList[String]()
    val unsupportedFunctions = new java.util.ArrayList[String]()
    val needInspectFunctions = new java.util.ArrayList[String]()
    for (func <- sparkBuiltInFunctions) {
      val exprInfo = functionRegistry.lookupFunction(func).get
      if (!ignoreFunctions.contains(exprInfo.getName)) {
        val examples = extractQuery(exprInfo.getExamples)
        if (examples.isEmpty) {
          needInspectFunctions.add(exprInfo.getName)
          print("###### Not found examples: " + exprInfo.getName + "\n")
        }
          var isSupported: Boolean = true
          breakable {
          for (example <- examples) {
            var executedPlan: SparkPlan = null
            try {
              executedPlan = spark.sql(example).queryExecution.executedPlan
            } catch {
              case _: Throwable =>
                needInspectFunctions.add(exprInfo.getName)
                print("------- " + exprInfo.getName + "\n")
                print(exprInfo.getExamples)
                break
            }
            val hasFallbackProject = executedPlan.find(_.isInstanceOf[ProjectExec]).isDefined
            if (hasFallbackProject) {
              isSupported = false
              break
            }
            val hasGlutenPlan = executedPlan.find(_.isInstanceOf[GlutenPlan]).isDefined
            if (!hasGlutenPlan) {
              isSupported = false
              break
            }
          }
          }
          if (isSupported && !needInspectFunctions.contains(exprInfo.getName)) {
            supportedFunctions.add(exprInfo.getName)
          } else if (!isSupported && !needInspectFunctions.contains(exprInfo.getName)) {
            unsupportedFunctions.add(exprInfo.getName)
          }
      }
    }
    print("overall functions: " + (sparkBuiltInFunctions.size - ignoreFunctions.size) + "\n")
    print("supported functions: " + supportedFunctions.size() + "\n")
    print("unsupported functions: " + unsupportedFunctions.size() + "\n")
    print("need inspect functions: " + needInspectFunctions.size() + "\n")
  }
}
