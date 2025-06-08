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
package org.apache.spark.sql.execution.python

import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

class GlutenExtractPythonUDFsSuite extends ExtractPythonUDFsSuite with GlutenSQLTestsBaseTrait {

  import testImplicits._

  def collectBatchExec(plan: SparkPlan): Seq[BatchEvalPythonExec] = plan.collect {
    case b: BatchEvalPythonExec => b
  }

  def collectArrowExec(plan: SparkPlan): Seq[EvalPythonExec] = plan.collect {
    case b: EvalPythonExec => b
  }

  testGluten("Python UDF should not break column pruning/filter pushdown -- Parquet V1") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath {
        f =>
          spark.range(10).select($"id".as("a"), $"id".as("b")).write.parquet(f.getCanonicalPath)
          val df = spark.read.parquet(f.getCanonicalPath)

          withClue("column pruning") {
            val query = df.filter(batchedPythonUDF($"a")).select($"a")

            val pythonEvalNodes = collectBatchExec(query.queryExecution.executedPlan)
            assert(pythonEvalNodes.length == 1)

            val scanNodes = query.queryExecution.executedPlan.collect {
              case scan: FileSourceScanExecTransformer => scan
            }
            assert(scanNodes.length == 1)
            assert(scanNodes.head.output.map(_.name) == Seq("a"))
          }

          withClue("filter pushdown") {
            val query = df.filter($"a" > 1 && batchedPythonUDF($"a"))
            val pythonEvalNodes = collectBatchExec(query.queryExecution.executedPlan)
            assert(pythonEvalNodes.length == 1)

            val scanNodes = query.queryExecution.executedPlan.collect {
              case scan: FileSourceScanExecTransformer => scan
            }
            assert(scanNodes.length == 1)
            // $"a" is not null and $"a" > 1
            assert(scanNodes.head.dataFilters.length == 2)
            assert(
              scanNodes.head.dataFilters.flatMap(_.references.map(_.name)).distinct == Seq("a"))
          }
      }
    }
  }
}
