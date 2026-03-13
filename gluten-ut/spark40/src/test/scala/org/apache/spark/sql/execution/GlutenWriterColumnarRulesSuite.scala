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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.VeloxColumnarToCarrierRowExec
import org.apache.spark.sql.{Dataset, GlutenSQLTestsBaseTrait, QueryTest, SaveMode}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.OverwriteByExpressionExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.QueryExecutionListener

class GlutenWriterColumnarRulesSuite extends QueryTest with SharedSparkSession with GlutenSQLTestsBaseTrait {

  override protected def shouldRun(testName: String): Boolean = true

  test("AdaptiveSparkPlanExec should be accessible in noop write plan") {
    var capturedPlan: SparkPlan = null
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        capturedPlan = qe.executedPlan
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    spark.listenerManager.register(listener)
    try {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true") {
        val df = Dataset.ofRows(spark, spark.range(100).repartition(10).logicalPlan)
        df.write.format("noop").mode(SaveMode.Overwrite).save()

        assert(capturedPlan != null, "Plan should have been captured by the listener")
        capturedPlan match {
          case OverwriteByExpressionExec(AdaptiveSparkPlanExec(_: VeloxColumnarToCarrierRowExec, _, _, _ ,_), _, _) =>
          case _ =>
            fail("Expected AdaptiveSparkPlanExec to be accessible in the plan structure")
        }
      }
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}
