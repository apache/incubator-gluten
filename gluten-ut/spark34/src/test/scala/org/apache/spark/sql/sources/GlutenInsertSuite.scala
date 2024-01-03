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
package org.apache.spark.sql.sources

import org.apache.spark.SparkConf
import org.apache.spark.executor.OutputMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.{CommandResultExec, QueryExecution, VeloxColumnarWriteFilesExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.util.QueryExecutionListener

class GlutenInsertSuite extends InsertSuite with GlutenSQLTestsBaseTrait {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.leafNodeDefaultParallelism", "1")
  }

  test("Gluten: insert partition table") {
    withTable("pt") {
      spark.sql("CREATE TABLE pt (c1 int, c2 string) USING PARQUET PARTITIONED BY (pt string)")

      var taskMetrics: OutputMetrics = null
      val taskListener = new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          taskMetrics = taskEnd.taskMetrics.outputMetrics
        }
      }

      var sqlMetrics: Map[String, SQLMetric] = null
      val queryListener = new QueryExecutionListener {
        override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}
        override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
          qe.executedPlan match {
            case dataWritingCommandExec: DataWritingCommandExec =>
              sqlMetrics = dataWritingCommandExec.cmd.metrics
            case _ =>
          }
        }
      }
      spark.sparkContext.addSparkListener(taskListener)
      spark.listenerManager.register(queryListener)
      try {
        val df =
          spark.sql("INSERT INTO TABLE pt partition(pt='a') SELECT * FROM VALUES(1, 'a'),(2, 'b')")
        spark.sparkContext.listenerBus.waitUntilEmpty()
        val writeFiles = df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .children
          .head
        assert(writeFiles.isInstanceOf[VeloxColumnarWriteFilesExec])

        assert(taskMetrics.bytesWritten > 0)
        assert(taskMetrics.recordsWritten == 2)
        assert(sqlMetrics("numParts").value == 1)
        assert(sqlMetrics("numOutputRows").value == 2)
        assert(sqlMetrics("numOutputBytes").value > 0)
        assert(sqlMetrics("numFiles").value == 1)

      } finally {
        spark.sparkContext.removeSparkListener(taskListener)
        spark.listenerManager.unregister(queryListener)
      }
    }
  }
}
