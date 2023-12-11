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
package org.apache.spark.sql.metric

import org.apache.spark.SparkContext
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter}

class SQLColumnarShuffleReadMetricsReporter(
    tempMetrics: TempShuffleReadMetrics,
    metrics: Map[String, SQLMetric])
  extends SQLShuffleReadMetricsReporter(tempMetrics, metrics) {

  private[this] val _batchesRead =
    metrics(SQLColumnarShuffleReadMetricsReporter.BATCHES_READ)

  private[this] val _recordsRead =
    metrics(SQLShuffleReadMetricsReporter.RECORDS_READ)

  override def incRecordsRead(v: Long): Unit = {
    _batchesRead.add(v)
    // tempMetrics.incRecordsRead(v)
  }

  def incBatchesRecordsRead(v: Long): Unit = {
    _recordsRead.add(v)
    tempMetrics.incRecordsRead(v)
  }

}

object SQLColumnarShuffleReadMetricsReporter {
  val BATCHES_READ = "batchesRead"

  def createShuffleReadMetrics(sc: SparkContext): Map[String, SQLMetric] = {
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sc) ++ Map(
      BATCHES_READ -> SQLMetrics.createMetric(sc, "batches read")
    )
  }
}
