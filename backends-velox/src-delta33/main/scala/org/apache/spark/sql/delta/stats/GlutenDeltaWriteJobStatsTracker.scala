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
package org.apache.spark.sql.delta.stats

import org.apache.gluten.execution.{PlaceholderRow, TerminalRow, VeloxColumnarToRowExec}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.DeltaIdentityColumnStatsTracker
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats, WriteTaskStatsTracker}
import org.apache.spark.sql.execution.metric.SQLMetric

class GlutenDeltaJobStatisticsTracker(val delegate: DeltaJobStatisticsTracker)
  extends WriteJobStatsTracker {
  import GlutenDeltaJobStatisticsTracker._

  override def newTaskInstance(): WriteTaskStatsTracker = {
    new GlutenDeltaTaskStatisticsTracker(
      delegate.newTaskInstance().asInstanceOf[DeltaTaskStatisticsTracker])
  }

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    delegate.processStats(stats, jobCommitTime)
  }
}

class GlutenDeltaIdentityColumnStatsTracker(override val delegate: DeltaIdentityColumnStatsTracker)
  extends GlutenDeltaJobStatisticsTracker(delegate)

private object GlutenDeltaJobStatisticsTracker {

  /**
   * This is a temporary implementation of statistics tracker for Delta Lake. It's sub-optimal in
   * performance because it internally performs C2R then send rows to the delegate row-based
   * tracker.
   *
   * TODO: Columnar-based statistics collection.
   */
  private class GlutenDeltaTaskStatisticsTracker(delegate: DeltaTaskStatisticsTracker)
    extends WriteTaskStatsTracker {

    private val c2r = new VeloxColumnarToRowExec.Converter(new SQLMetric("convertTime"))

    override def newPartition(partitionValues: InternalRow): Unit = {
      delegate.newPartition(partitionValues)
    }

    override def newFile(filePath: String): Unit = {
      delegate.newFile(filePath)
    }

    override def closeFile(filePath: String): Unit = {
      delegate.closeFile(filePath)
    }

    override def newRow(filePath: String, row: InternalRow): Unit = {
      row match {
        case _: PlaceholderRow =>
        case t: TerminalRow =>
          c2r.toRowIterator(t.batch()).foreach(eachRow => delegate.newRow(filePath, eachRow))
        case otherRow =>
          delegate.newRow(filePath, otherRow)
      }
    }

    override def getFinalStats(taskCommitTime: Long): WriteTaskStats = {
      delegate.getFinalStats(taskCommitTime)
    }
  }
}
