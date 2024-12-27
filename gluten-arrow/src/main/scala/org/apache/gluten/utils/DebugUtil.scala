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
package org.apache.gluten.utils

import org.apache.gluten.GlutenConfig

import org.apache.spark.TaskContext

object DebugUtil {
  // if taskId is specified and matches, then do that task
  // if stageId is not specified or doesn't match, then do nothing
  // if specify stageId but no partitionId, then do all partitions for that stage
  // if specify stageId and partitionId, then only do that partition for that stage
  def saveInputToFile(): Boolean = {
    def taskIdMatches =
      GlutenConfig.getConf.benchmarkTaskId.nonEmpty &&
        GlutenConfig.getConf.benchmarkTaskId
          .split(",")
          .map(_.toLong)
          .contains(TaskContext.get().taskAttemptId())

    def partitionIdMatches =
      TaskContext.get().stageId() == GlutenConfig.getConf.benchmarkStageId &&
        (GlutenConfig.getConf.benchmarkPartitionId.isEmpty ||
          GlutenConfig.getConf.benchmarkPartitionId
            .split(",")
            .map(_.toInt)
            .contains(TaskContext.get().partitionId()))

    val saveInput = taskIdMatches || partitionIdMatches
    if (saveInput) {
      if (GlutenConfig.getConf.benchmarkSaveDir.isEmpty) {
        throw new IllegalArgumentException(GlutenConfig.BENCHMARK_SAVE_DIR.key + " is not set.")
      }
    }
    saveInput
  }
}
