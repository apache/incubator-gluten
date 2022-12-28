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
package io.glutenproject.utils

import io.glutenproject.GlutenConfig

import org.apache.spark.TaskContext

object DebugUtil {
  // if not specify stageId, use default 1
  // if specify partitionId, use this one
  // if not, use specified taskId or save all the input of this stage
  def saveInputToFile(): Boolean = {
    if (!GlutenConfig.getSessionConf.debug) {
      return false
    }
    if (TaskContext.get().stageId() != GlutenConfig.getSessionConf.taskStageId) {
      return false
    }
    if (TaskContext.getPartitionId() == GlutenConfig.getSessionConf.taskPartitionId) {
      true
    } else if (GlutenConfig.getSessionConf.taskPartitionId == -1) {
      TaskContext.get().taskAttemptId() == GlutenConfig.getSessionConf.taskId ||
        GlutenConfig.getSessionConf.taskId == -1
    } else {
      false
    }
  }
}
