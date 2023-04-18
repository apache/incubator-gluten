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

package org.apache.spark.shuffle

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

import org.apache.spark._
import org.apache.spark.internal.Logging

import java.io.IOException

class CelebornPartitionPusher(
    val appId: String,
    val shuffleId: Int,
    val numMappers: Int,
    val numPartitions: Int,
    val context: TaskContext,
    val mapId: Int,
    val client: ShuffleClient,
    val celebornConf: CelebornConf)
  extends Logging {

  @throws[IOException]
  def pushPartitionData(partitionId: Int, buffer: Array[Byte]): Int = {
    logDebug(s"Push record, size ${buffer.length}.")
    if (buffer.length > celebornConf.pushBufferMaxSize) {
      client.pushData(
        appId,
        shuffleId,
        mapId,
        context.attemptNumber,
        partitionId,
        buffer,
        0,
        buffer.length,
        numMappers,
        numPartitions)
    } else {
      client.mergeData(
        appId,
        shuffleId,
        mapId,
        context.attemptNumber,
        partitionId,
        buffer,
        0,
        buffer.length,
        numMappers,
        numPartitions)
    }
  }
}
