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
package org.apache.spark.rpc

import java.util

trait GlutenRpcMessage extends Serializable

object GlutenRpcMessages {
  case class GlutenRegisterExecutor(
      executorId: String,
      executorRef: RpcEndpointRef
  ) extends GlutenRpcMessage

  case class GlutenOnExecutionStart(executionId: String) extends GlutenRpcMessage

  case class GlutenOnExecutionEnd(executionId: String) extends GlutenRpcMessage

  case class GlutenExecutorRemoved(executorId: String) extends GlutenRpcMessage

  case class GlutenCleanExecutionResource(executionId: String, broadcastHashIds: util.Set[String])
    extends GlutenRpcMessage

  // for mergetree cache
  case class GlutenMergeTreeCacheLoad(
      mergeTreeTable: String,
      columns: util.Set[String],
      onlyMetaCache: Boolean)
    extends GlutenRpcMessage

  case class GlutenCacheLoadStatus(jobId: String)

  case class CacheJobInfo(status: Boolean, jobId: String, reason: String = "")
    extends GlutenRpcMessage

  case class GlutenFilesCacheLoad(files: Array[Byte]) extends GlutenRpcMessage

  case class GlutenFilesCacheLoadStatus(jobId: String)

  /** Start async native stack collection; driver returns a `requestId` immediately */
  case class GlutenStartNativeStackAsync(executorId: String) extends GlutenRpcMessage

  /** Internal async dump request delivered to executor (no options) */
  case class GlutenDumpNativeStackAsyncRequest(requestId: String) extends GlutenRpcMessage

  /** Executor reports async dump result back to driver */
  case class GlutenNativeStackAsyncResult(requestId: String, success: Boolean, message: String)
    extends GlutenRpcMessage

  /** Executor reports async dump partial chunk to driver */
  case class GlutenNativeStackAsyncChunk(requestId: String, chunk: String) extends GlutenRpcMessage

  /** Query async native stack status by requestId, driver returns JSON string */
  case class GlutenQueryNativeStackStatus(requestId: String) extends GlutenRpcMessage

  /** Query async native stack raw message by requestId, driver returns plain text */
  case class GlutenQueryNativeStackRaw(requestId: String) extends GlutenRpcMessage
}
