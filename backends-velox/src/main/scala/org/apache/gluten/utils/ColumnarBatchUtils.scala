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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatchJniWrapper}
import org.apache.gluten.runtime.{Runtime, Runtimes}

import org.apache.spark.sql.vectorized.ColumnarBatch

object ColumnarBatchUtils {

  /**
   * Returns a new ColumnarBatch that contains at most `limit` rows from the given batch.
   *
   * If `limit >= batch.numRows()`, returns the original batch. Otherwise, copies up to `limit` rows
   * into new column vectors.
   *
   * @param batch
   *   the original batch
   * @param limit
   *   the maximum number of rows to include
   * @return
   *   a new pruned [[ColumnarBatch]] with row count = `limit`, or the original batch if no pruning
   *   is required
   */
  def pruneBatch(batch: ColumnarBatch, limit: Int): ColumnarBatch = {
    val totalRows = batch.numRows()
    if (limit >= totalRows) {
      // No need to prune
      batch
    } else {
      val runtime: Runtime = Runtimes.contextInstance(
        BackendsApiManager.getBackendName,
        "VeloxColumnarBatches#pruneBatch")
      val nativeHandle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, batch)
      val handle: Long =
        VeloxColumnarBatchJniWrapper.create(runtime).pruneBatch(nativeHandle, limit)
      ColumnarBatches.create(handle)
    }
  }
}
