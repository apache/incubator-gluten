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

package com.intel.oap.vectorized

import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils

/**
 * An Iterator that insures that the batches [[ColumnarBatch]]s it iterates over are all closed
 * properly.
 */
class CloseableColumnBatchIterator(itr: Iterator[ColumnarBatch])
    extends Iterator[ColumnarBatch]
    with Logging {
  var cb: ColumnarBatch = null

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      //logInfo(s"${itr} close ${cb}.")
      cb.close
      cb = null
    }
  }

  SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit]((tc: TaskContext) => {
      closeCurrentBatch()
    })

  override def hasNext: Boolean = {
    itr.hasNext
  }

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = itr.next()
    cb
  }
}
