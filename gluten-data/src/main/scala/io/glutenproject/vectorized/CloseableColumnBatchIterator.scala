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

package io.glutenproject.vectorized

import java.util.concurrent.TimeUnit

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.memory.TaskResources

/**
 * An Iterator that insures that the batches [[ColumnarBatch]]s it iterates over are all closed
 * properly.
 */
class CloseableColumnBatchIterator(itr: Iterator[ColumnarBatch],
                                   pipelineTime: Option[SQLMetric] = None)
  extends Iterator[ColumnarBatch]
    with Logging {
  var cb: ColumnarBatch = _
  var scanTime = 0L

  override def hasNext: Boolean = {
    val beforeTime = System.nanoTime()
    val res = itr.hasNext
    scanTime += System.nanoTime() - beforeTime
    if (!res) {
      pipelineTime.foreach(t => t += TimeUnit.NANOSECONDS.toMillis(scanTime))
    }
    res
  }

  TaskResources.addRecycler(100) {
    closeCurrentBatch()
  }

  override def next(): ColumnarBatch = {
    val beforeTime = System.nanoTime()
    closeCurrentBatch()
    cb = itr.next()
    scanTime += System.nanoTime() - beforeTime
    cb
  }

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close()
      cb = null
    }
  }
}
