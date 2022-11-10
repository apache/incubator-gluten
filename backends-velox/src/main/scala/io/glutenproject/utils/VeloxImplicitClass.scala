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

import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.vectorized.ArrowWritableColumnVector
import org.apache.arrow.vector.util.VectorBatchAppender

import org.apache.spark.sql.vectorized.ColumnarBatch

object VeloxImplicitClass {

  implicit class ArrowColumnarBatchRetainer(val cb: ColumnarBatch) {
    def retain(): Unit = {
      (0 until cb.numCols).toList.foreach(i =>
        ArrowColumnarBatches
          .ensureLoaded(ArrowBufferAllocators.contextInstance(), cb)
          .column(i).asInstanceOf[ArrowWritableColumnVector].retain())
    }
  }

  def coalesce(targetBatch: ColumnarBatch, batchesToAppend: List[ColumnarBatch]): Unit = {
    (0 until targetBatch.numCols).toList.foreach { i =>
      val targetVector =
        ArrowColumnarBatches
          .ensureLoaded(ArrowBufferAllocators.contextInstance(), targetBatch)
          .column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector
      val vectorsToAppend = batchesToAppend.map { cb =>
        ArrowColumnarBatches
          .ensureLoaded(ArrowBufferAllocators.contextInstance(), cb)
          .column(i).asInstanceOf[ArrowWritableColumnVector].getValueVector
      }
      VectorBatchAppender.batchAppend(targetVector, vectorsToAppend: _*)
    }
  }
}
