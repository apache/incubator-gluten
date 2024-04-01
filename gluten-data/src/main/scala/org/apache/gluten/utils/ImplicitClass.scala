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

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.memory.arrowalloc.ArrowBufferAllocators
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.sql.vectorized.ColumnarBatch

object ImplicitClass {

  implicit class ArrowColumnarBatchRetainer(val cb: ColumnarBatch) {
    def retain(): Unit = {
      (0 until cb.numCols).toList.foreach(
        i =>
          ColumnarBatches
            .ensureLoaded(ArrowBufferAllocators.contextInstance(), cb)
            .column(i)
            .asInstanceOf[ArrowWritableColumnVector]
            .retain())
    }
  }
}
