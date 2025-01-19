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

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, ValueVector}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.gluten.vectorized.ArrowWritableColumnVector
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

object ColumnarBatchUtils {

  /**
   * Returns a new ColumnarBatch that contains at most `limit` rows
   * from the given batch.
   *
   * If `limit >= batch.numRows()`, returns the original batch. Otherwise,
   * copies up to `limit` rows into new column vectors.
   *
   * @param batch the original batch
   * @param limit the maximum number of rows to include
   * @return a new pruned [[ColumnarBatch]] with row count = `limit`,
   *         or the original batch if no pruning is required
   */
  def pruneBatch(batch: ColumnarBatch, limit: Int): ColumnarBatch = {
    val totalRows = batch.numRows()
    if (limit >= totalRows) {
      // No need to prune
      batch
    } else {
      val numCols = batch.numCols()
      val prunedColumns = Array.ofDim[ColumnVector](numCols)

      for (colIdx <- 0 until numCols) {
        prunedColumns(colIdx) = pruneColumn(batch.column(colIdx), limit, colIdx)
      }

      new ColumnarBatch(prunedColumns, limit)
    }
  }

  /**
   * Prune a single column to the specified limit rows.
   *
   * @param original the original column
   * @param limit the number of rows to copy
   * @param colIndex the column index (used when creating a new ArrowWritableColumnVector)
   * @return a new ColumnVector containing up to `limit` rows
   */
  private def pruneColumn(original: ColumnVector, limit: Int, colIndex: Int): ColumnVector = {
    val arrowCol = original.asInstanceOf[ArrowWritableColumnVector]
    val sourceVec = arrowCol.getValueVector

    val targetVec = createEmptyVectorLike(sourceVec, limit)

    val tp = sourceVec.makeTransferPair(targetVec)
    for (i <- 0 until limit) {
      tp.copyValueSafe(i, i)
    }
    targetVec.setValueCount(limit)

    val prunedCol = new ArrowWritableColumnVector(
      targetVec,
      null,
      colIndex,
      limit,
      false
    )
    prunedCol.setValueCount(limit)
    prunedCol
  }

  private def createEmptyVectorLike(source: ValueVector, capacity: Int): ValueVector = {
    val field: Field            = source.getField
    val allocator: BufferAllocator = source.getAllocator
    val newFieldVector = field.createVector(allocator).asInstanceOf[FieldVector]

    newFieldVector.setInitialCapacity(capacity)
    newFieldVector.allocateNew()
    newFieldVector
  }
}
