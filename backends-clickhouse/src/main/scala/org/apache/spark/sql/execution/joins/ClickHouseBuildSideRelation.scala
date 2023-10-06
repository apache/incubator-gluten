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
package org.apache.spark.sql.execution.joins

import io.glutenproject.execution.{BroadCastHashJoinContext, ColumnarNativeIterator}
import io.glutenproject.utils.{IteratorUtil, PlanNodesUtil}
import io.glutenproject.vectorized._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.CHShuffleReadStreamFactory

import scala.collection.JavaConverters._

case class ClickHouseBuildSideRelation(
    mode: BroadcastMode,
    output: Seq[Attribute],
    batches: Array[Byte],
    newBuildKeys: Seq[Expression] = Seq.empty)
  extends BuildSideRelation
  with Logging {

  override def deserialized: Iterator[ColumnarBatch] = Iterator.empty

  override def asReadOnlyCopy(
      broadCastContext: BroadCastHashJoinContext): ClickHouseBuildSideRelation = this

  private var hashTableData: Long = 0L
  def buildHashTable(
      broadCastContext: BroadCastHashJoinContext): (Long, ClickHouseBuildSideRelation) =
    synchronized {
      if (hashTableData == 0) {
        logDebug(
          s"BHJ value size: " +
            s"${broadCastContext.buildHashTableId} = ${batches.length}")
        // Build the hash table
        hashTableData =
          StorageJoinBuilder.build(batches, broadCastContext, newBuildKeys.asJava, output.asJava)
        (hashTableData, this)
      } else {
        (StorageJoinBuilder.nativeCloneBuildHashTable(hashTableData), null)
      }
    }

  def reset(): Unit = synchronized {
    hashTableData = 0
  }

  /**
   * Transform columnar broadcast value to Array[InternalRow] by key and distinct.
   *
   * @return
   */
  override def transform(key: Expression): Array[InternalRow] = {
    // native block reader
    val blockReader = new CHStreamReader(CHShuffleReadStreamFactory.create(batches, true))
    val broadCastIter: Iterator[ColumnarBatch] = IteratorUtil.createBatchIterator(blockReader)
    // Expression compute, return block iterator
    val expressionEval = new SimpleExpressionEval(
      new ColumnarNativeIterator(broadCastIter.asJava),
      PlanNodesUtil.genProjectionsPlanNode(key, output))

    try {
      // convert columnar to row
      val converter = new CHBlockConverterJniWrapper()
      asScalaIterator(expressionEval).flatMap {
        block =>
          val batch = new CHNativeBlock(block)
          if (batch.numRows == 0) {
            Iterator.empty
          } else {
            val info = converter.convertColumnarToRow(block)

            new Iterator[InternalRow] {
              var rowId = 0
              val row = new UnsafeRow(batch.numColumns())
              var closed = false

              override def hasNext: Boolean = {
                val result = rowId < batch.numRows()
                if (!result && !closed) {
                  converter.freeMemory(info.memoryAddress, info.totalSize)
                  closed = true
                }
                result
              }

              override def next: UnsafeRow = {
                if (rowId >= batch.numRows()) throw new NoSuchElementException

                val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
                row.pointTo(null, info.memoryAddress + offset, length.toInt)
                rowId += 1
                row.copy()
              }
            }
          }
      }.toArray
    } finally {
      blockReader.close()
      expressionEval.close()
    }
  }
}
