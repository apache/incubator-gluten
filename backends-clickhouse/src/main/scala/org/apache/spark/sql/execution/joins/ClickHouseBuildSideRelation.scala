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

import java.io.ByteArrayInputStream

import scala.collection.mutable

import io.glutenproject.execution.BroadCastHashJoinContext
import io.glutenproject.row.SparkRowInfo
import io.glutenproject.vectorized.{BlockNativeConverter, BlockNativeReader, CHNativeBlock, StorageJoinBuilder}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ClickHouseBuildSideRelation(mode: BroadcastMode,
                                       output: Seq[Attribute],
                                       batches: Array[Array[Byte]])
  extends BuildSideRelation with Logging {

  override def deserialized: Iterator[ColumnarBatch] = Iterator.empty

  override def asReadOnlyCopy(broadCastContext: BroadCastHashJoinContext)
  : ClickHouseBuildSideRelation = {
    val allBatches = batches.flatten
    logDebug(s"BHJ value size: " +
      s"${broadCastContext.buildHashTableId} = ${allBatches.size}")
    val storageJoinBuilder = new StorageJoinBuilder(
      new ByteArrayInputStream(allBatches),
      broadCastContext)
    // Build the hash table
    storageJoinBuilder.build()
    this
  }

  override def broadcastMode: BroadcastMode = mode

  /**
    * Convert broadcasted value to Iterator[InternalRow].
    *
    * @return
    */
  override def convertColumnarToInternalRow: (Long, Iterator[InternalRow]) = {
    val allBatches = batches.flatten
    val reader = new BlockNativeReader(new ByteArrayInputStream(allBatches))
    val blocks = mutable.MutableList[Long]()
    val blockRows = mutable.MutableList[Long]()
    var total_rows = 0
    var column_number = 0
    while (reader.hasNext) {
      val block = reader.next()
      val nativeBlock = new CHNativeBlock(block)
      val rows = nativeBlock.numRows()
      if (column_number == 0) {
        column_number = nativeBlock.numColumns()
      }
      total_rows += rows
      blockRows+=rows
      blocks+= block
    }
    val iter: Iterator[InternalRow] = new Iterator[InternalRow] {
      val converter = new BlockNativeConverter()
      var sparkRowInfo : SparkRowInfo = converter.converColumarToRow(blocks.head)
      var row = new UnsafeRow(column_number)
      var current_block = 0
      var current_row = 0
      var closed = false

      override def hasNext: Boolean = {
        // block and row are out of range
        if (current_row > blockRows(current_block) && current_block >= blocks.length - 1) {
          if (!closed) {
            closed = true
            converter.freeMemory(sparkRowInfo.memoryAddress, sparkRowInfo.totalSize)
          }
          return false
        };
        // next row in current block
        if (current_row < blockRows(current_block) - 1) {
          current_row += 1
          true
        // next row in next block
        } else {
          current_block += 1
          current_row = 0
          converter.freeMemory(sparkRowInfo.memoryAddress, sparkRowInfo.totalSize)
          sparkRowInfo = converter.converColumarToRow(blocks(current_block))
          true
        }
      }

      override def next(): InternalRow = {
        val (offset, length) = (sparkRowInfo.offsets(current_row),
          sparkRowInfo.lengths(current_row))
        row.pointTo(null, sparkRowInfo.memoryAddress + offset, length.toInt)
        row
      }
    }
    // convert broadcasted value to Iterator[InternalRow] with row num.
    (total_rows, iter)
  }
}
