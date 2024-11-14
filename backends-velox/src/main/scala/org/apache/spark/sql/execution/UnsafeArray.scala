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
package org.apache.spark.sql.execution

import org.apache.spark.SparkEnv
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.memory.{
  MemoryConsumer,
  MemoryMode,
  SparkOutOfMemoryError,
  TaskMemoryManager
}
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.unsafe.{Platform, UnsafeAlignedOffset}
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterSpillWriter

import java.util
import java.util.LinkedList

class UnsafeArray(taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager, MemoryMode.OFF_HEAP) {

  protected var page: MemoryBlock = null
  acquirePage(taskMemoryManager.pageSizeBytes)
  protected var base: AnyRef = page.getBaseObject
  protected var pageCursor = 0
  private var keyOffsets: Array[Long] = null
  protected var numRows = 0

  def iterator() {}

  private def acquirePage(requiredSize: Long): Boolean = {
    try page = allocatePage(requiredSize)

    catch {
      case SparkOutOfMemoryError =>
        return false
    }
    base = page.getBaseObject
    pageCursor = 0
    true
  }

  def get(rowId: Int): UnsafeArrayData = {
    val offset = keyOffsets(rowId)
    val klen = UnsafeAlignedOffset.getSize(base, offset - UnsafeAlignedOffset.getUaoSize)
    val result = new UnsafeArrayData
    result.pointTo (base, offset, klen)
    result
  }

  def getLength(): Int = {
    numRows
  }

  def write(bytes: Array[Byte], inputOffset: Long, inputLength: Int): Unit = {
    var offset: Long = page.getBaseOffset + pageCursor
    val recordOffset = offset

    val uaoSize = UnsafeAlignedOffset.getUaoSize

    val recordLength = 2L * uaoSize + inputLength + 8L

    UnsafeAlignedOffset.putSize(base, offset, inputLength + uaoSize)
    offset += 2L * uaoSize
    Platform.copyMemory(bytes, inputOffset, base, offset, inputLength)
    Platform.putLong(base, offset, 0)

    pageCursor += recordLength
    keyOffsets(numRows) = recordOffset + 2L * uaoSize;
    numRows += 1
  }

  override def spill(numBytes: Long, memoryConsumer: MemoryConsumer): Long = {
    // should we fail, or write to disk? probably the latter.
    // TODO handle multiple data pages
    // write to disk
    var spillBase = page.getBaseObject
    var spillOffset = page.getBaseOffset
    var numRecords: Int = UnsafeAlignedOffset.getSize(spillBase, spillOffset) // probably wrong
    val writeMetrics = new ShuffleWriteMetrics
    val spillWriter = new UnsafeSorterSpillWriter(
      if (SparkEnv.get != null) SparkEnv.get.blockManager else null,
      32 * 1024,
      writeMetrics,
      numRecords
    )
    val uaoSize: Int = UnsafeAlignedOffset.getUaoSize
    while (numRecords > 0) {
      val length = UnsafeAlignedOffset.getSize(spillBase, spillOffset)
      spillWriter.write(spillBase, spillOffset + uaoSize, length, 0)
      spillOffset += uaoSize + length + 8 // why 8? OBO?
      numRecords -= 1
    }
    spillWriter.close()

    // memory stuff
    // free a whole page, but right now we only have a single page. TODO

    0L // TODO return released size
  }


}
