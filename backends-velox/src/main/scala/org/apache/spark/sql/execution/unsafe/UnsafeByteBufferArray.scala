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
package org.apache.spark.sql.execution.unsafe

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.memory.GlobalOffHeapMemory
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryAllocator

/**
 * Used to store broadcast variable off-heap memory for broadcast variable. The underlying data
 * structure is a LongArray allocated in off-heap memory.
 *
 * @param arraySize
 *   underlying array[array[byte]]'s length
 * @param byteBufferLengths
 *   underlying array[array[byte]] per byteBuffer length
 * @param totalBytes
 *   all byteBuffer's length plus together
 */
// scalastyle:off no.finalize
@Experimental
case class UnsafeByteBufferArray(arraySize: Int, byteBufferLengths: Array[Int], totalBytes: Long)
  extends Logging {
  {
    assert(
      arraySize == byteBufferLengths.length,
      "Unsafe buffer array size " +
        "not equal to buffer lengths!")
    assert(totalBytes >= 0, "Unsafe buffer array total bytes can't be negative!")
  }
  private val allocatedBytes = (totalBytes + 7) / 8 * 8

  /**
   * A single array to store all byteBufferArray's value, it's inited once when first time get
   * accessed.
   */
  private var longArray: LongArray = _

  /** Index the start of each byteBuffer's offset to underlying LongArray's initial position. */
  private val byteBufferOffset = if (byteBufferLengths.isEmpty) {
    new Array(0)
  } else {
    byteBufferLengths.init.scanLeft(0L)(_ + _)
  }

  /**
   * Put byteBuffer at specified array index.
   *
   * @param index
   *   index of the array.
   * @param byteBuffer
   *   byteBuffer to put.
   */
  def putByteBuffer(index: Int, byteBuffer: Array[Byte]): Unit = this.synchronized {
    assert(index < arraySize)
    assert(byteBuffer.length == byteBufferLengths(index))
    // first to allocate underlying long array
    if (null == longArray && index == 0) {
      GlobalOffHeapMemory.acquire(allocatedBytes)
      longArray = new LongArray(MemoryAllocator.UNSAFE.allocate(allocatedBytes))
    }

    Platform.copyMemory(
      byteBuffer,
      Platform.BYTE_ARRAY_OFFSET,
      longArray.getBaseObject,
      longArray.getBaseOffset + byteBufferOffset(index),
      byteBufferLengths(index))
  }

  /**
   * Get byteBuffer at specified index.
   * @param index
   * @return
   */
  def getByteBuffer(index: Int): Array[Byte] = {
    assert(index < arraySize)
    if (null == longArray) {
      return new Array[Byte](0)
    }
    val bytes = new Array[Byte](byteBufferLengths(index))
    Platform.copyMemory(
      longArray.getBaseObject,
      longArray.getBaseOffset + byteBufferOffset(index),
      bytes,
      Platform.BYTE_ARRAY_OFFSET,
      byteBufferLengths(index))
    bytes
  }

  /**
   * Get the byteBuffer memory address and length at specified index, usually used when read memory
   * direct from offheap.
   *
   * @param index
   * @return
   */
  def getByteBufferOffsetAndLength(index: Int): (Long, Int) = {
    assert(index < arraySize)
    assert(longArray != null, "The broadcast data in offheap should not be null!")
    val offset = longArray.getBaseOffset + byteBufferOffset(index)
    val length = byteBufferLengths(index)
    (offset, length)
  }

  /**
   * It's needed once the broadcast variable is garbage collected. Since now, we don't have an
   * elegant way to free the underlying memory in offheap.
   */
  override def finalize(): Unit = {
    try {
      if (longArray != null) {
        longArray = null
        GlobalOffHeapMemory.release(allocatedBytes)
      }
    } finally {
      super.finalize()
    }
  }
}
// scalastyle:on no.finalize
