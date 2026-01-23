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
package org.apache.spark.shuffle.sort

import org.apache.spark.SparkConf
import org.apache.spark.shuffle._

import java.io.DataInputStream
import java.io.File
import java.nio.channels.Channels
import java.nio.channels.SeekableByteChannel

class ColumnarIndexShuffleBlockResolver(conf: SparkConf) extends IndexShuffleBlockResolver(conf) {
  private def isNewFormat(index: File): Boolean = {
    // Simple heuristic to determine new format by appending 1 extra bytes.
    // Old index file always has length multiple of 8 bytes.
    if (index.length() % 8 == 1) true else false
  }

  private def getSegmentsFromIndex(
      channel: SeekableByteChannel,
      startId: Int,
      endId: Int): Seq[(Long, Long)] = {
    // New Index Format:
    // To support a partiton index with multiple segments,
    // the index file is composed of three parts:
    // 1) Partition Index: index_0, index_1, ... index_N
    //    Each index_i is an 8-byte long integer representing the byte offset in the file
    //    For partition i, its segments are stored between index_i and index_(i+1).
    // 2) Segment Data: [offset_0][size_0][offset_1][size_1]...[offset_N][size_N]
    //    Each segment is represented by two 8-byte long integers: offset and size.
    //    offset is the byte offset in the data file, size is the length in bytes of this segment.
    //    All segments for all partitions are stored sequentially after the partition index.
    // 3) One extra byte at the end to distinguish from old format.
    channel.position(startId * 8L)
    val in = new DataInputStream(Channels.newInputStream(channel))
    var startOffset = in.readLong()
    channel.position(endId * 8L)
    val endOffset = in.readLong()
    if (endOffset < startOffset || (endOffset - startOffset) % 16 != 0) {
      throw new IllegalStateException(
        s"Index file: Invalid index to segments ($startOffset, $endOffset)")
    }
    val segmentCount = (endOffset - startOffset) / 16
    // Read segments
    channel.position(startOffset)
    val segments = for (i <- 0 until segmentCount.toInt) yield {
      val offset = in.readLong()
      val size = in.readLong()
      (offset, size)
    }
    segments.filter(_._2 > 0) // filter out zero-size segments
  }
}
