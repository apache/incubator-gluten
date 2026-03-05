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
import org.apache.spark.SparkException
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle._
import org.apache.spark.storage._

import java.io.DataInputStream
import java.io.File
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.channels.SeekableByteChannel
import java.nio.file.StandardOpenOption

class ColumnarIndexShuffleBlockResolver(
    conf: SparkConf,
    private var blockManager: BlockManager = null)
  extends IndexShuffleBlockResolver(conf, blockManager) {

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
  private val SHUFFLE_SERVICE_ENABLED = "spark.shuffle.service.enabled"
  private val PUSH_BASED_SHUFFLE_ENABLED = "spark.shuffle.push.based.enabled"
  private var newFormatEnabled: Boolean = _

  // When external shuffle service or push-based shuffle is enabled,
  // it may directly access the shuffle files without going through this resolver.
  // we cannot use the new index format to maintain backward compatibility.
  newFormatEnabled = !(conf.getBoolean(PUSH_BASED_SHUFFLE_ENABLED, false) ||
    conf.getBoolean(SHUFFLE_SERVICE_ENABLED, false))

  def canUseNewFormat(): Boolean = newFormatEnabled

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
    // To support a partition index with multiple segments,
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

  private def checkIndexAndDataFile(indexFile: File, dataFile: File, numPartitions: Int): Unit = {
    if (!indexFile.exists()) {
      throw new IllegalStateException(s"Index file $indexFile does not exist")
    }
    if (!dataFile.exists()) {
      throw new IllegalStateException(s"Data file $dataFile does not exist")
    }
    if (!isNewFormat(indexFile)) {
      throw new IllegalStateException(s"Index file $indexFile is not in the new format")
    }

    var index = FileChannel.open(indexFile.toPath, StandardOpenOption.READ)
    var dataFileSize = dataFile.length()
    try {
      for (i <- 0 until numPartitions) {
        var segments = getSegmentsFromIndex(index, startId = i, endId = i + 1)
        segments.foreach {
          case (offset, size) =>
            if (offset < 0 || size < 0 || offset + size > dataFileSize) {
              throw new IllegalStateException(
                s"Index file $indexFile has invalid segment ($offset, $size) " +
                  s"for partition $i in data file of size $dataFileSize")
            }
        }
      }
    } finally {
      index.close()
    }
  }

  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Long,
      dataTmp: File,
      indexTmp: File,
      numPartitions: Int,
      checksums: Option[Array[Long]] = None
  ): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val dataFile = getDataFile(shuffleId, mapId)
    this.synchronized {
      checkIndexAndDataFile(indexTmp, dataTmp, numPartitions)
      dataTmp.renameTo(dataFile)
      indexTmp.renameTo(indexFile)
    }
  }

  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw SparkException.internalError(
          s"unexpected shuffle block id format: $blockId",
          category = "SHUFFLE")
    }
    val indexFile = getIndexFile(shuffleId, mapId, dirs)

    if (!isNewFormat(indexFile)) {
      // fallback to old implementation
      return super.getBlockData(blockId, dirs)
    }

    var index = FileChannel.open(indexFile.toPath, StandardOpenOption.READ)
    try {
      val segments = getSegmentsFromIndex(index, startReduceId, endReduceId)
      val dataFile = getDataFile(shuffleId, mapId, dirs)
      new FileSegmentsManagedBuffer(transportConf, dataFile, segments)
    } finally {
      index.close()
    }
  }
}
