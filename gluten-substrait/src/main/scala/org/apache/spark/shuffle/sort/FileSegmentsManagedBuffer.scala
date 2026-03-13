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

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.network.util.TransportConf

import java.io.{EOFException, File, InputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/** A {@link ManagedBuffer} backed by a set of segments in a file. */
class FileSegmentsManagedBuffer(
    conf: TransportConf,
    file: File,
    segments: Seq[(Long, Long)]
) extends ManagedBuffer {
  private val totalCount: Long = segments.map(_._2).sum

  override def size(): Long = totalCount

  override def retain(): ManagedBuffer = this
  override def release(): ManagedBuffer = this

  override def nioByteBuffer(): ByteBuffer = {
    val totalSize = size()
    val channel = new RandomAccessFile(file, "r").getChannel
    try {
      if (conf != null && totalSize >= conf.memoryMapBytes() && segments.length == 1) {
        // zero-copy for single segment that is large enough
        val (offset, length) = segments.head
        return channel.map(FileChannel.MapMode.READ_ONLY, offset, length)
      }
      val totalSizeInt = Math.toIntExact(totalSize)
      val buffer = ByteBuffer.allocate(totalSizeInt)
      var destPos = 0
      segments.foreach {
        case (offset, length) =>
          buffer.position(destPos)
          buffer.limit(destPos + length.toInt)
          channel.position(offset)
          var remaining = length
          while (remaining > 0) {
            val n = channel.read(buffer)
            if (n == -1) {
              throw new EOFException(s"EOF reached while reading segment at offset $offset")
            }
            remaining -= n
          }
          destPos += length.toInt
      }
      buffer.flip()
      buffer
    } finally {
      JavaUtils.closeQuietly(channel)
    }
  }

  override def createInputStream(): InputStream = {
    new FileSegmentsInputStream(file, segments)
  }

  override def convertToNetty(): AnyRef = {
    var lazyOpen = false
    if (conf != null) lazyOpen = conf.lazyFileDescriptor()
    new DiscontiguousFileRegion(file, segments, lazyOpen)
  }
}
