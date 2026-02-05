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

import java.io.{File, InputStream, IOException, RandomAccessFile}
import java.nio.ByteBuffer

/**
 * An InputStream that reads a list of non-contiguous file segments sequentially.
 *
 * @param file
 *   the file to read
 * @param segments
 *   a list of (file_offset, size) segments to read in sequence
 */
class FileSegmentsInputStream(file: File, segments: Seq[(Long, Long)]) extends InputStream {
  private val raf = new RandomAccessFile(file, "r")
  private val channel = raf.getChannel

  private var currentIndex = 0
  private var currentOffset = 0L
  private var remainingInSegment = 0L
  private var closed = false

  if (segments.nonEmpty) {
    currentOffset = segments.head._1
    remainingInSegment = segments.head._2
    channel.position(currentOffset)
  }

  override def read(): Int = {
    val buf = new Array[Byte](1)
    val n = read(buf, 0, 1)
    if (n <= 0) {
      -1
    } else {
      buf(0) & 0xff
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (closed) {
      throw new IOException("Stream is closed")
    }
    if (b == null) {
      throw new NullPointerException("buffer is null")
    }
    if (off < 0 || len < 0 || off + len > b.length) {
      throw new IndexOutOfBoundsException(s"offset=$off, length=$len, buffer length=${b.length}")
    }
    if (len == 0) {
      return 0
    }

    var totalRead = 0
    var remaining = len
    while (remaining > 0 && currentIndex < segments.length) {
      if (remainingInSegment == 0) {
        if (!advanceSegment()) {
          if (totalRead == 0) return -1
          return totalRead
        }
      }
      val bytesToRead = Math.min(remainingInSegment, remaining.toLong).toInt
      val buf = ByteBuffer.wrap(b, off + totalRead, bytesToRead)
      val n = channel.read(buf)
      if (n == -1) {
        if (totalRead == 0) return -1
        return totalRead
      }
      remainingInSegment -= n
      totalRead += n
      remaining -= n
    }

    if (totalRead == 0 && currentIndex >= segments.length) {
      -1
    } else {
      totalRead
    }
  }

  override def skip(n: Long): Long = {
    if (closed) {
      throw new IOException("Stream is closed")
    }
    if (n <= 0) {
      return 0L
    }
    var remaining = n
    var skipped = 0L
    while (remaining > 0 && currentIndex < segments.length) {
      if (remainingInSegment == 0 && !advanceSegment()) {
        return skipped
      }
      val toSkip = Math.min(remainingInSegment, remaining)
      channel.position(channel.position() + toSkip)
      remainingInSegment -= toSkip
      remaining -= toSkip
      skipped += toSkip
    }
    skipped
  }

  override def available(): Int = {
    if (remainingInSegment > Int.MaxValue) Int.MaxValue else remainingInSegment.toInt
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      channel.close()
      raf.close()
    }
  }

  private def advanceSegment(): Boolean = {
    currentIndex += 1
    if (currentIndex >= segments.length) {
      return false
    }
    val (offset, length) = segments(currentIndex)
    currentOffset = offset
    remainingInSegment = length
    channel.position(currentOffset)
    true
  }
}
