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
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.network.util.TransportConf

import java.io.{BufferedInputStream, EOFException, File, FileInputStream, InputStream, IOException, SequenceInputStream}
import java.nio.ByteBuffer
import java.util.Vector

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
    // Implement logic to return a ByteBuffer representing the file segments
    throw new UnsupportedOperationException("Not implemented yet")
  }

  @throws[IOException]
  private def skipFully(in: InputStream, n: Long): Unit = {
    var remaining = n
    val toSkip = n
    while (remaining > 0L) {
      val amt = in.skip(remaining)
      if (amt == 0L) {
        // Try reading a byte to see if we are at EOF
        if (in.read() == -1) {
          val skipped = toSkip - remaining
          throw new EOFException(
            s"reached end of stream after skipping $skipped bytes; $toSkip bytes expected")
        }
        remaining -= 1
      } else {
        remaining -= amt
      }
    }
  }

  @throws[IOException]
  override def createInputStream(): InputStream = {
    val streams = new Vector[InputStream]()
    val filesToClose = new Vector[FileInputStream]()
    var shouldCloseFile = true
    try {
      segments.foreach {
        case (offset, length) =>
          val fis = new FileInputStream(file)
          filesToClose.add(fis)
          skipFully(fis, offset)
          streams.add(new BufferedInputStream(new LimitedInputStream(fis, length)))
      }
      shouldCloseFile = false
      new SequenceInputStream(streams.elements())
    } finally {
      if (shouldCloseFile) {
        val it = filesToClose.elements()
        while (it.hasMoreElements) {
          JavaUtils.closeQuietly(it.nextElement())
        }
      }
    }
  }

  override def convertToNetty(): AnyRef = {
    // Implement logic to convert to Netty buffer if needed
    throw new UnsupportedOperationException("Not implemented yet")
  }
}
