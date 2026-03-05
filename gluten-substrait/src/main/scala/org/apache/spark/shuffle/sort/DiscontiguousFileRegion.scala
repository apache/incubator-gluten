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

// org.apache.spark.shuffle.sort has io objects conflicting with Netty's io objects.
import _root_.io.netty.channel.FileRegion
import _root_.io.netty.util.AbstractReferenceCounted

import java.io.File
import java.io.IOException
import java.nio.channels.{FileChannel, WritableByteChannel}
import java.nio.file.StandardOpenOption

/**
 * A FileRegion that maps a continuous logical stream (0 to N) onto multiple discontiguous physical
 * file segments.
 */
class DiscontiguousFileRegion(
    private val file: File,
    private val segments: Seq[(Long, Long)], // (Physical File Offset, Length)
    private val lazyOpen: Boolean = false // If true, delay opening the file until first use
) extends AbstractReferenceCounted
  with FileRegion {

  require(segments.nonEmpty, "Must provide at least one segment")

  private val totalCount: Long = segments.map(_._2).sum
  private var bytesTransferred: Long = 0L
  private var fileChannel: FileChannel = null
  private var closed: Boolean = false

  if (!lazyOpen) {
    ensureOpen()
  }

  private def ensureOpen(): Unit = {
    if (closed) {
      throw new IOException("File is already closed")
    }
    if (fileChannel == null) {
      fileChannel = FileChannel.open(file.toPath, StandardOpenOption.READ)
    }
  }

  /**
   * Transfers data starting from the LOGICAL 'position'.
   * @param target
   *   The socket/channel to write to.
   * @param position
   *   The relative position (0 to totalCount) within this specific region where the transfer should
   *   begin.
   */
  override def transferTo(target: WritableByteChannel, position: Long): Long = {
    var logicalPos = position
    var totalWritten = 0L

    ensureOpen()

    if (logicalPos >= totalCount) {
      return 0L
    }

    // 1. Locate the starting segment
    var segmentIndex = 0
    var currentSegmentBase = 0L // Logical start of the current segment

    // Fast-forward to the segment containing 'position'
    while (segmentIndex < segments.length) {
      val (phyOffset, length) = segments(segmentIndex)
      val currentSegmentEnd = currentSegmentBase + length

      if (logicalPos < currentSegmentEnd) {
        // FOUND IT: The request starts inside this segment.

        // 2. Calculate offsets
        val offsetInSegment = logicalPos - currentSegmentBase
        val physicalPos = phyOffset + offsetInSegment
        val remainingInSegment = length - offsetInSegment

        // 3. Perform the transfer (Zero-Copy)
        val written = fileChannel.transferTo(physicalPos, remainingInSegment, target)

        if (written == 0) {
          // Socket buffer full. Return what we have.
          return totalWritten
        } else if (written == -1) {
          throw new IOException("EOF encountered in underlying file")
        }

        // 4. Update state
        bytesTransferred += written
        totalWritten += written
        logicalPos += written

        // Optimization: If we finished this segment exactly, loop immediately to the next
        // so we don't return to the Netty loop just to call us back for the next byte.
        if (written == remainingInSegment) {
          currentSegmentBase += length
          segmentIndex += 1
          // The loop continues to the next segment...
        } else {
          // We wrote only part of the segment (socket likely full). Done for now.
          return totalWritten
        }

      } else {
        // Target position is further down. Skip this segment.
        currentSegmentBase += length
        segmentIndex += 1
      }
    }

    totalWritten
  }

  override def count(): Long = totalCount

  // This returns the position within the REGION, usually 0 for the start of the region object.
  override def position(): Long = 0

  override def transferred(): Long = bytesTransferred
  @deprecated override def transfered(): Long = bytesTransferred

  // --- Reference Counting ---
  override def touch(hint: Any): FileRegion = this
  override def touch(): FileRegion = this
  override def retain(): FileRegion = { super.retain(); this }
  override def retain(increment: Int): FileRegion = { super.retain(increment); this }

  override protected def deallocate(): Unit = {
    if (fileChannel != null && !closed) {
      fileChannel.close()
      fileChannel = null
      closed = true
    }
  }
}
