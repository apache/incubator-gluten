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

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

class ColumnarIndexShuffleBlockResolverSuite
  extends AnyFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private var tmpFile: File = _
  private var channel: FileChannel = _

  override def beforeAll(): Unit = {
    tmpFile = File.createTempFile("index", ".bin")
    val fc = FileChannel.open(tmpFile.toPath, StandardOpenOption.WRITE, StandardOpenOption.READ)
    // Partition index: 4 partitions, so 5 offsets
    // Partition 0: 0 segments
    // Partition 1: 1 segment
    // Partition 2: 2 segments
    // Partition 3: 3 segments
    // Offsets: [indexEnd, seg1Start, seg2Start, seg3Start, segEnd]
    // Let's say index is 5*8 = 40 bytes, segments start at 40
    val segOffsets = Array(40L, 40L, 56L, 88L, 136L)
    val indexBuf = ByteBuffer.allocate(5 * 8)
    segOffsets.foreach(indexBuf.putLong)
    indexBuf.flip()
    fc.write(indexBuf)
    // Segment data: total 6 segments (1+2+3)
    val segBuf = ByteBuffer.allocate(6 * 16)
    // Define a data file with 200 bytes.
    // Partition 1:
    segBuf.putLong(0L); segBuf.putLong(10L)
    // Partition 2:
    segBuf.putLong(10L); segBuf.putLong(20L)
    segBuf.putLong(100L); segBuf.putLong(50L)
    // Partition 3:
    segBuf.putLong(30L); segBuf.putLong(70L)
    segBuf.putLong(150L); segBuf.putLong(50L)
    segBuf.putLong(200L); segBuf.putLong(0L)
    segBuf.flip()
    fc.write(segBuf)
    // Add one extra byte to mark new format
    fc.write(ByteBuffer.wrap(Array(1.toByte)))
    fc.close()
  }

  override def beforeEach(): Unit = {
    channel = FileChannel.open(tmpFile.toPath, StandardOpenOption.READ)
  }

  override def afterEach(): Unit = {
    if (channel != null) {
      channel.close()
      channel = null
    }
  }

  override def afterAll(): Unit = {
    if (tmpFile != null) tmpFile.delete()
  }

  test("getSegmentsFromIndex returns correct segments for complex index file") {
    val resolver = new ColumnarIndexShuffleBlockResolver(new SparkConf())
    val method = classOf[ColumnarIndexShuffleBlockResolver].getDeclaredMethod(
      "getSegmentsFromIndex",
      classOf[java.nio.channels.SeekableByteChannel],
      classOf[Int],
      classOf[Int])
    method.setAccessible(true)
    // Partition 0: 0 segments
    val segs0 =
      method.invoke(resolver, channel, Int.box(0), Int.box(1)).asInstanceOf[Seq[(Long, Long)]]
    assert(segs0.isEmpty)
    // Partition 1 - 2: 3 segment
    val segs1 =
      method.invoke(resolver, channel, Int.box(1), Int.box(3)).asInstanceOf[Seq[(Long, Long)]]
    assert(segs1 == Seq((0L, 10L), (10L, 20L), (100L, 50L)))
    // Partition 3: 2 segments, empty segment will be filter out
    val segs3 =
      method.invoke(resolver, channel, Int.box(3), Int.box(4)).asInstanceOf[Seq[(Long, Long)]]
    assert(segs3 == Seq((30L, 70L), (150L, 50L)))
  }
}
