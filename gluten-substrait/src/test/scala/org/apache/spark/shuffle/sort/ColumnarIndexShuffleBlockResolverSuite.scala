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
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage._
import org.apache.spark.util.Utils

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
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

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _

  private val conf: SparkConf = new SparkConf(loadDefaults = false)
  private var tempDir: File = _
  private val appId = "TESTAPP"

  private var indexFile: File = _
  private var dataFile: File = _
  private var resolver: ColumnarIndexShuffleBlockResolver = _

  override def beforeAll(): Unit = {
    indexFile = File.createTempFile("index", ".bin")
    val fc = FileChannel.open(indexFile.toPath, StandardOpenOption.WRITE, StandardOpenOption.READ)
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

    // data file with 200 bytes, 0-199
    dataFile = File.createTempFile("data", ".bin")
    val out = new java.io.FileOutputStream(dataFile)
    out.write((0 until 200).map(_.toByte).toArray)
    out.close()
  }

  override def beforeEach(): Unit = {
    tempDir = Utils.createTempDir()
    MockitoAnnotations.initMocks(this)

    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(diskBlockManager.getFile(any[BlockId])).thenAnswer(
      (invocation: InvocationOnMock) => new File(tempDir, invocation.getArguments.head.toString))
    when(diskBlockManager.getFile(any[String])).thenAnswer(
      (invocation: InvocationOnMock) => new File(tempDir, invocation.getArguments.head.toString))
    when(diskBlockManager.getMergedShuffleFile(any[BlockId], any[Option[Array[String]]]))
      .thenAnswer(
        (invocation: InvocationOnMock) => new File(tempDir, invocation.getArguments.head.toString))
    when(diskBlockManager.localDirs).thenReturn(Array(tempDir))
    when(diskBlockManager.createTempFileWith(any(classOf[File])))
      .thenAnswer {
        invocationOnMock =>
          val file = invocationOnMock.getArguments()(0).asInstanceOf[File]
          Utils.tempFileWith(file)
      }
    conf.set("spark.app.id", appId)

    resolver = new ColumnarIndexShuffleBlockResolver(conf, blockManager)
  }

  override def afterEach(): Unit = {
    resolver = null
    Utils.deleteRecursively(tempDir)
  }

  override def afterAll(): Unit = {
    if (indexFile != null) indexFile.delete()
    if (dataFile != null) dataFile.delete()
  }

  test("getSegmentsFromIndex returns correct segments for complex index file") {
    val channel = FileChannel.open(indexFile.toPath, StandardOpenOption.READ)
    try {
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
      val segs1_2 =
        method.invoke(resolver, channel, Int.box(1), Int.box(3)).asInstanceOf[Seq[(Long, Long)]]
      assert(segs1_2 == Seq((0L, 10L), (10L, 20L), (100L, 50L)))
      // Partition 3: 2 segments, empty segment will be filter out
      val segs3 =
        method.invoke(resolver, channel, Int.box(3), Int.box(4)).asInstanceOf[Seq[(Long, Long)]]
      assert(segs3 == Seq((30L, 70L), (150L, 50L)))
    } finally {
      channel.close()
    }
  }

  test("getBlockData returns correct data for partition segments") {
    val shuffleId = 1
    val mapId = 0L
    val partitionId = 2
    val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
    // commit index and data files
    resolver.writeIndexFileAndCommit(shuffleId, mapId, dataFile, indexFile, numPartitions = 4)

    // getBlockData should return a ManagedBuffer for the requested block
    val buffer = resolver.getBlockData(blockId)
    val bytes = readManagedBuffer(buffer)
    // Partition 2 segments: (10,20), (100,50)
    assert(bytes.length == 70, "Expected 70 bytes, got ${bytes.length}")
    val expected = ((10 until 30) ++ (100 until 150)).map(_.toByte).toArray
    assert(
      bytes.sameElements(expected),
      s"Expected ${expected.mkString(",")}, got ${bytes.mkString(",")}")
  }

  private def readManagedBuffer(buffer: ManagedBuffer): Array[Byte] = {
    val in = buffer.createInputStream()
    val out = new scala.collection.mutable.ArrayBuffer[Byte]()
    val buf = new Array[Byte](4096)
    var n = in.read(buf)
    while (n != -1) {
      out ++= buf.take(n)
      n = in.read(buf)
    }
    in.close()
    out.toArray
  }

  test("canUseNewFormat returns correct value based on config") {
    val conf1 = new SparkConf()
      .set("spark.shuffle.push.based.enabled", "false")
      .set("spark.shuffle.service.enabled", "false")
    val resolver1 = new ColumnarIndexShuffleBlockResolver(conf1)
    assert(resolver1.canUseNewFormat(), "Should use new format when both configs are false")

    val conf2 = new SparkConf()
      .set("spark.shuffle.push.based.enabled", "true")
      .set("spark.shuffle.service.enabled", "false")
    val resolver2 = new ColumnarIndexShuffleBlockResolver(conf2)
    assert(
      !resolver2.canUseNewFormat(),
      "Should not use new format when push-based shuffle is enabled")

    val conf3 = new SparkConf()
      .set("spark.shuffle.push.based.enabled", "false")
      .set("spark.shuffle.service.enabled", "true")
    val resolver3 = new ColumnarIndexShuffleBlockResolver(conf3)
    assert(
      !resolver3.canUseNewFormat(),
      "Should not use new format when shuffle service is enabled")

    val conf4 = new SparkConf()
      .set("spark.shuffle.push.based.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")
    val resolver4 = new ColumnarIndexShuffleBlockResolver(conf4)
    assert(!resolver4.canUseNewFormat(), "Should not use new format when both are enabled")
  }
}
