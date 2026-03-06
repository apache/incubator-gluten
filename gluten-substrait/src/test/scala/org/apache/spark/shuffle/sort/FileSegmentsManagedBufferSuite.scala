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

import org.apache.spark.network.util.TransportConf

import _root_.io.netty.channel.FileRegion
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io._
import java.nio.channels.WritableByteChannel
import java.nio.file.Files

class FakeTransportConf(private val lazyOpen: Boolean = false) extends TransportConf("test", null) {
  override def memoryMapBytes(): Int = 4096 // 4 KB
  override def lazyFileDescriptor(): Boolean = lazyOpen
}

class FileSegmentsManagedBufferSuite extends AnyFunSuite with BeforeAndAfterAll {
  private class ByteArrayWritableChannel extends WritableByteChannel {
    private val out = new ByteArrayOutputStream()
    private var open = true

    def toByteArray: Array[Byte] = out.toByteArray

    override def write(src: java.nio.ByteBuffer): Int = {
      val size = src.remaining()
      val buf = new Array[Byte](size)
      src.get(buf)
      out.write(buf)
      size
    }

    override def isOpen: Boolean = open
    override def close(): Unit = { open = false }
  }

  private var tempFile: File = _
  private val fileData: Array[Byte] = (0 until 100).map(_.toByte).toArray

  override def beforeAll(): Unit = {
    tempFile = Files.createTempFile("fsegments-test", ".bin").toFile
    val fos = new FileOutputStream(tempFile)
    fos.write(fileData)
    fos.close()
  }

  override def afterAll(): Unit = {
    if (tempFile != null && tempFile.exists()) tempFile.delete()
  }

  test("convertToNetty returns FileRegion with correct count and content") {
    val conf = new FakeTransportConf()
    val segments = Seq((2L, 3L), (10L, 4L))
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    val nettyObj = buf.convertToNetty()
    assert(nettyObj.isInstanceOf[FileRegion])
    val region = nettyObj.asInstanceOf[FileRegion]
    assert(region.count() == 7L)
    assert(region.position() == 0L)
    val target = new ByteArrayWritableChannel()
    val written = region.transferTo(target, 0)
    assert(written == 7L)
    val expected = fileData.slice(2, 5) ++ fileData.slice(10, 14)
    assert(target.toByteArray.sameElements(expected))
  }

  test("convertToNetty supports lazyOpen via FileRegion transfer") {
    val conf = new FakeTransportConf(lazyOpen = true)
    val segments = Seq((5L, 3L))
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    val nettyObj = buf.convertToNetty()
    assert(nettyObj.isInstanceOf[FileRegion])
    val region = nettyObj.asInstanceOf[FileRegion]
    val target = new ByteArrayWritableChannel()
    val written = region.transferTo(target, 0)
    assert(written == 3L)
    assert(target.toByteArray.sameElements(fileData.slice(5, 8)))
  }

  test("size returns sum of segment lengths") {
    val conf = null.asInstanceOf[TransportConf]
    val segments = Seq((2L, 10L), (20L, 5L), (50L, 15L))
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    assert(buf.size() == 10L + 5L + 15L)
  }

  test("empty segments returns zero size") {
    val conf = null.asInstanceOf[TransportConf]
    val segments = Seq.empty[(Long, Long)]
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    assert(buf.size() == 0L)
  }

  test("nioByteBuffer reads single segment correctly") {
    val conf = null.asInstanceOf[TransportConf]
    val segments = Seq((10L, 5L))
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    val bb = buf.nioByteBuffer()
    val arr = new Array[Byte](5)
    bb.get(arr)
    assert(arr.sameElements(fileData.slice(10, 15)))
    assert(!bb.hasRemaining)
  }

  test("nioByteBuffer reads multiple segments in sequence") {
    val conf = null.asInstanceOf[TransportConf]
    val segments = Seq((5L, 3L), (20L, 2L), (40L, 4L))
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    val bb = buf.nioByteBuffer()
    val arr = new Array[Byte](9)
    bb.get(arr)
    assert(arr.slice(0, 3).sameElements(fileData.slice(5, 8)))
    assert(arr.slice(3, 5).sameElements(fileData.slice(20, 22)))
    assert(arr.slice(5, 9).sameElements(fileData.slice(40, 44)))
    assert(!bb.hasRemaining)
  }

  test("nioByteBuffer throws EOFException if segment exceeds file length") {
    val conf = null.asInstanceOf[TransportConf]
    val segments = Seq((95L, 10L)) // goes past EOF
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    val thrown = intercept[EOFException] {
      buf.nioByteBuffer()
    }
    assert(thrown.getMessage.contains("EOF reached while reading segment"))
  }

  test("nioByteBuffer with empty segments returns empty buffer") {
    val conf = null.asInstanceOf[TransportConf]
    val segments = Seq.empty[(Long, Long)]
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    val bb = buf.nioByteBuffer()
    assert(bb.remaining() == 0)
    assert(!bb.hasRemaining)
  }

  test("nioByteBuffer with mmap for a single segment") {
    // Create a large file (16 KB)
    val largeData = (0 until 16384).map(_.toByte).toArray
    val largeFile = Files.createTempFile("fsegments-mmap-test", ".bin").toFile
    val fos = new FileOutputStream(largeFile)
    fos.write(largeData)
    fos.close()

    val conf = new FakeTransportConf()
    val segment = (0L, 16384L)
    val buf = new FileSegmentsManagedBuffer(conf, largeFile, Seq(segment))
    val nioBuf = buf.nioByteBuffer()
    val arr = new Array[Byte](16384)
    nioBuf.get(arr)
    assert(arr.sameElements(largeData))

    largeFile.delete()
  }

  test("createInputStream reads single segment correctly") {
    val conf = null.asInstanceOf[TransportConf] // Not used in this test
    val segments = Seq((10L, 5L))
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    val in = buf.createInputStream()
    val read = new Array[Byte](5)
    assert(in.read(read) == 5)
    assert(read.sameElements(fileData.slice(10, 15)))
    assert(in.read() == -1)
    in.close()
  }

  test("createInputStream reads multiple segments in sequence") {
    val conf = null.asInstanceOf[TransportConf]
    val segments = Seq((5L, 3L), (20L, 2L), (40L, 4L))
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    val in = buf.createInputStream()
    val out = new Array[Byte](9)
    var total = 0
    var n = 0
    while (n > 0 || total == 0) {
      n = in.read(out, total, out.length - total)
      if (n > 0) total += n
      else if (total == 0) n = 1 // ensure loop runs at least once
    }
    assert(total == 9)
    assert(out.slice(0, 3).sameElements(fileData.slice(5, 8)))
    assert(out.slice(3, 5).sameElements(fileData.slice(20, 22)))
    assert(out.slice(5, 9).sameElements(fileData.slice(40, 44)))
    assert(in.read() == -1)
    in.close()
  }

  test("createInputStream read nothing if segment exceeds file length") {
    val conf = null.asInstanceOf[TransportConf]
    val segments = Seq((105L, 10L)) // goes past EOF
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    // should raise EOFException or read nothing
    try {
      val in = buf.createInputStream()
      val read = new Array[Byte](10)
      assert(in.read(read) == -1)
      in.close()
    } catch {
      case e: EOFException =>
        assert(e.getMessage.contains("reached end of stream"))
      case t: Throwable =>
        fail(s"Unexpected exception: $t")
    }
  }

  test("createInputStream with empty segments returns EOF immediately") {
    val conf = null.asInstanceOf[TransportConf]
    val segments = Seq.empty[(Long, Long)]
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    val in = buf.createInputStream()
    assert(in.read() == -1)
    val out = new Array[Byte](8)
    assert(in.read(out) == -1)
    in.close()
  }

  test("convertToNetty with empty segments has zero readable bytes") {
    val conf = new FakeTransportConf()
    val segments = Seq.empty[(Long, Long)]
    val buf = new FileSegmentsManagedBuffer(conf, tempFile, segments)
    val nettyObj = buf.convertToNetty()
    assert(nettyObj.isInstanceOf[FileRegion])
    val region = nettyObj.asInstanceOf[FileRegion]
    assert(region.count() == 0L)
    val target = new ByteArrayWritableChannel()
    assert(region.transferTo(target, 0) == 0L)
    assert(target.toByteArray.isEmpty)
  }
}
