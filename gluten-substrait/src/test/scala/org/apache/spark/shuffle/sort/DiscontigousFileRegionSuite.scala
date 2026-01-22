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

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayOutputStream, File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, WritableByteChannel}
import java.nio.charset.StandardCharsets

// The Helper Class (Mock Socket)
class ByteArrayWritableChannel extends WritableByteChannel {
  private val out = new ByteArrayOutputStream()
  private var open = true

  def toByteArray: Array[Byte] = out.toByteArray

  override def write(src: ByteBuffer): Int = {
    val size = src.remaining()
    val buf = new Array[Byte](size)
    src.get(buf)
    out.write(buf)
    size
  }

  override def isOpen: Boolean = open
  override def close(): Unit = { open = false }
}

class DiscontiguousFileRegionSuite
  extends AnyFunSuite
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  var tempFile: File = _
  var raf: RandomAccessFile = _
  var fileChannel: FileChannel = _

  val fileContent = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

  override def beforeAll(): Unit = {
    tempFile = File.createTempFile("netty-test", ".dat")
    val fos = new FileOutputStream(tempFile)
    fos.write(fileContent.getBytes(StandardCharsets.UTF_8))
    fos.close()
  }

  override def beforeEach(): Unit = {
    raf = new RandomAccessFile(tempFile, "r")
    fileChannel = raf.getChannel
  }

  override def afterEach(): Unit = {
    if (fileChannel != null && fileChannel.isOpen) fileChannel.close()
    if (raf != null) raf.close()
  }

  override def afterAll(): Unit = {
    if (tempFile != null) tempFile.delete()
  }

  // --- TESTS ---
  test("transfer a single segment correctly") {
    // Segment: "BCD" (Offset 1, Length 3)
    val region = new DiscontiguousFileRegion(fileChannel, Seq((1L, 3L)))
    val target = new ByteArrayWritableChannel()

    val written = region.transferTo(target, 0)

    assert(written == 3, "Should have written exactly 3 bytes")
    assert(new String(target.toByteArray) == "BCD", "Content should match the first 3 letters")
  }

  test("concatenate multiple discontiguous segments") {
    // Segment 1: "AB" (0, 2)
    // Segment 2: "YZ" (24, 2)
    val region = new DiscontiguousFileRegion(fileChannel, Seq((0L, 2L), (24L, 2L)))
    val target = new ByteArrayWritableChannel()

    val written = region.transferTo(target, 0)

    assert(written == 4)
    assert(new String(target.toByteArray) == "ABYZ")
  }

  test("handle the 'position' parameter correctly (Mid-segment start)") {
    // Combined View: "ABC" + "XYZ" = "ABCXYZ"
    // Indices:       012     345
    val region = new DiscontiguousFileRegion(fileChannel, Seq((0L, 3L), (23L, 3L)))
    val target = new ByteArrayWritableChannel()

    // Act: Start from logical position 2 (Letter 'C')
    // Expectation: Should read 'C' (from seg1) then "XYZ" (from seg2)
    val written = region.transferTo(target, 2)

    assert(written == 4)
    assert(new String(target.toByteArray) == "CXYZ")
  }

  test("handle starting exactly at the boundary of the second segment") {
    // Combined View: "AB" (size 2) + "YZ" (size 2)
    val region = new DiscontiguousFileRegion(fileChannel, Seq((0L, 2L), (24L, 2L)))
    val target = new ByteArrayWritableChannel()

    // Act: Start from logical position 2 (Start of second segment)
    val written = region.transferTo(target, 2)

    assert(written == 2)
    assert(new String(target.toByteArray) == "YZ")
  }

  test("handle a position that skips multiple initial segments") {
    // Segments: "A", "B", "C", "D"
    val region = new DiscontiguousFileRegion(
      fileChannel,
      Seq(
        (0L, 1L),
        (1L, 1L),
        (2L, 1L),
        (3L, 1L)
      ))
    val target = new ByteArrayWritableChannel()

    // Act: Start at logical pos 3 (Should be only "D")
    val written = region.transferTo(target, 3)

    assert(written == 1)
    assert(new String(target.toByteArray) == "D")
  }

  test("return 0 if position is beyond total size") {
    val region = new DiscontiguousFileRegion(fileChannel, Seq((0L, 5L)))
    val target = new ByteArrayWritableChannel()

    val written = region.transferTo(target, 100)

    assert(written == 0)
    assert(target.toByteArray.length == 0)
  }

  test("file channel should closed after release") {
    val region = new DiscontiguousFileRegion(fileChannel, Seq((0L, 5L)))
    region.release()
    assert(!fileChannel.isOpen)
  }
}
