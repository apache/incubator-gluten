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

import _root_.io.netty.util.internal.PlatformDependent
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, FileOutputStream}
import java.nio.file.Files

class FileSegmentsInputStreamSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var tempFile: File = _
  private val fileData: Array[Byte] = (0 until 100).map(_.toByte).toArray

  override def beforeAll(): Unit = {
    tempFile = Files.createTempFile("fsegments-inputstream", ".bin").toFile
    val fos = new FileOutputStream(tempFile)
    fos.write(fileData)
    fos.close()
  }

  override def afterAll(): Unit = {
    if (tempFile != null && tempFile.exists()) tempFile.delete()
  }

  test("read single segment") {
    val segments = Seq((10L, 5L))
    val in = new FileSegmentsInputStream(tempFile, segments)
    val out = readAll(in, 5)
    assert(out.sameElements(fileData.slice(10, 15)))
    assert(in.read() == -1)
    in.close()
  }

  test("read multiple non-contiguous segments") {
    val segments = Seq((2L, 3L), (10L, 4L), (50L, 2L))
    val in = new FileSegmentsInputStream(tempFile, segments)
    val out = readAll(in, 9)
    val expected = fileData.slice(2, 5) ++ fileData.slice(10, 14) ++ fileData.slice(50, 52)
    assert(out.sameElements(expected))
    assert(in.read() == -1)
    in.close()
  }

  test("read single bytes sequentially") {
    val segments = Seq((20L, 3L))
    val in = new FileSegmentsInputStream(tempFile, segments)
    val b0 = in.read()
    val b1 = in.read()
    val b2 = in.read()
    val b3 = in.read()
    assert(b0 == (fileData(20) & 0xff))
    assert(b1 == (fileData(21) & 0xff))
    assert(b2 == (fileData(22) & 0xff))
    assert(b3 == -1)
    in.close()
  }

  test("skip bytes across segments") {
    val segments = Seq((10L, 3L), (20L, 4L))
    val in = new FileSegmentsInputStream(tempFile, segments)
    // Skip 2 bytes in the first segment
    val skipped1 = in.skip(2)
    assert(skipped1 == 2)
    // Should now be at fileData(12)
    val b = in.read()
    assert(b == (fileData(12) & 0xff))
    // Skip 2 bytes (should move to the second segment)
    val skipped2 = in.skip(2)
    assert(skipped2 == 2)
    // Should now be at fileData(22)
    val b2 = in.read()
    assert(b2 == (fileData(22) & 0xff))
    // Skip more than remaining (should reach end)
    val skipped3 = in.skip(100)
    assert(skipped3 == 1) // 1 byte left in segments
    assert(in.read() == -1)
    in.close()
  }

  test("read into native memory") {
    val segments = Seq((2L, 3L), (10L, 4L))
    val in = new FileSegmentsInputStream(tempFile, segments)
    val expected = fileData.slice(2, 5) ++ fileData.slice(10, 14)

    val buffer = java.nio.ByteBuffer.allocateDirect(expected.length)
    val addr = PlatformDependent.directBufferAddress(buffer)
    val read = in.read(addr, expected.length)
    assert(read == expected.length)

    buffer.limit(expected.length)
    val out = new Array[Byte](expected.length)
    buffer.get(out)
    assert(out.sameElements(expected))
    assert(in.read(addr, 1) == 0)
    in.close()
  }

  private def readAll(in: FileSegmentsInputStream, size: Int): Array[Byte] = {
    val buffer = new Array[Byte](size)
    var total = 0
    var n = 0
    while (n >= 0 && total < size) {
      n = in.read(buffer, total, size - total)
      if (n > 0) {
        total += n
      }
    }
    buffer
  }
}
