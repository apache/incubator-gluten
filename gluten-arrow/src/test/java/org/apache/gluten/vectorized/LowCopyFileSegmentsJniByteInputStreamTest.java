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
package org.apache.gluten.vectorized;

import io.netty.util.internal.PlatformDependent;
import org.apache.spark.shuffle.sort.FileSegmentsInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class LowCopyFileSegmentsJniByteInputStreamTest {
  private File tempFile;

  @Before
  public void setUp() throws Exception {
    tempFile = Files.createTempFile("gluten-segments", ".bin").toFile();
  }

  @After
  public void tearDown() throws Exception {
    if (tempFile != null && tempFile.exists()) {
      Files.deleteIfExists(tempFile.toPath());
    }
  }

  @Test
  public void testReadAcrossSegments() throws Exception {
    byte[] bytes = "abcdefg-0123456".getBytes(StandardCharsets.UTF_8);
    try (FileOutputStream out = new FileOutputStream(tempFile)) {
      out.write(bytes);
    }

    int firstLen = 4;
    int secondLen = bytes.length - firstLen;
    Seq<Tuple2<Object, Object>> segments =
        toScalaSeq(
            Arrays.asList(
                new Tuple2<>(0L, (long) firstLen),
                new Tuple2<>((long) firstLen, (long) secondLen)));

    FileSegmentsInputStream segmentStream = new FileSegmentsInputStream(tempFile, segments);
    Assert.assertTrue(LowCopyFileSegmentsJniByteInputStream.isSupported(segmentStream));

    LowCopyFileSegmentsJniByteInputStream in =
        new LowCopyFileSegmentsJniByteInputStream(segmentStream);
    ByteBuffer buffer = PlatformDependent.allocateDirectNoCleaner(bytes.length);
    long addr = PlatformDependent.directBufferAddress(buffer);

    long firstRead = in.read(addr, 3);
    long secondRead = in.read(addr + firstRead, bytes.length - firstRead);
    long totalRead = firstRead + secondRead;

    Assert.assertEquals(bytes.length, totalRead);
    Assert.assertEquals(bytes.length, in.tell());

    buffer.limit(bytes.length);
    byte[] out = new byte[bytes.length];
    buffer.get(out);
    Assert.assertArrayEquals(bytes, out);

    in.close();
  }

  @Test
  public void testReadNonContiguousSegments() throws Exception {
    byte[] bytes = "abcdefghij123456789".getBytes(StandardCharsets.UTF_8);
    try (FileOutputStream out = new FileOutputStream(tempFile)) {
      out.write(bytes);
    }

    // Select two non-contiguous segments: [2,5) and [10,15)
    Seq<Tuple2<Object, Object>> segments =
        toScalaSeq(Arrays.asList(new Tuple2<>(2L, 3L), new Tuple2<>(10L, 5L)));

    FileSegmentsInputStream segmentStream = new FileSegmentsInputStream(tempFile, segments);
    Assert.assertTrue(LowCopyFileSegmentsJniByteInputStream.isSupported(segmentStream));

    LowCopyFileSegmentsJniByteInputStream in =
        new LowCopyFileSegmentsJniByteInputStream(segmentStream);
    ByteBuffer buffer = PlatformDependent.allocateDirectNoCleaner(8);
    long addr = PlatformDependent.directBufferAddress(buffer);

    long read = in.read(addr, 8);
    Assert.assertEquals(8, read);
    Assert.assertEquals(8, in.tell());

    buffer.limit(8);
    byte[] out = new byte[8];
    buffer.get(out);
    // Expected: "cde12345"
    Assert.assertArrayEquals("cde12345".getBytes(StandardCharsets.UTF_8), out);

    in.close();
  }

  private static Seq<Tuple2<Object, Object>> toScalaSeq(
      java.util.List<Tuple2<Object, Object>> segments) {
    return JavaConverters.asScalaBuffer(segments).toSeq();
  }
}
