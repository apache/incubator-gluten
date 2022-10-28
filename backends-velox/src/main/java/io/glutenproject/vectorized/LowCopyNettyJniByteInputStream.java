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

package io.glutenproject.vectorized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

/**
 * This implementation is targeted to optimize against Spark's
 * {@link org.apache.spark.network.buffer.NettyManagedBuffer} to make sure shuffle data is shared
 * over JNI without unnecessary copy.
 */
public class LowCopyNettyJniByteInputStream implements JniByteInputStream {

  private static final Field FIELD_ByteBufInputStream_buffer;

  static {
    try {
      FIELD_ByteBufInputStream_buffer = ByteBufInputStream.class.getDeclaredField("buffer");
      FIELD_ByteBufInputStream_buffer.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private final InputStream in;
  private final long baseAddress;
  private final int initReaderIndex;
  private int readerIndex;
  private int readableBytes;


  public LowCopyNettyJniByteInputStream(InputStream in) {
    this.in = in; // to prevent underlying netty buffer from being collected by GC
    final InputStream unwrapped = JniInputStreams.unwrapSparkInputStream(in);
    try {
      final ByteBuf byteBuf = (ByteBuf) FIELD_ByteBufInputStream_buffer.get(unwrapped);
      baseAddress = byteBuf.memoryAddress();
      initReaderIndex = byteBuf.readerIndex();
      readerIndex = initReaderIndex;
      readableBytes = byteBuf.readableBytes();
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long read(long destAddress, long maxSize) {
    long bytesToRead = Math.min(maxSize, readableBytes);
    if (bytesToRead == 0) {
      return 0;
    }
    memCopy(baseAddress + readerIndex, destAddress, bytesToRead);
    readerIndex += bytesToRead;
    readableBytes -= bytesToRead;
    return bytesToRead;
  }

  public native void memCopy(long srcAddress, long destAddress, long size);

  public static boolean isSupported(InputStream in) {
    if (!(in instanceof ByteBufInputStream)) {
      return false;
    }
    ByteBufInputStream bbin = (ByteBufInputStream) in;
    try {
      final ByteBuf byteBuf = (ByteBuf) FIELD_ByteBufInputStream_buffer.get(bbin);
      if (!byteBuf.hasMemoryAddress()) {
        return false;
      }
      return true;
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long tell() {
    return readerIndex - initReaderIndex;
  }

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
