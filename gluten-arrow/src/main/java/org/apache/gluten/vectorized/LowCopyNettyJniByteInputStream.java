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

import org.apache.gluten.exception.GlutenException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This implementation is targeted to optimize against Spark's {@link
 * org.apache.spark.network.buffer.NettyManagedBuffer} to make sure shuffle data is shared over JNI
 * without unnecessary copy.
 */
public class LowCopyNettyJniByteInputStream implements JniByteInputStream {

  // Cache shaded netty ByteBufInputStream_buffer field, <className, field>
  private static final Map<String, Field> BYTE_BUF_INPUT_STREAM_BUFFER_FIELD_MAP =
      new ConcurrentHashMap<>();

  static {
    try {
      Field bufferField = ByteBufInputStream.class.getDeclaredField("buffer");
      bufferField.setAccessible(true);
      BYTE_BUF_INPUT_STREAM_BUFFER_FIELD_MAP.put(ByteBufInputStream.class.getName(), bufferField);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private final InputStream in;

  private ByteBuf byteBuf;

  private int readBytesCount = 0;

  public LowCopyNettyJniByteInputStream(InputStream in) {
    this.in = in; // to prevent underlying netty buffer from being collected by GC
    final InputStream unwrapped = JniByteInputStreams.unwrapSparkInputStream(in);
    this.byteBuf = getBuffer(unwrapped);
  }

  @Override
  public long read(long destAddress, long maxSize) {
    long bytesToRead = Math.min(maxSize, this.byteBuf.readableBytes());
    int bytesToRead32 = Math.toIntExact(bytesToRead);
    if (bytesToRead32 == 0) {
      return 0;
    }
    ByteBuffer direct = PlatformDependent.directBuffer(destAddress, bytesToRead32);
    // read data directly from ByteBuf to native address
    this.byteBuf.readBytes(direct);
    readBytesCount += direct.position();
    return direct.position();
  }

  public static boolean isSupported(InputStream in) {
    if (!isByteBufInputStream(in)) {
      return false;
    }
    ByteBufInputStream bbin = (ByteBufInputStream) in;
    final ByteBuf byteBuf = getBuffer(bbin);
    return byteBuf.isDirect();
  }

  private static final String BYTE_BUF_INPUT_STREAM_CLASSNAME = ByteBufInputStream.class.getName();

  private static boolean isByteBufInputStream(InputStream bbin) {
    String className = bbin.getClass().getName();
    if (BYTE_BUF_INPUT_STREAM_BUFFER_FIELD_MAP.containsKey(className)) {
      return true;
    }
    if (!className.endsWith(BYTE_BUF_INPUT_STREAM_CLASSNAME)) {
      return false;
    }

    try {
      Field field = bbin.getClass().getDeclaredField("buffer");
      field.setAccessible(true);
      BYTE_BUF_INPUT_STREAM_BUFFER_FIELD_MAP.put(className, field);
      return true;
    } catch (NoSuchFieldException e) {
      return false;
    }
  }

  private static ByteBuf getBuffer(Object bbin) {
    String className = bbin.getClass().getName();
    Field field = BYTE_BUF_INPUT_STREAM_BUFFER_FIELD_MAP.get(className);
    if (field == null) {
      throw new GlutenException(
          "Field 'buffer' not cached for shaded netty ByteBufInputStream: " + className);
    }
    try {
      return (ByteBuf) field.get(bbin);
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
  }

  @Override
  public long tell() {
    return readBytesCount;
  }

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException e) {
      throw new GlutenException(e);
    }
  }
}
