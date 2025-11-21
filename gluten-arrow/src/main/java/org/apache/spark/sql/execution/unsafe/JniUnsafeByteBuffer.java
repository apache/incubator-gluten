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
package org.apache.spark.sql.execution.unsafe;

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.spark.unsafe.Platform;

/**
 * A temperate unsafe byte buffer implementation that is created and operated from C++ via JNI. The
 * buffer has to be converted either to a Java on-heap byte array or to a Java off-heap unsafe byte
 * array after Java code receives this object.
 */
public class JniUnsafeByteBuffer {
  private ArrowBuf buffer;
  private long size;
  private boolean closed = false;

  private JniUnsafeByteBuffer(ArrowBuf buffer, long size) {
    this.buffer = buffer;
    this.size = size;
  }

  public static JniUnsafeByteBuffer allocate(long size) {
    final ArrowBuf arrowBuf = ArrowBufferAllocators.globalInstance().buffer(size);
    return new JniUnsafeByteBuffer(arrowBuf, size);
  }

  public long address() {
    ensureOpen();
    return buffer.memoryAddress();
  }

  public long size() {
    ensureOpen();
    return size;
  }

  private synchronized void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Already closed");
    }
  }

  private synchronized void release() {
    ensureOpen();
    buffer.close();
  }

  private synchronized void close() {
    ensureOpen();
    closed = true;
    buffer = null;
    size = 0;
  }

  public synchronized byte[] toByteArray() {
    ensureOpen();
    final byte[] values = new byte[Math.toIntExact(size)];
    Platform.copyMemory(
        null, buffer.memoryAddress(), values, Platform.BYTE_ARRAY_OFFSET, values.length);
    release();
    close();
    return values;
  }

  public synchronized UnsafeByteArray toUnsafeByteArray() {
    final UnsafeByteArray out;
    ensureOpen();
    out = new UnsafeByteArray(buffer, size);
    // Do not release the Arrow buffer since we've transferred it to the target unsafe byte array
    // which will manage its life cycle afterward.
    close();
    return out;
  }
}
