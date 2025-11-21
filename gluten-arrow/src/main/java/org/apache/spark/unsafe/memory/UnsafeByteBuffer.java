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
package org.apache.spark.unsafe.memory;

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.task.TaskResources;
import org.apache.spark.unsafe.Platform;

/** The API is for being called from C++ via JNI. */
public class UnsafeByteBuffer {
  private final ArrowBuf buffer;
  private final long size;

  private UnsafeByteBuffer(ArrowBuf buffer, long size) {
    this.buffer = buffer;
    this.size = size;
  }

  public static UnsafeByteBuffer allocate(long size) {
    final BufferAllocator allocator;
    if (TaskResources.inSparkTask()) {
      allocator = ArrowBufferAllocators.contextInstance(UnsafeByteBuffer.class.getName());
    } else {
      allocator = ArrowBufferAllocators.globalInstance();
    }
    final ArrowBuf arrowBuf = allocator.buffer(size);
    return new UnsafeByteBuffer(arrowBuf, size);
  }

  public long address() {
    return buffer.memoryAddress();
  }

  public long size() {
    return size;
  }

  public void release() {
    buffer.close();
  }

  public byte[] toByteArray() {
    final byte[] values = new byte[Math.toIntExact(size)];
    Platform.copyMemory(
        null, buffer.memoryAddress(), values, Platform.BYTE_ARRAY_OFFSET, values.length);
    return values;
  }
}
