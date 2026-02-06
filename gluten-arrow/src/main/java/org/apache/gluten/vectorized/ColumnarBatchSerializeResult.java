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

import com.google.common.base.Preconditions;
import org.apache.spark.sql.execution.unsafe.JniUnsafeByteBuffer;
import org.apache.spark.sql.execution.unsafe.UnsafeByteArray;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnarBatchSerializeResult implements Serializable {
  public static final ColumnarBatchSerializeResult EMPTY =
      new ColumnarBatchSerializeResult(true, 0, Collections.emptyList());

  private final boolean isOffHeap;
  private final long numRows;
  private final long sizeInBytes;
  private final List<byte[]> onHeapData;
  private final List<UnsafeByteArray> offHeapData;

  public ColumnarBatchSerializeResult(
      boolean isOffHeap, long numRows, List<JniUnsafeByteBuffer> serialized) {
    this.numRows = numRows;
    this.isOffHeap = isOffHeap;
    if (isOffHeap) {
      onHeapData = null;
      offHeapData =
          serialized.stream()
              .map(JniUnsafeByteBuffer::toUnsafeByteArray)
              .collect(Collectors.toList());
      sizeInBytes = offHeapData.stream().mapToInt(unsafe -> Math.toIntExact(unsafe.size())).sum();
    } else {
      onHeapData =
          serialized.stream().map(JniUnsafeByteBuffer::toByteArray).collect(Collectors.toList());
      offHeapData = null;
      sizeInBytes = onHeapData.stream().mapToInt(bytes -> bytes.length).sum();
    }
  }

  public boolean isOffHeap() {
    return isOffHeap;
  }

  public long numRows() {
    return numRows;
  }

  public long sizeInBytes() {
    return sizeInBytes;
  }

  public List<byte[]> onHeapData() {
    Preconditions.checkState(!isOffHeap);
    return onHeapData;
  }

  public List<UnsafeByteArray> offHeapData() {
    Preconditions.checkState(isOffHeap);
    return offHeapData;
  }
}
