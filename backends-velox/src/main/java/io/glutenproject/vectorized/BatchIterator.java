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

import org.apache.arrow.dataset.jni.UnsafeRecordBatchSerializer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.Serializable;

public class BatchIterator extends AbstractBatchIterator {
  private native boolean nativeHasNext(long nativeHandler);
  private native byte[] nativeNext(long nativeHandler);
  private native long nativeCHNext(long nativeHandler);
  private native void nativeClose(long nativeHandler);
  private native MetricsObject nativeFetchMetrics(long nativeHandler);

  public BatchIterator() throws IOException {}

  public BatchIterator(long instance_id) throws IOException {
    super(instance_id);
  }

  @Override
  public boolean hasNextInternal() throws IOException {
    return nativeHasNext(nativeHandler);
  }

  @Override
  public ArrowRecordBatch nextInternal() throws IOException {
    BufferAllocator allocator = SparkMemoryUtils.contextAllocator();
    if (nativeHandler == 0) {
      return null;
    }
    byte[] serializedRecordBatch = nativeNext(nativeHandler);
    if (serializedRecordBatch == null) {
      return null;
    }
    return UnsafeRecordBatchSerializer.deserializeUnsafe(allocator,
        serializedRecordBatch);
  }

  @Override
  public MetricsObject getMetricsInternal() throws IOException, ClassNotFoundException {
    return nativeFetchMetrics(nativeHandler);
  }

  @Override
  public void closeInternal() {
    if (!closed) {
      nativeClose(nativeHandler);
      closed = true;
    }
  }
}
