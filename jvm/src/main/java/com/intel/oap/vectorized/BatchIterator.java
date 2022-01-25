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

package com.intel.oap.vectorized;

import java.io.IOException;

import org.apache.arrow.dataset.jni.UnsafeRecordBatchSerializer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;

public class BatchIterator implements AutoCloseable, Serializable {
  private native boolean nativeHasNext(long nativeHandler);
  private native byte[] nativeNext(long nativeHandler);
  private native void nativeClose(long nativeHandler);
  private native MetricsObject nativeFetchMetrics(long nativeHandler);

  private long nativeHandler = 0;
  private boolean closed = false;

  public BatchIterator() throws IOException {}

  public BatchIterator(long instance_id) throws IOException {
    nativeHandler = instance_id;
  }

  public boolean hasNext() throws IOException {
    return nativeHasNext(nativeHandler);
  }

  public ArrowRecordBatch next() throws IOException {
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

  public MetricsObject getMetrics() throws IOException, ClassNotFoundException {
    if (nativeHandler == 0) {
      return null;
    }
    return nativeFetchMetrics(nativeHandler);
  }

  @Override
  public void close() {
    if (!closed) {
      nativeClose(nativeHandler);
      closed = true;
    }
  }

  byte[] getSchemaBytesBuf(Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    return out.toByteArray();
  }

  long getInstanceId() {
    return nativeHandler;
  }
}
