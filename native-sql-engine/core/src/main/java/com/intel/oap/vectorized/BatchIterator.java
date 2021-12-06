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
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import org.apache.arrow.gandiva.evaluator.SelectionVectorInt16;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;

public class BatchIterator implements AutoCloseable, Serializable {
  private native boolean nativeHasNext(long nativeHandler);
  private native byte[] nativeNext(long nativeHandler);
  private native MetricsObject nativeFetchMetrics(long nativeHandler);
  private native byte[] nativeProcess(long nativeHandler,
      byte[] schemaBuf, int numRows, long[] bufAddrs, long[] bufSizes);
  private native void nativeProcessAndCacheOne(long nativeHandler, byte[] schemaBuf,
      int numRows, long[] bufAddrs, long[] bufSizes);
  private native byte[] nativeProcessWithSelection(long nativeHandler,
      byte[] schemaBuf, int numRows, long[] bufAddrs, long[] bufSizes,
      int selectionVectorRecordCount, long selectionVectorAddr, long selectionVectorSize);
  private native void nativeProcessAndCacheOneWithSelection(long nativeHandler,
      byte[] schemaBuf, int numRows, long[] bufAddrs, long[] bufSizes,
      int selectionVectorRecordCount, long selectionVectorAddr, long selectionVectorSize);
  private native void nativeSetDependencies(long nativeHandler, long[] dependencies);
  private native NativeSerializableObject nativeNextHashRelation(long nativeHandler);
  private native void nativeSetHashRelation(
      long nativeHandler, long[] memoryAddrs, int[] sizes);
  private native void nativeClose(long nativeHandler);

  private long nativeHandler = 0;
  private boolean closed = false;

  public BatchIterator() throws IOException {}

  public BatchIterator(long instance_id) throws IOException {
    JniUtils.getInstance();
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

  public SerializableObject nextHashRelationObject()
      throws IOException, ClassNotFoundException {
    if (nativeHandler == 0) {
      return null;
    }
    NativeSerializableObject obj = nativeNextHashRelation(nativeHandler);
    SerializableObject objImpl = new SerializableObject(obj, new AutoCloseable[]{this});
    return objImpl;
  }

  public void setHashRelationObject(SerializableObject obj) throws IOException {
    if (nativeHandler == 0) {
      return;
    }
    // pass directbuffer memory to below function
    nativeSetHashRelation(nativeHandler, obj.getDirectMemoryAddrs(), obj.size);
  }

  public ArrowRecordBatch process(Schema schema, ArrowRecordBatch recordBatch)
      throws IOException {
    return process(schema, recordBatch, null);
  }

  public ArrowRecordBatch process(Schema schema, ArrowRecordBatch recordBatch,
      SelectionVectorInt16 selectionVector) throws IOException {
    int num_rows = recordBatch.getLength();
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();

    long[] bufAddrs = new long[buffers.size()];
    long[] bufSizes = new long[buffers.size()];

    int idx = 0;
    for (ArrowBuf buf : buffers) {
      bufAddrs[idx++] = buf.memoryAddress();
    }

    idx = 0;
    for (ArrowBuffer bufLayout : buffersLayout) {
      bufSizes[idx++] = bufLayout.getSize();
    }

    if (nativeHandler == 0) {
      return null;
    }
    BufferAllocator allocator = SparkMemoryUtils.contextAllocator();
    byte[] serializedRecordBatch;
    if (selectionVector != null) {
      int selectionVectorRecordCount = selectionVector.getRecordCount();
      long selectionVectorAddr = selectionVector.getBuffer().memoryAddress();
      long selectionVectorSize = selectionVector.getBuffer().capacity();
      serializedRecordBatch = nativeProcessWithSelection(nativeHandler,
          getSchemaBytesBuf(schema), num_rows, bufAddrs, bufSizes,
          selectionVectorRecordCount, selectionVectorAddr, selectionVectorSize);
    } else {
      serializedRecordBatch = nativeProcess(
          nativeHandler, getSchemaBytesBuf(schema), num_rows, bufAddrs, bufSizes);
    }
    if (serializedRecordBatch == null) {
      return null;
    }
    return UnsafeRecordBatchSerializer.deserializeUnsafe(allocator,
        serializedRecordBatch);
  }

  public void processAndCacheOne(Schema schema, ArrowRecordBatch recordBatch)
      throws IOException {
    processAndCacheOne(schema, recordBatch, null);
  }

  public void processAndCacheOne(Schema schema, ArrowRecordBatch recordBatch,
      SelectionVectorInt16 selectionVector) throws IOException {
    int num_rows = recordBatch.getLength();
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();

    long[] bufAddrs = new long[buffers.size()];
    long[] bufSizes = new long[buffers.size()];

    int idx = 0;
    for (ArrowBuf buf : buffers) {
      bufAddrs[idx++] = buf.memoryAddress();
    }

    idx = 0;
    for (ArrowBuffer bufLayout : buffersLayout) {
      bufSizes[idx++] = bufLayout.getSize();
    }

    if (nativeHandler == 0) {
      return;
    }
    if (selectionVector != null) {
      int selectionVectorRecordCount = selectionVector.getRecordCount();
      long selectionVectorAddr = selectionVector.getBuffer().memoryAddress();
      long selectionVectorSize = selectionVector.getBuffer().capacity();
      nativeProcessAndCacheOneWithSelection(nativeHandler, getSchemaBytesBuf(schema),
          num_rows, bufAddrs, bufSizes, selectionVectorRecordCount, selectionVectorAddr,
          selectionVectorSize);
    } else {
      nativeProcessAndCacheOne(
          nativeHandler, getSchemaBytesBuf(schema), num_rows, bufAddrs, bufSizes);
    }
  }

  public void setDependencies(BatchIterator[] dependencies) {
    long[] instanceIdList = new long[dependencies.length];
    for (int i = 0; i < dependencies.length; i++) {
      instanceIdList[i] = dependencies[i].getInstanceId();
    }
    nativeSetDependencies(nativeHandler, instanceIdList);
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
