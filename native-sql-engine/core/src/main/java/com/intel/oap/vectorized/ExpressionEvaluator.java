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

import com.intel.oap.GazellePluginConfig;
import com.intel.oap.execution.ColumnarNativeIterator;
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.Spiller;
import com.intel.oap.substrait.plan.PlanNode;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.jni.UnsafeRecordBatchSerializer;
import org.apache.arrow.gandiva.evaluator.SelectionVectorInt16;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;

public class ExpressionEvaluator implements AutoCloseable {
  private long nativeHandler = 0;
  private ExpressionEvaluatorJniWrapper jniWrapper;

  /** Wrapper for native API. */
  public ExpressionEvaluator() throws IOException, IllegalAccessException, IllegalStateException {
    this(java.util.Collections.emptyList());
  }

  public ExpressionEvaluator(List<String> listJars) throws IOException, IllegalAccessException, IllegalStateException {
    String tmp_dir = GazellePluginConfig.getTempFile();
    if (tmp_dir == null) {
      tmp_dir = System.getProperty("java.io.tmpdir");
    }
    jniWrapper = new ExpressionEvaluatorJniWrapper(tmp_dir, listJars);
    jniWrapper.nativeSetJavaTmpDir(jniWrapper.tmp_dir_path);
    jniWrapper.nativeSetBatchSize(GazellePluginConfig.getBatchSize());
    jniWrapper.nativeSetMetricsTime(GazellePluginConfig.getEnableMetricsTime());
    GazellePluginConfig.setRandomTempDir(jniWrapper.tmp_dir_path);
  }

  long getInstanceId() {
    return nativeHandler;
  }

  /** Convert ExpressionTree into native function. */
  public String build(Schema schema, List<ExpressionTree> exprs)
      throws RuntimeException, IOException, GandivaException {
    NativeMemoryPool memoryPool = SparkMemoryUtils.contextMemoryPool();
    nativeHandler = jniWrapper.nativeBuild(memoryPool.getNativeInstanceId(), getSchemaBytesBuf(schema),
        getExprListBytesBuf(exprs), null, false);
    return jniWrapper.nativeGetSignature(nativeHandler);
  }

  /** Convert ExpressionTree into native function. */
  public String build(Schema schema, List<ExpressionTree> exprs, boolean finishReturn)
      throws RuntimeException, IOException, GandivaException {
    NativeMemoryPool memoryPool = SparkMemoryUtils.contextMemoryPool();
    nativeHandler = jniWrapper.nativeBuild(memoryPool.getNativeInstanceId(), getSchemaBytesBuf(schema),
        getExprListBytesBuf(exprs), null, finishReturn);
    return jniWrapper.nativeGetSignature(nativeHandler);
  }

  /** Convert ExpressionTree into native function. */
  public String build(Schema schema, List<ExpressionTree> exprs, Schema resSchema)
      throws RuntimeException, IOException, GandivaException {
    NativeMemoryPool memoryPool = SparkMemoryUtils.contextMemoryPool();
    nativeHandler = jniWrapper.nativeBuild(memoryPool.getNativeInstanceId(), getSchemaBytesBuf(schema),
        getExprListBytesBuf(exprs), getSchemaBytesBuf(resSchema), false);
    return jniWrapper.nativeGetSignature(nativeHandler);
  }

  /** Convert ExpressionTree into native function. */
  public String build(Schema schema, List<ExpressionTree> exprs, Schema resSchema, boolean finishReturn)
      throws RuntimeException, IOException, GandivaException {
    NativeMemoryPool memoryPool = SparkMemoryUtils.contextMemoryPool();
    nativeHandler = jniWrapper.nativeBuild(memoryPool.getNativeInstanceId(), getSchemaBytesBuf(schema),
        getExprListBytesBuf(exprs), getSchemaBytesBuf(resSchema), finishReturn);
    return jniWrapper.nativeGetSignature(nativeHandler);
  }

  /** Convert ExpressionTree into native function. */
  public String build(Schema schema, List<ExpressionTree> exprs, Schema resSchema, boolean finishReturn, NativeMemoryPool memoryPool)
      throws RuntimeException, IOException, GandivaException {
    nativeHandler = jniWrapper.nativeBuild(memoryPool.getNativeInstanceId(), getSchemaBytesBuf(schema),
        getExprListBytesBuf(exprs), getSchemaBytesBuf(resSchema), finishReturn);
    return jniWrapper.nativeGetSignature(nativeHandler);
  }

  /** Convert ExpressionTree into native function. */
  public String build(Schema schema, List<ExpressionTree> exprs, Schema resSchema, boolean finishReturn, boolean enableSpill)
      throws RuntimeException, IOException, GandivaException {
    final NativeMemoryPool memoryPool;
    if (enableSpill) {
      memoryPool = SparkMemoryUtils.createSpillableMemoryPool(new NativeSpiller());
    } else {
      memoryPool = SparkMemoryUtils.contextMemoryPool();
    }
    nativeHandler = jniWrapper.nativeBuild(memoryPool.getNativeInstanceId(), getSchemaBytesBuf(schema),
        getExprListBytesBuf(exprs), getSchemaBytesBuf(resSchema), finishReturn);
    return jniWrapper.nativeGetSignature(nativeHandler);
  }

  /** Convert ExpressionTree into native function. */
  public String build(Schema schema, List<ExpressionTree> exprs, List<ExpressionTree> finish_exprs)
      throws RuntimeException, IOException, GandivaException {
    NativeMemoryPool memoryPool = SparkMemoryUtils.contextMemoryPool();
    nativeHandler = jniWrapper.nativeBuildWithFinish(memoryPool.getNativeInstanceId(),
        getSchemaBytesBuf(schema), getExprListBytesBuf(exprs), getExprListBytesBuf(finish_exprs));
    return jniWrapper.nativeGetSignature(nativeHandler);
  }

  /** Used by WholeStageTransfrom */
  public BatchIterator createKernelWithIterator(
          Schema ws_in_schema, PlanNode ws_plan,
          Schema ws_res_schema,
          List<ExpressionTree> in_exprs, ColumnarNativeIterator batchItr,
          BatchIterator[] dependencies, boolean finishReturn)
          throws RuntimeException, IOException, GandivaException {
    NativeMemoryPool memoryPool = SparkMemoryUtils.contextMemoryPool();
    long[] instanceIdList = new long[dependencies.length];
    for (int i = 0; i < dependencies.length; i++) {
      instanceIdList[i] = dependencies[i].getInstanceId();
    }
    long batchIteratorInstance = jniWrapper.nativeCreateKernelWithIterator(
            memoryPool.getNativeInstanceId(), getSchemaBytesBuf(ws_in_schema),
            getPlanBytesBuf(ws_plan), getSchemaBytesBuf(ws_res_schema),
            getExprListBytesBuf(in_exprs),
            batchItr, instanceIdList, finishReturn);
    return new BatchIterator(batchIteratorInstance);
  }

  /** Set result Schema in some special cases */
  public void setReturnFields(Schema schema) throws RuntimeException, IOException, GandivaException {
    jniWrapper.nativeSetReturnFields(nativeHandler, getSchemaBytesBuf(schema));
  }

  /**
   * Evaluate input data using builded native function, and output as recordBatch.
   */
  public ArrowRecordBatch[] evaluate(ArrowRecordBatch recordBatch) throws RuntimeException, IOException {
    return evaluate(recordBatch, null);
  }

  public void evaluate(ColumnarNativeIterator batchItr)
          throws RuntimeException, IOException {
    jniWrapper.nativeEvaluateWithIterator(nativeHandler,
        batchItr);
  }

  /**
   * Evaluate input data using builded native function, and output as recordBatch.
   */
  public ArrowRecordBatch[] evaluate2(ArrowRecordBatch recordBatch) throws RuntimeException, IOException {
    byte[] bytes = UnsafeRecordBatchSerializer.serializeUnsafe(recordBatch);
    byte[][] serializedBatchArray = jniWrapper.nativeEvaluate2(nativeHandler, bytes);
    BufferAllocator allocator = SparkMemoryUtils.contextAllocator();
    ArrowRecordBatch[] recordBatchList = new ArrowRecordBatch[serializedBatchArray.length];
    for (int i = 0; i < serializedBatchArray.length; i++) {
      if (serializedBatchArray[i] == null) {
        recordBatchList[i] = null;
        break;
      }
      recordBatchList[i] = UnsafeRecordBatchSerializer.deserializeUnsafe(allocator,
          serializedBatchArray[i]);
    }
    return recordBatchList;
  }

  /**
   * Evaluate input data using builded native function, and output as recordBatch.
   */
  public ArrowRecordBatch[] evaluate(ArrowRecordBatch recordBatch, SelectionVectorInt16 selectionVector)
      throws RuntimeException, IOException {
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

    BufferAllocator allocator = SparkMemoryUtils.contextAllocator();

    byte[][] serializedBatchArray;
    if (selectionVector != null) {
      int selectionVectorRecordCount = selectionVector.getRecordCount();
      long selectionVectorAddr = selectionVector.getBuffer().memoryAddress();
      long selectionVectorSize = selectionVector.getBuffer().capacity();
      serializedBatchArray = jniWrapper.nativeEvaluateWithSelection(nativeHandler, recordBatch.getLength(),
          bufAddrs, bufSizes, selectionVectorRecordCount, selectionVectorAddr, selectionVectorSize);
    } else {
      serializedBatchArray = jniWrapper.nativeEvaluate(nativeHandler, recordBatch.getLength(), bufAddrs, bufSizes);
    }
    ArrowRecordBatch[] recordBatchList = new ArrowRecordBatch[serializedBatchArray.length];
    for (int i = 0; i < serializedBatchArray.length; i++) {
      if (serializedBatchArray[i] == null) {
        recordBatchList[i] = null;
        break;
      }
      recordBatchList[i] = UnsafeRecordBatchSerializer.deserializeUnsafe(allocator,
          serializedBatchArray[i]);
    }
    return recordBatchList;
  }

  /**
   * Evaluate input data using builded native function, and output as recordBatch.
   */
  public void SetMember(ArrowRecordBatch recordBatch) throws RuntimeException, IOException {
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

    jniWrapper.nativeSetMember(nativeHandler, recordBatch.getLength(), bufAddrs, bufSizes);
  }

  public ArrowRecordBatch[] finish() throws RuntimeException, IOException {
    BufferAllocator allocator = SparkMemoryUtils.contextAllocator();
    byte[][] serializedBatchArray = jniWrapper.nativeFinish(nativeHandler);
    ArrowRecordBatch[] recordBatchList = new ArrowRecordBatch[serializedBatchArray.length];
    for (int i = 0; i < serializedBatchArray.length; i++) {
      if (serializedBatchArray[i] == null) {
        recordBatchList[i] = null;
        break;
      }
      recordBatchList[i] = UnsafeRecordBatchSerializer.deserializeUnsafe(allocator,
          serializedBatchArray[i]);
    }
    return recordBatchList;
  }

  public BatchIterator finishByIterator() throws RuntimeException, IOException {
    long batchIteratorInstance = jniWrapper.nativeFinishByIterator(nativeHandler);
    return new BatchIterator(batchIteratorInstance);
  }

  public void setDependency(BatchIterator child) throws RuntimeException, IOException {
    jniWrapper.nativeSetDependency(nativeHandler, child.getInstanceId(), -1);
  }

  public void setDependency(BatchIterator child, int index) throws RuntimeException, IOException {
    jniWrapper.nativeSetDependency(nativeHandler, child.getInstanceId(), index);
  }

  @Override
  public void close() {
    jniWrapper.nativeClose(nativeHandler);
  }

  byte[] getSchemaBytesBuf(Schema schema) throws IOException {
    if (schema == null) return null; 
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    return out.toByteArray();
  }

  byte[] getExprListBytesBuf(List<ExpressionTree> exprs) throws GandivaException {
    GandivaTypes.ExpressionList.Builder builder = GandivaTypes.ExpressionList.newBuilder();
    for (ExpressionTree expr : exprs) {
      builder.addExprs(expr.toProtobuf());
    }
    return builder.build().toByteArray();
  }

  byte[] getPlanBytesBuf(PlanNode planNode) {
    return planNode.toProtobuf().toByteArray();
  }

  private class NativeSpiller implements Spiller {

    private NativeSpiller() {
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) {
      if (nativeHandler == 0) {
        throw new IllegalStateException("Fatal: spill() called before a spillable expression " +
            "evaluator is created. This behavior should be optimized by moving memory " +
            "allocations from expression build to evaluation");
      }
      return jniWrapper.nativeSpill(nativeHandler, size, false); // fixme pass true when being called by self
    }
  }
}
