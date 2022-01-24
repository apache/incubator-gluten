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
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.ipc.WriteChannel;
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
    this(listJars, null);
  }

  public ExpressionEvaluator(List<String> listJars, String libName) throws IOException, IllegalAccessException, IllegalStateException {
    String tmp_dir = GazellePluginConfig.getTempFile();
    if (tmp_dir == null) {
      tmp_dir = System.getProperty("java.io.tmpdir");
    }
    jniWrapper = new ExpressionEvaluatorJniWrapper(tmp_dir, listJars, libName);
    jniWrapper.nativeSetJavaTmpDir(jniWrapper.tmp_dir_path);
    jniWrapper.nativeSetBatchSize(GazellePluginConfig.getBatchSize());
    jniWrapper.nativeSetMetricsTime(GazellePluginConfig.getEnableMetricsTime());
    GazellePluginConfig.setRandomTempDir(jniWrapper.tmp_dir_path);
  }

  long getInstanceId() {
    return nativeHandler;
  }

  /** Used by WholeStageTransfrom */
  public BatchIterator createNativeKernelWithIterator(
          Schema ws_in_schema, byte[] ws_plan,
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
            ws_plan, getSchemaBytesBuf(ws_res_schema),
            getExprListBytesBuf(in_exprs),
            batchItr, instanceIdList, finishReturn);
    return new BatchIterator(batchIteratorInstance);
  }

  public BatchIterator createKernelWithIterator(
          Schema ws_in_schema, PlanNode ws_plan,
          Schema ws_res_schema,
          List<ExpressionTree> in_exprs, ColumnarNativeIterator batchItr,
          BatchIterator[] dependencies, boolean finishReturn)
          throws RuntimeException, IOException, GandivaException {
    return createNativeKernelWithIterator(ws_in_schema, getPlanBytesBuf(ws_plan),
            ws_res_schema, in_exprs, batchItr, dependencies, finishReturn);
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
