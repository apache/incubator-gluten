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

import com.intel.oap.GazelleJniConfig;
import com.intel.oap.execution.ColumnarNativeIterator;
import com.intel.oap.row.RowIterator;
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
import java.util.ArrayList;
import java.util.List;

public class ExpressionEvaluator implements AutoCloseable {
  private long nativeHandler = 0;
  private ExpressionEvaluatorJniWrapper jniWrapper;

  /** Wrapper for native API. */
  public ExpressionEvaluator() throws IOException, IllegalAccessException, IllegalStateException {
    this(java.util.Collections.emptyList());
  }

  public ExpressionEvaluator(List<String> listJars) throws IOException, IllegalAccessException, IllegalStateException {
    this(listJars, GazelleJniConfig.getConf().nativeLibName());
  }

  public ExpressionEvaluator(List<String> listJars, String libName) throws IOException, IllegalAccessException, IllegalStateException {
    this(listJars, libName,
            GazelleJniConfig.getSessionConf().nativeLibPath(),
            GazelleJniConfig.getConf().loadArrow());
  }

  public ExpressionEvaluator(String libPath)
          throws IOException, IllegalAccessException, IllegalStateException {
    this(java.util.Collections.emptyList(), null, libPath, GazelleJniConfig.getConf().loadArrow());
  }

  public ExpressionEvaluator(List<String> listJars, String libName,
                             String libPath, boolean loadArrowAndGandiva)
          throws IOException, IllegalAccessException, IllegalStateException {
    String tmp_dir = GazelleJniConfig.getTempFile();
    if (tmp_dir == null) {
      tmp_dir = System.getProperty("java.io.tmpdir");
    }
    jniWrapper = new ExpressionEvaluatorJniWrapper(tmp_dir, listJars, libName, libPath,
            loadArrowAndGandiva);
    jniWrapper.nativeSetJavaTmpDir(jniWrapper.tmp_dir_path);
    jniWrapper.nativeSetBatchSize(GazelleJniConfig.getBatchSize());
    jniWrapper.nativeSetMetricsTime(GazelleJniConfig.getEnableMetricsTime());
    GazelleJniConfig.setRandomTempDir(jniWrapper.tmp_dir_path);
  }

  long getInstanceId() {
    return nativeHandler;
  }

  public void initNative() {
    jniWrapper.nativeInitNative();
  }

  /** Used by WholeStageTransfrom */
  public BatchIterator createKernelWithBatchIterator(
          byte[] wsPlan, ArrayList<ColumnarNativeIterator> iterList)
          throws RuntimeException, IOException {
    NativeMemoryPool memoryPool = SparkMemoryUtils.contextMemoryPool();
    ColumnarNativeIterator[] iterArray = new ColumnarNativeIterator[iterList.size()];
    long batchIteratorInstance = jniWrapper.nativeCreateKernelWithIterator(
            memoryPool.getNativeInstanceId(), wsPlan, iterList.toArray(iterArray));
    return new BatchIterator(batchIteratorInstance);
  }

  public BatchIterator createKernelWithBatchIterator(
          PlanNode wsPlan, ArrayList<ColumnarNativeIterator> iterList)
          throws RuntimeException, IOException {
    return createKernelWithBatchIterator(getPlanBytesBuf(wsPlan), iterList);
  }

  public RowIterator createKernelWithRowIterator(
          byte[] wsPlan,
          ArrayList<ColumnarNativeIterator> iterList) throws RuntimeException, IOException {
      long rowIteratorInstance = jniWrapper.nativeCreateKernelWithRowIterator(wsPlan);
      return new RowIterator(rowIteratorInstance);
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
}
