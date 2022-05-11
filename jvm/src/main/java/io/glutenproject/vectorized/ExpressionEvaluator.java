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

import io.glutenproject.GlutenConfig;
import io.glutenproject.backendsapi.BackendsApiManager;
import io.glutenproject.execution.AbstractColumnarNativeIterator;
import io.glutenproject.row.RowIterator;
import io.glutenproject.substrait.plan.PlanNode;

import scala.collection.JavaConverters;

import java.io.IOException;
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
    this(listJars, GlutenConfig.getConf().nativeLibName());
  }

  public ExpressionEvaluator(List<String> listJars, String libName) throws IOException, IllegalAccessException, IllegalStateException {
    this(listJars, libName,
            GlutenConfig.getSessionConf().nativeLibPath(),
            GlutenConfig.getConf().glutenBackendLib(),
            GlutenConfig.getConf().loadArrow());
  }

  public ExpressionEvaluator(String libPath)
          throws IOException, IllegalAccessException, IllegalStateException {
    this(java.util.Collections.emptyList(), null, libPath, null, GlutenConfig.getConf().loadArrow());
  }

  public ExpressionEvaluator(List<String> listJars, String libName,
                             String libPath, String customBackendLib,
                             boolean loadArrowAndGandiva)
          throws IOException, IllegalAccessException, IllegalStateException {
    String tmp_dir = GlutenConfig.getTempFile();
    if (tmp_dir == null) {
      tmp_dir = System.getProperty("java.io.tmpdir");
    }
    jniWrapper = new ExpressionEvaluatorJniWrapper(tmp_dir, listJars, libName, libPath,
            customBackendLib, loadArrowAndGandiva);
    jniWrapper.nativeSetJavaTmpDir(jniWrapper.tmpDirPath);
    jniWrapper.nativeSetBatchSize(GlutenConfig.getBatchSize());
    jniWrapper.nativeSetMetricsTime(GlutenConfig.getEnableMetricsTime());
    GlutenConfig.setRandomTempDir(jniWrapper.tmpDirPath);
  }

  long getInstanceId() {
    return nativeHandler;
  }

  // Used to initialize the native computing.
  public void initNative() {
    jniWrapper.nativeInitNative();
  }

  // Used to validate the Substrait plan in native compute engine.
  public boolean doValidate(byte[] subPlan) {
    return jniWrapper.nativeDoValidate(subPlan);
  }

  // Used by WholeStageTransfrom to create the native computing pipeline and
  // return a columnar result iterator.
  public AbstractBatchIterator createKernelWithBatchIterator(
          byte[] wsPlan, ArrayList<AbstractColumnarNativeIterator> iterList)
          throws RuntimeException, IOException {
    /* long poolId = 0;
    if (!GlutenConfig.getConf().isClickHouseBackend()) {
      // NativeMemoryPool memoryPool = SparkMemoryUtils.contextMemoryPool();
      // poolId = memoryPool.getNativeInstanceId();
    }
    ColumnarNativeIterator[] iterArray = new ColumnarNativeIterator[iterList.size()];
    long batchIteratorInstance = jniWrapper.nativeCreateKernelWithIterator(
            poolId, wsPlan, iterList.toArray(iterArray)); */
    return BackendsApiManager.getIteratorApiInstance()
        .genBatchIterator(wsPlan,
            JavaConverters.asScalaIteratorConverter(iterList.iterator()).asScala().toSeq(),
            jniWrapper);
  }

  // Used by WholeStageTransfrom to create the native computing pipeline and
  // return a columnar result iterator.
  public AbstractBatchIterator createKernelWithBatchIterator(
          PlanNode wsPlan, ArrayList<AbstractColumnarNativeIterator> iterList)
          throws RuntimeException, IOException {
    return createKernelWithBatchIterator(getPlanBytesBuf(wsPlan), iterList);
  }

  // Used by WholeStageTransfrom to create the native computing pipeline and
  // return a row result iterator.
  public RowIterator createKernelWithRowIterator(
          byte[] wsPlan,
          ArrayList<AbstractColumnarNativeIterator> iterList) throws RuntimeException, IOException {
      long rowIteratorInstance = jniWrapper.nativeCreateKernelWithRowIterator(wsPlan);
      return new RowIterator(rowIteratorInstance);
  }

  @Override
  public void close() {
    jniWrapper.nativeClose(nativeHandler);
  }

  byte[] getPlanBytesBuf(PlanNode planNode) {
    return planNode.toProtobuf().toByteArray();
  }
}
