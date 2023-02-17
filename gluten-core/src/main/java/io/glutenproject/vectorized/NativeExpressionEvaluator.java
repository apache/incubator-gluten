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

import com.google.protobuf.Any;
import io.glutenproject.GlutenConfig;
import io.glutenproject.backendsapi.BackendsApiManager;
import io.glutenproject.memory.alloc.NativeMemoryAllocators;
import io.glutenproject.substrait.expression.ExpressionBuilder;
import io.glutenproject.substrait.expression.StringMapNode;
import io.glutenproject.substrait.extensions.AdvancedExtensionNode;
import io.glutenproject.substrait.extensions.ExtensionBuilder;
import io.glutenproject.substrait.plan.PlanBuilder;
import io.glutenproject.substrait.plan.PlanNode;
import io.glutenproject.utils.DebugUtil;
import io.substrait.proto.Plan;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.expressions.Attribute;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A toolkit for backends that may require JNI to implement native validation/evaluation.
 * Note it's not a MUST for backend to implement ExpressionEvaluatorJniWrapper. In the case
 * do not explicitly make any JVM code reference to this class so the jni wrapper will not get
 * initialized by classloader.
 */
public abstract class NativeExpressionEvaluator implements AutoCloseable {
  private final ExpressionEvaluatorJniWrapper jniWrapper;

  public NativeExpressionEvaluator() {
    jniWrapper = new ExpressionEvaluatorJniWrapper();
  }

  // Used to initialize the native computing.
  public void initNative(SparkConf conf) {
    jniWrapper.nativeInitNative(buildNativeConfNode(GlutenConfig.getNativeStaticConf(conf,
        BackendsApiManager.getSettings().getBackendConfigPrefix())).toProtobuf().toByteArray());
  }

  // Used to validate the Substrait plan in native compute engine.
  public boolean doValidate(byte[] subPlan) {
    return jniWrapper.nativeDoValidate(subPlan);
  }

  private PlanNode buildNativeConfNode(Map<String, String> confs) {
    StringMapNode stringMapNode = ExpressionBuilder.makeStringMap(confs);
    AdvancedExtensionNode extensionNode = ExtensionBuilder
        .makeAdvancedExtension(Any.pack(stringMapNode.toProtobuf()));
    return PlanBuilder.makePlan(extensionNode);
  }

  // Used by WholeStageTransfrom to create the native computing pipeline and
  // return a columnar result iterator.
  public GeneralOutIterator createKernelWithBatchIterator(
      Plan wsPlan, List<GeneralInIterator> iterList, List<Attribute> outAttrs)
      throws RuntimeException, IOException {
    long allocId = NativeMemoryAllocators.contextInstance().getNativeInstanceId();
    long handle =
        jniWrapper.nativeCreateKernelWithIterator(allocId, getPlanBytesBuf(wsPlan),
            iterList.toArray(new GeneralInIterator[0]), TaskContext.get().stageId(),
            TaskContext.getPartitionId(), TaskContext.get().taskAttemptId(),
            DebugUtil.saveInputToFile(),
            buildNativeConfNode(GlutenConfig.getNativeSessionConf()).toProtobuf().toByteArray());
    return createOutIterator(handle, outAttrs);
  }

  protected abstract GeneralOutIterator createOutIterator(
      long nativeHandle, List<Attribute> outAttrs) throws IOException;

  byte[] getPlanBytesBuf(Plan planNode) {
    return planNode.toByteArray();
  }

  @Override
  public void close() throws Exception {
    // no-op
  }
}
