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
import io.glutenproject.exec.ExecutionCtxs;
import io.glutenproject.memory.nmm.NativeMemoryManagers;
import io.glutenproject.substrait.expression.ExpressionBuilder;
import io.glutenproject.substrait.expression.StringMapNode;
import io.glutenproject.substrait.extensions.AdvancedExtensionNode;
import io.glutenproject.substrait.extensions.ExtensionBuilder;
import io.glutenproject.substrait.plan.PlanBuilder;
import io.glutenproject.substrait.plan.PlanNode;
import io.glutenproject.utils.DebugUtil;
import io.glutenproject.validate.NativePlanValidationInfo;

import com.google.protobuf.Any;
import io.substrait.proto.Plan;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.util.SparkDirectoryUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class NativePlanEvaluator {

  private final PlanEvaluatorJniWrapper jniWrapper;

  public NativePlanEvaluator() {
    jniWrapper = new PlanEvaluatorJniWrapper();
  }

  public NativePlanValidationInfo doNativeValidateWithFailureReason(byte[] subPlan) {
    return jniWrapper.nativeValidateWithFailureReason(subPlan);
  }

  private PlanNode buildNativeConfNode(Map<String, String> confs) {
    StringMapNode stringMapNode = ExpressionBuilder.makeStringMap(confs);
    AdvancedExtensionNode extensionNode =
        ExtensionBuilder.makeAdvancedExtension(Any.pack(stringMapNode.toProtobuf()));
    return PlanBuilder.makePlan(extensionNode);
  }

  // Used by WholeStageTransform to create the native computing pipeline and
  // return a columnar result iterator.
  public GeneralOutIterator createKernelWithBatchIterator(
      Plan wsPlan, List<GeneralInIterator> iterList) throws RuntimeException, IOException {
    final long executionCtxHandle = ExecutionCtxs.contextInstance().getHandle();
    final AtomicReference<ColumnarBatchOutIterator> outIterator = new AtomicReference<>();
    final long memoryManagerHandle =
        NativeMemoryManagers.create(
                "WholeStageIterator",
                (size) -> {
                  ColumnarBatchOutIterator instance =
                      Optional.of(outIterator.get())
                          .orElseThrow(
                              () ->
                                  new IllegalStateException(
                                      "Fatal: spill() called before a output iterator "
                                          + "is created. This behavior should be optimized "
                                          + "by moving memory allocations from create() to "
                                          + "hasNext()/next()"));
                  return instance.spill(size);
                })
            .getNativeInstanceHandle();

    final String spillDirPath =
        SparkDirectoryUtil.namespace("gluten-spill")
            .mkChildDirRoundRobin(UUID.randomUUID().toString())
            .getAbsolutePath();

    long iterHandle =
        jniWrapper.nativeCreateKernelWithIterator(
            executionCtxHandle,
            memoryManagerHandle,
            getPlanBytesBuf(wsPlan),
            iterList.toArray(new GeneralInIterator[0]),
            TaskContext.get().stageId(),
            TaskContext.getPartitionId(),
            TaskContext.get().taskAttemptId(),
            DebugUtil.saveInputToFile(),
            BackendsApiManager.getSparkPlanExecApiInstance().rewriteSpillPath(spillDirPath),
            buildNativeConfNode(
                    GlutenConfig.getNativeSessionConf(
                        BackendsApiManager.getSettings().getBackendConfigPrefix(),
                        SQLConf.get().getAllConfs()))
                .toProtobuf()
                .toByteArray());
    outIterator.set(createOutIterator(executionCtxHandle, iterHandle));
    return outIterator.get();
  }

  private ColumnarBatchOutIterator createOutIterator(long executionCtxHandle, long iterHandle)
      throws IOException {
    return new ColumnarBatchOutIterator(executionCtxHandle, iterHandle);
  }

  private byte[] getPlanBytesBuf(Plan planNode) {
    return planNode.toByteArray();
  }
}
