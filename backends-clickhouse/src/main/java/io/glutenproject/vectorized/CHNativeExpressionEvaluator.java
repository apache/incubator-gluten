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
import io.substrait.proto.Plan;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CHNativeExpressionEvaluator {
  private final ExpressionEvaluatorJniWrapper jniWrapper;

  public CHNativeExpressionEvaluator() {
    jniWrapper = new ExpressionEvaluatorJniWrapper();
  }

  // Used to initialize the native computing.
  public void initNative(SparkConf conf) {
    Tuple2<String, String>[] all = conf.getAll();
    Map<String, String> confMap =
        Arrays.stream(all).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    String prefix = BackendsApiManager.getSettings().getBackendConfigPrefix();
    Map<String, String> nativeConfMap =
        GlutenConfig.getNativeBackendConf(prefix, JavaConverters.mapAsScalaMap(confMap));

    // Get the customer config from SparkConf for each backend
    BackendsApiManager.getTransformerApiInstance().postProcessNativeConfig(nativeConfMap, prefix);

    jniWrapper.nativeInitNative(buildNativeConfNode(nativeConfMap).toProtobuf().toByteArray());
  }

  public void finalizeNative() {
    jniWrapper.nativeFinalizeNative();
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

  // Used by WholeStageTransform to create the native computing pipeline and
  // return a columnar result iterator.
  public GeneralOutIterator createKernelWithBatchIterator(
      Plan wsPlan, List<GeneralInIterator> iterList, List<Attribute> outAttrs)
      throws RuntimeException, IOException {
    long allocId = NativeMemoryAllocators.contextInstance().getNativeInstanceId();
    long handle =
        jniWrapper.nativeCreateKernelWithIterator(allocId, getPlanBytesBuf(wsPlan),
            iterList.toArray(new GeneralInIterator[0]));
    return createOutIterator(handle, outAttrs);
  }

  private byte[] getPlanBytesBuf(Plan planNode) {
    return planNode.toByteArray();
  }

  private GeneralOutIterator createOutIterator(
      long nativeHandle, List<Attribute> outAttrs) throws IOException {
    return new BatchIterator(nativeHandle, outAttrs);
  }
}
