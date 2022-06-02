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

#include <jni.h>

#include "compute/VeloxPlanConverter.h"
#include "velox/substrait/SubstraitToVeloxPlanValidator.h"

static jint JNI_VERSION = JNI_VERSION_1_8;

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
#ifdef DEBUG
  std::cout << "Loaded Velox backend." << std::endl;
#endif
  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative(
    JNIEnv* env, jobject obj) {
  gluten::SetBackendFactory(
      [] { return std::make_shared<::velox::compute::VeloxPlanConverter>(); });
  static auto veloxInitializer = std::make_shared<::velox::compute::VeloxInitializer>();
}

JNIEXPORT jboolean JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeDoValidate(
    JNIEnv* env, jobject obj, jbyteArray planArray) {
  auto planData =
      reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArray, 0));
  auto planSize = env->GetArrayLength(planArray);
  ::substrait::Plan subPlan;
  ParseProtobuf(planData, planSize, &subPlan);
  auto planValidator =
      std::make_shared<facebook::velox::substrait::SubstraitToVeloxPlanValidator>();
  return planValidator->validate(subPlan);
}

#ifdef __cplusplus
}
#endif
