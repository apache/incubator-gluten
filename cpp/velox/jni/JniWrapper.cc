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
#include "arrow/c/bridge.h"

#include <glog/logging.h>
#include <jni/JniCommon.h>
#include <exception>
#include "compute/VeloxBackend.h"
#include "compute/VeloxInitializer.h"
#include "compute/VeloxParquetDatasource.h"
#include "config/GlutenConfig.h"
#include "jni/JniErrors.h"
#include "memory/VeloxMemoryPool.h"
#include "velox/substrait/SubstraitToVeloxPlanValidator.h"

#include <iostream>

using namespace facebook;

static std::unordered_map<std::string, std::string> sparkConfs;

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    return JNI_ERR;
  }
  // logging
  google::InitGoogleLogging("gluten");
  gluten::getJniErrorsState()->initialize(env);

#ifdef GLUTEN_PRINT_DEBUG
  std::cout << "Loaded Velox backend." << std::endl;
#endif
  return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative( // NOLINT
    JNIEnv* env,
    jobject obj,
    jbyteArray planArray) {
  JNI_METHOD_START
  sparkConfs = gluten::getConfMap(env, planArray);
  // FIXME this is not thread-safe. The function can be called twice
  //   within Spark local-mode, one from Driver, another from Executor.
  gluten::setBackendFactory([] { return std::make_shared<gluten::VeloxBackend>(sparkConfs); });
  static auto veloxInitializer = std::make_shared<gluten::VeloxInitializer>(sparkConfs);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenprovject_vectorized_ExpressionEvaluatorJniWrapper_nativeFinalizeNative( // NOLINT
    JNIEnv* env){JNI_METHOD_START
                     // TODO Release resources allocated for single executor/driver
                     JNI_METHOD_END()}

JNIEXPORT jboolean JNICALL Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeDoValidate( // NOLINT
    JNIEnv* env,
    jobject obj,
    jbyteArray planArray) {
  JNI_METHOD_START
  auto planData = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArray, 0));
  auto planSize = env->GetArrayLength(planArray);
  ::substrait::Plan subPlan;
  gluten::parseProtobuf(planData, planSize, &subPlan);

  // A query context used for function validation.
  velox::core::QueryCtx queryCtx;
  auto pool = gluten::getDefaultVeloxLeafMemoryPool().get();
  // An execution context used for function validation.
  velox::core::ExecCtx execCtx(pool, &queryCtx);

  velox::substrait::SubstraitToVeloxPlanValidator planValidator(pool, &execCtx);
  try {
    return planValidator.validate(subPlan);
  } catch (std::invalid_argument& e) {
    LOG(INFO) << "Failed to validate substrait plan because " << e.what();
    return false;
  }
  JNI_METHOD_END(false)
}

#ifdef __cplusplus
}
#endif
