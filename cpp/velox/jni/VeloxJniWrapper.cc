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

#include <glog/logging.h>
#include <jni/JniCommon.h>
#include <exception>
#include "JniUdf.h"
#include "compute/VeloxBackend.h"
#include "compute/VeloxExecutionCtx.h"
#include "config/GlutenConfig.h"
#include "jni/JniError.h"
#include "jni/JniFileSystem.h"
#include "memory/VeloxMemoryManager.h"
#include "substrait/SubstraitToVeloxPlanValidator.h"

#include <iostream>

using namespace facebook;

namespace {

gluten::ExecutionCtx* veloxExecutionCtxFactory(const std::unordered_map<std::string, std::string>& sparkConfs) {
  return new gluten::VeloxExecutionCtx(sparkConfs);
}

} // namespace

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void*) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    return JNI_ERR;
  }

  // logging
  google::InitGoogleLogging("gluten");
  FLAGS_logtostderr = true;
  gluten::getJniCommonState()->ensureInitialized(env);
  gluten::getJniErrorState()->ensureInitialized(env);
  gluten::initVeloxJniFileSystem(env);
  gluten::initVeloxJniUDF(env);

  DEBUG_OUT << "Loaded Velox backend." << std::endl;

  return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void*) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);
  gluten::finalizeVeloxJniUDF(env);
  gluten::finalizeVeloxJniFileSystem(env);
  gluten::getJniErrorState()->close();
  gluten::getJniCommonState()->close();
  google::ShutdownGoogleLogging();
}

JNIEXPORT void JNICALL Java_io_glutenproject_init_BackendJniWrapper_initializeBackend( // NOLINT
    JNIEnv* env,
    jclass,
    jbyteArray planArray) {
  JNI_METHOD_START
  auto sparkConfs = gluten::getConfMap(env, planArray);
  gluten::setExecutionCtxFactory(veloxExecutionCtxFactory, sparkConfs);
  gluten::VeloxBackend::create(sparkConfs);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_udf_UdfJniWrapper_nativeLoadUdfLibraries( // NOLINT
    JNIEnv* env,
    jclass,
    jstring libPaths) {
  JNI_METHOD_START
  gluten::jniLoadUdf(env, jStringToCString(env, libPaths));
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_PlanEvaluatorJniWrapper_nativeValidateWithFailureReason( // NOLINT
    JNIEnv* env,
    jobject,
    jbyteArray planArray) {
  JNI_METHOD_START
  auto planData = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArray, 0));
  auto planSize = env->GetArrayLength(planArray);
  ::substrait::Plan subPlan;
  gluten::parseProtobuf(planData, planSize, &subPlan);

  // A query context used for function validation.
  velox::core::QueryCtx queryCtx;
  auto pool = gluten::defaultLeafVeloxMemoryPool().get();
  // An execution context used for function validation.
  velox::core::ExecCtx execCtx(pool, &queryCtx);

  gluten::SubstraitToVeloxPlanValidator planValidator(pool, &execCtx);
  jclass infoCls = env->FindClass("Lio/glutenproject/validate/NativePlanValidationInfo;");
  if (infoCls == nullptr) {
    std::string errorMessage = "Unable to CreateGlobalClassReferenceOrError for NativePlanValidationInfo";
    throw gluten::GlutenException(errorMessage);
  }
  jmethodID method = env->GetMethodID(infoCls, "<init>", "(ILjava/lang/String;)V");
  try {
    auto isSupported = planValidator.validate(subPlan);
    auto logs = planValidator.getValidateLog();
    std::string concatLog;
    for (int i = 0; i < logs.size(); i++) {
      concatLog += logs[i] + "@";
    }
    return env->NewObject(infoCls, method, isSupported, env->NewStringUTF(concatLog.c_str()));
  } catch (std::invalid_argument& e) {
    LOG(INFO) << "Failed to validate substrait plan because " << e.what();
    // return false;
    auto isSupported = false;
    return env->NewObject(infoCls, method, isSupported, env->NewStringUTF(""));
  }
  JNI_METHOD_END(nullptr)
}

#ifdef __cplusplus
}
#endif
