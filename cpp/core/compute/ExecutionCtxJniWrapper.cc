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

#include "compute/ExecutionCtx.h"
#include "jni/JniCommon.h"
#include "jni/JniError.h"

#ifdef __cplusplus
extern "C" {
#endif

using namespace gluten;

JNIEXPORT jlong JNICALL Java_io_glutenproject_exec_ExecutionCtxJniWrapper_createExecutionCtx( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jbackendType,
    jbyteArray sessionConf) {
  JNI_METHOD_START
  auto backendType = jStringToCString(env, jbackendType);
  auto sparkConf = gluten::getConfMap(env, sessionConf);
  auto executionCtx = gluten::ExecutionCtx::create(backendType, sparkConf);
  return reinterpret_cast<jlong>(executionCtx);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_exec_ExecutionCtxJniWrapper_releaseExecutionCtx( // NOLINT
    JNIEnv* env,
    jclass,
    jlong ctxHandle) {
  JNI_METHOD_START
  auto executionCtx = jniCastOrThrow<ExecutionCtx>(ctxHandle);

  gluten::ExecutionCtx::release(executionCtx);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
