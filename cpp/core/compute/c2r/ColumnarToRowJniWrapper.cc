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

#include "jni/JniCommon.h"
#include "jni/JniError.h"

#ifdef __cplusplus
extern "C" {
#endif

using namespace gluten;

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeColumnarToRowInit( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  // Convert the native batch to Spark unsafe row.
  return ctx->createColumnar2RowConverter(memoryManager);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeColumnarToRowConvert( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle,
    jlong c2rHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto columnarToRowConverter = ctx->getColumnar2RowConverter(c2rHandle);
  auto cb = ctx->getBatch(batchHandle);
  columnarToRowConverter->convert(cb);

  const auto& offsets = columnarToRowConverter->getOffsets();
  const auto& lengths = columnarToRowConverter->getLengths();

  auto numRows = cb->numRows();

  auto offsetsArr = env->NewIntArray(numRows);
  auto offsetsSrc = reinterpret_cast<const jint*>(offsets.data());
  env->SetIntArrayRegion(offsetsArr, 0, numRows, offsetsSrc);
  auto lengthsArr = env->NewIntArray(numRows);
  auto lengthsSrc = reinterpret_cast<const jint*>(lengths.data());
  env->SetIntArrayRegion(lengthsArr, 0, numRows, lengthsSrc);
  long address = reinterpret_cast<long>(columnarToRowConverter->getBufferAddress());

  jobject nativeColumnarToRowInfo = env->NewObject(
      gluten::getJniCommonState()->nativeColumnarToRowInfoClass,
      gluten::getJniCommonState()->nativeColumnarToRowInfoConstructor,
      offsetsArr,
      lengthsArr,
      address);
  return nativeColumnarToRowInfo;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeClose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong c2rHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  ctx->releaseColumnar2RowConverter(c2rHandle);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
