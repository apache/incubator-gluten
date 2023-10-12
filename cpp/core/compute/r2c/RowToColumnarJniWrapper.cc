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

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_NativeRowToColumnarJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  return ctx->createRow2ColumnarConverter(memoryManager, reinterpret_cast<struct ArrowSchema*>(cSchema));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_NativeRowToColumnarJniWrapper_nativeConvertRowToColumnar( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong r2cHandle,
    jlongArray rowLength,
    jlong memoryAddress) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  if (rowLength == nullptr) {
    throw gluten::GlutenException("Native convert row to columnar: buf_addrs can't be null");
  }
  int numRows = env->GetArrayLength(rowLength);
  jlong* inRowLength = env->GetLongArrayElements(rowLength, nullptr);
  uint8_t* address = reinterpret_cast<uint8_t*>(memoryAddress);

  auto converter = ctx->getRow2ColumnarConverter(r2cHandle);
  auto cb = converter->convert(numRows, reinterpret_cast<int64_t*>(inRowLength), address);
  env->ReleaseLongArrayElements(rowLength, inRowLength, JNI_ABORT);
  return ctx->addBatch(cb);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_NativeRowToColumnarJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong r2cHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  ctx->releaseRow2ColumnarConverter(r2cHandle);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
