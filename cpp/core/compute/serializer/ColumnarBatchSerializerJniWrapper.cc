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

JNIEXPORT jobject JNICALL Java_io_glutenproject_vectorized_ColumnarBatchSerializerJniWrapper_serialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlongArray handles,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  int32_t numBatches = env->GetArrayLength(handles);
  jlong* batchHandles = env->GetLongArrayElements(handles, nullptr);

  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  int64_t numRows = 0L;
  for (int32_t i = 0; i < numBatches; i++) {
    auto batch = ctx->getBatch(batchHandles[i]);
    GLUTEN_DCHECK(batch != nullptr, "Cannot find the ColumnarBatch with handle " + std::to_string(batchHandles[i]));
    numRows += batch->numRows();
    batches.emplace_back(batch);
  }
  env->ReleaseLongArrayElements(handles, batchHandles, JNI_ABORT);

  auto arrowPool = memoryManager->getArrowMemoryPool();
  auto serializer = ctx->createTempColumnarBatchSerializer(memoryManager, arrowPool, nullptr);
  auto buffer = serializer->serializeColumnarBatches(batches);
  auto bufferArr = env->NewByteArray(buffer->size());
  env->SetByteArrayRegion(bufferArr, 0, buffer->size(), reinterpret_cast<const jbyte*>(buffer->data()));

  jobject columnarBatchSerializeResult = env->NewObject(
      gluten::getJniCommonState()->columnarBatchSerializeResultClass,
      gluten::getJniCommonState()->columnarBatchSerializeResultConstructor,
      numRows,
      bufferArr);

  return columnarBatchSerializeResult;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ColumnarBatchSerializerJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  auto arrowPool = memoryManager->getArrowMemoryPool();
  return ctx->createColumnarBatchSerializer(memoryManager, arrowPool, reinterpret_cast<struct ArrowSchema*>(cSchema));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ColumnarBatchSerializerJniWrapper_deserialize( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong serializerHandle,
    jbyteArray data) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto serializer = ctx->getColumnarBatchSerializer(serializerHandle);
  GLUTEN_DCHECK(serializer != nullptr, "ColumnarBatchSerializer cannot be null");
  int32_t size = env->GetArrayLength(data);
  jbyte* serialized = env->GetByteArrayElements(data, nullptr);
  auto batch = serializer->deserialize(reinterpret_cast<uint8_t*>(serialized), size);
  env->ReleaseByteArrayElements(data, serialized, JNI_ABORT);
  return ctx->addBatch(batch);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ColumnarBatchSerializerJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong serializerHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  ctx->releaseColumnarBatchSerializer(serializerHandle);
  JNI_METHOD_END()
}
#ifdef __cplusplus
}
#endif
