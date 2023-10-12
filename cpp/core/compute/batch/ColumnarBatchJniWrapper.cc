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

JNIEXPORT jstring JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_getType( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto batch = ctx->getBatch(batchHandle);
  return env->NewStringUTF(batch->getType().c_str());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_numBytes( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto batch = ctx->getBatch(batchHandle);
  return batch->numBytes();
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_numColumns( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto batch = ctx->getBatch(batchHandle);
  return batch->numColumns();
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_numRows( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto batch = ctx->getBatch(batchHandle);
  return batch->numRows();
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_compose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlongArray batchHandles) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  int handleCount = env->GetArrayLength(batchHandles);
  jlong* handleArray = env->GetLongArrayElements(batchHandles, nullptr);

  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  for (int i = 0; i < handleCount; ++i) {
    jlong handle = handleArray[i];
    auto batch = ctx->getBatch(handle);
    batches.push_back(batch);
  }
  auto newBatch = CompositeColumnarBatch::create(std::move(batches));
  env->ReleaseLongArrayElements(batchHandles, handleArray, JNI_ABORT);
  return ctx->addBatch(newBatch);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_exportToArrow( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto batch = ctx->getBatch(batchHandle);
  std::shared_ptr<ArrowSchema> exportedSchema = batch->exportArrowSchema();
  std::shared_ptr<ArrowArray> exportedArray = batch->exportArrowArray();
  ArrowSchemaMove(exportedSchema.get(), reinterpret_cast<struct ArrowSchema*>(cSchema));
  ArrowArrayMove(exportedArray.get(), reinterpret_cast<struct ArrowArray*>(cArray));
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_createWithArrowArray( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  std::unique_ptr<ArrowSchema> targetSchema = std::make_unique<ArrowSchema>();
  std::unique_ptr<ArrowArray> targetArray = std::make_unique<ArrowArray>();
  auto* arrowSchema = reinterpret_cast<ArrowSchema*>(cSchema);
  auto* arrowArray = reinterpret_cast<ArrowArray*>(cArray);
  ArrowArrayMove(arrowArray, targetArray.get());
  ArrowSchemaMove(arrowSchema, targetSchema.get());
  std::shared_ptr<ColumnarBatch> batch =
      std::make_shared<ArrowCStructColumnarBatch>(std::move(targetSchema), std::move(targetArray));
  return ctx->addBatch(batch);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_getForEmptySchema( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jint numRows) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  return ctx->createOrGetEmptySchemaBatch(static_cast<int32_t>(numRows));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_select( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong memoryManagerHandle,
    jlong batchHandle,
    jintArray jcolumnIndices) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  int* tmp = env->GetIntArrayElements(jcolumnIndices, nullptr);
  int size = env->GetArrayLength(jcolumnIndices);
  std::vector<int32_t> columnIndices;
  for (int32_t i = 0; i < size; i++) {
    columnIndices.push_back(tmp[i]);
  }
  env->ReleaseIntArrayElements(jcolumnIndices, tmp, JNI_ABORT);

  return ctx->select(memoryManager, batchHandle, std::move(columnIndices));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  ctx->releaseBatch(batchHandle);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
