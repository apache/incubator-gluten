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

JNIEXPORT jlong JNICALL Java_io_glutenproject_datasource_velox_DatasourceJniWrapper_nativeInitDatasource( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jstring filePath,
    jlong cSchema,
    jlong memoryManagerHandle,
    jbyteArray options) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  ResourceHandle handle = kInvalidResourceHandle;

  if (cSchema == -1) {
    // Only inspect the schema and not write
    handle = ctx->createDatasource(jStringToCString(env, filePath), memoryManager, nullptr);
  } else {
    auto datasourceOptions = gluten::getConfMap(env, options);
    auto& sparkConf = ctx->getConfMap();
    datasourceOptions.insert(sparkConf.begin(), sparkConf.end());
    auto schema = gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));
    handle = ctx->createDatasource(jStringToCString(env, filePath), memoryManager, schema);
    auto datasource = ctx->getDatasource(handle);
    datasource->init(datasourceOptions);
  }

  return handle;
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_datasource_velox_DatasourceJniWrapper_inspectSchema( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jlong cSchema) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto datasource = ctx->getDatasource(dsHandle);
  datasource->inspectSchema(reinterpret_cast<struct ArrowSchema*>(cSchema));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_datasource_velox_DatasourceJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto datasource = ctx->getDatasource(dsHandle);
  datasource->close();
  ctx->releaseDatasource(dsHandle);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_datasource_velox_DatasourceJniWrapper_write( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jobject jIter) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto datasource = ctx->getDatasource(dsHandle);
  auto iter = JniColumnarBatchIterator::makeJniColumnarBatchIterator(env, jIter, ctx, nullptr);
  while (true) {
    auto batch = iter->next();
    if (!batch) {
      break;
    }
    datasource->write(batch);
  }
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_datasource_velox_DatasourceJniWrapper_splitBlockByPartitionAndBucket( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong batchAddress,
    jintArray partitionColIndice,
    jboolean hasBucket,
    jlong memoryManagerId) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto batch = ctx->getBatch(batchAddress);
  int* pIndice = env->GetIntArrayElements(partitionColIndice, nullptr);
  int size = env->GetArrayLength(partitionColIndice);
  std::vector<size_t> partitionColIndiceVec;
  for (int i = 0; i < size; ++i) {
    partitionColIndiceVec.push_back(pIndice[i]);
  }
  env->ReleaseIntArrayElements(partitionColIndice, pIndice, JNI_ABORT);

  size = ctx->getColumnarBatchPerRowSize(batch);
  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  char* rowBytes = new char[size];
  auto newBatch = ctx->getNonPartitionedColumnarBatch(batch, partitionColIndiceVec, memoryManager, rowBytes);
  auto batchHandler = ctx->addBatch(newBatch);

  jbyteArray bytesArray = env->NewByteArray(size);
  env->SetByteArrayRegion(bytesArray, 0, size, reinterpret_cast<jbyte*>(rowBytes));
  delete[] rowBytes;

  jlongArray batchArray = env->NewLongArray(1);
  long* cBatchArray = new long[1];
  cBatchArray[0] = batchHandler;
  env->SetLongArrayRegion(batchArray, 0, 1, cBatchArray);
  delete[] cBatchArray;

  jobject block_stripes = env->NewObject(
      gluten::getJniCommonState()->blockStripesClass,
      gluten::getJniCommonState()->blockStripesConstructor,
      batchAddress,
      batchArray,
      nullptr,
      batch->numColumns(),
      bytesArray);
  return block_stripes;
  JNI_METHOD_END(nullptr)
}

#ifdef __cplusplus
}
#endif
