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

JNIEXPORT jboolean JNICALL Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeHasNext( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto iter = ctx->getResultIterator(iterHandle);
  if (iter == nullptr) {
    std::string errorMessage = "faked to get batch iterator";
    throw gluten::GlutenException(errorMessage);
  }
  return iter->hasNext();
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeNext( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto iter = ctx->getResultIterator(iterHandle);
  if (!iter->hasNext()) {
    return kInvalidResourceHandle;
  }

  std::shared_ptr<ColumnarBatch> batch = iter->next();
  auto batchHandle = ctx->addBatch(batch);

  iter->setExportNanos(batch->getExportNanos());
  return batchHandle;
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jobject JNICALL Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeFetchMetrics( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto iter = ctx->getResultIterator(iterHandle);
  auto metrics = iter->getMetrics();
  unsigned int numMetrics = 0;
  if (metrics) {
    numMetrics = metrics->numMetrics;
  }

  jlongArray longArray[Metrics::kNum];
  for (auto i = (int)Metrics::kBegin; i != (int)Metrics::kEnd; ++i) {
    longArray[i] = env->NewLongArray(numMetrics);
    if (metrics) {
      env->SetLongArrayRegion(longArray[i], 0, numMetrics, metrics->get((Metrics::TYPE)i));
    }
  }

  return env->NewObject(
      gluten::getJniCommonState()->metricsBuilderClass,
      gluten::getJniCommonState()->metricsBuilderConstructor,
      longArray[Metrics::kInputRows],
      longArray[Metrics::kInputVectors],
      longArray[Metrics::kInputBytes],
      longArray[Metrics::kRawInputRows],
      longArray[Metrics::kRawInputBytes],
      longArray[Metrics::kOutputRows],
      longArray[Metrics::kOutputVectors],
      longArray[Metrics::kOutputBytes],
      longArray[Metrics::kCpuCount],
      longArray[Metrics::kWallNanos],
      metrics ? metrics->veloxToArrow : -1,
      longArray[Metrics::kPeakMemoryBytes],
      longArray[Metrics::kNumMemoryAllocations],
      longArray[Metrics::kSpilledBytes],
      longArray[Metrics::kSpilledRows],
      longArray[Metrics::kSpilledPartitions],
      longArray[Metrics::kSpilledFiles],
      longArray[Metrics::kNumDynamicFiltersProduced],
      longArray[Metrics::kNumDynamicFiltersAccepted],
      longArray[Metrics::kNumReplacedWithDynamicFilterRows],
      longArray[Metrics::kFlushRowCount],
      longArray[Metrics::kScanTime],
      longArray[Metrics::kSkippedSplits],
      longArray[Metrics::kProcessedSplits],
      longArray[Metrics::kSkippedStrides],
      longArray[Metrics::kProcessedStrides]);

  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeSpill( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle,
    jlong size) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto it = ctx->getResultIterator(iterHandle);
  return it->spillFixedSize(size);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeClose( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  ctx->releaseResultIterator(iterHandle);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
