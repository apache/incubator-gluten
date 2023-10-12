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

#include <filesystem>

#include "jni/JniCommon.h"
#include "jni/JniError.h"

#ifdef __cplusplus
extern "C" {
#endif

using namespace gluten;

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_PlanEvaluatorJniWrapper_nativeCreateKernelWithIterator( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong memoryManagerHandle,
    jbyteArray planArr,
    jobjectArray iterArr,
    jint stageId,
    jint partitionId,
    jlong taskId,
    jboolean saveInput,
    jstring spillDir) {
  JNI_METHOD_START

  auto ctx = getExecutionCtx(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  auto spillDirStr = jStringToCString(env, spillDir);

  auto planData = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArr, nullptr));
  auto planSize = env->GetArrayLength(planArr);

  ctx->parsePlan(planData, planSize, {stageId, partitionId, taskId});
  auto& conf = ctx->getConfMap();

  // Handle the Java iters
  jsize itersLen = env->GetArrayLength(iterArr);
  std::vector<std::shared_ptr<ResultIterator>> inputIters;
  for (int idx = 0; idx < itersLen; idx++) {
    std::shared_ptr<ArrowWriter> writer = nullptr;
    if (saveInput) {
      auto dir = conf.at(kGlutenSaveDir);
      std::filesystem::path f{dir};
      if (!std::filesystem::exists(f)) {
        throw GlutenException("Save input path " + dir + " does not exists");
      }
      auto file = conf.at(kGlutenSaveDir) + "/input_" + std::to_string(taskId) + "_" + std::to_string(idx) + "_" +
          std::to_string(partitionId) + ".parquet";
      writer = std::make_shared<ArrowWriter>(file);
    }
    jobject iter = env->GetObjectArrayElement(iterArr, idx);
    auto arrayIter = JniColumnarBatchIterator::makeJniColumnarBatchIterator(env, iter, ctx, writer);
    auto resultIter = std::make_shared<ResultIterator>(std::move(arrayIter));
    inputIters.push_back(std::move(resultIter));
  }

  return ctx->createResultIterator(memoryManager, spillDirStr, inputIters, conf);
  JNI_METHOD_END(kInvalidResourceHandle)
}

#ifdef __cplusplus
}
#endif
