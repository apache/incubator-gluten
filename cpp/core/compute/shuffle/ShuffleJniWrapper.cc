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

#include "compute/shuffle/LocalPartitionWriter.h"
#include "compute/shuffle/PartitionWriterCreator.h"
#include "compute/shuffle/ShuffleReader.h"
#include "compute/shuffle/ShuffleWriter.h"
#include "compute/shuffle/Utils.h"
#include "compute/shuffle/rss/CelebornPartitionWriter.h"
#include "utils/ArrowStatus.h"

#ifdef __cplusplus
extern "C" {
#endif

using namespace gluten;

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_nativeMake( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jstring partitioningNameJstr,
    jint numPartitions,
    jint bufferSize,
    jstring codecJstr,
    jstring codecBackendJstr,
    jint bufferCompressThreshold,
    jstring compressionModeJstr,
    jstring dataFileJstr,
    jint numSubDirs,
    jstring localDirsJstr,
    jlong memoryManagerHandle,
    jboolean writeEOS,
    jdouble reallocThreshold,
    jlong firstBatchHandle,
    jlong taskAttemptId,
    jint pushBufferMaxSize,
    jobject partitionPusher,
    jstring partitionWriterTypeJstr) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);
  if (partitioningNameJstr == nullptr) {
    throw gluten::GlutenException(std::string("Short partitioning name can't be null"));
    return kInvalidResourceHandle;
  }

  auto partitioningName = jStringToCString(env, partitioningNameJstr);

  auto shuffleWriterOptions = ShuffleWriterOptions::defaults();
  shuffleWriterOptions.partitioning_name = partitioningName;
  shuffleWriterOptions.buffered_write = true;
  if (bufferSize > 0) {
    shuffleWriterOptions.buffer_size = bufferSize;
  }

  if (codecJstr != NULL) {
    shuffleWriterOptions.compression_type = getCompressionType(env, codecJstr);
    shuffleWriterOptions.codec_backend = getCodecBackend(env, codecBackendJstr);
    shuffleWriterOptions.compression_mode = getCompressionMode(env, compressionModeJstr);
  }

  shuffleWriterOptions.memory_pool = memoryManager->getArrowMemoryPool();

  jclass cls = env->FindClass("java/lang/Thread");
  jmethodID mid = env->GetStaticMethodID(cls, "currentThread", "()Ljava/lang/Thread;");
  jobject thread = env->CallStaticObjectMethod(cls, mid);
  checkException(env);
  if (thread == NULL) {
    std::cerr << "Thread.currentThread() return NULL" << std::endl;
  } else {
    jmethodID midGetid = getMethodIdOrError(env, cls, "getId", "()J");
    jlong sid = env->CallLongMethod(thread, midGetid);
    checkException(env);
    shuffleWriterOptions.thread_id = (int64_t)sid;
  }

  shuffleWriterOptions.task_attempt_id = (int64_t)taskAttemptId;
  shuffleWriterOptions.buffer_compress_threshold = bufferCompressThreshold;

  auto partitionWriterTypeC = env->GetStringUTFChars(partitionWriterTypeJstr, JNI_FALSE);
  auto partitionWriterType = std::string(partitionWriterTypeC);
  env->ReleaseStringUTFChars(partitionWriterTypeJstr, partitionWriterTypeC);

  std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator;

  if (partitionWriterType == "local") {
    shuffleWriterOptions.partition_writer_type = kLocal;
    if (dataFileJstr == NULL) {
      throw gluten::GlutenException(std::string("Shuffle DataFile can't be null"));
    }
    if (localDirsJstr == NULL) {
      throw gluten::GlutenException(std::string("Shuffle DataFile can't be null"));
    }

    shuffleWriterOptions.write_eos = writeEOS;
    shuffleWriterOptions.buffer_realloc_threshold = reallocThreshold;

    if (numSubDirs > 0) {
      shuffleWriterOptions.num_sub_dirs = numSubDirs;
    }

    auto dataFileC = env->GetStringUTFChars(dataFileJstr, JNI_FALSE);
    shuffleWriterOptions.data_file = std::string(dataFileC);
    env->ReleaseStringUTFChars(dataFileJstr, dataFileC);

    auto localDirs = env->GetStringUTFChars(localDirsJstr, JNI_FALSE);
    setenv(gluten::kGlutenSparkLocalDirs.c_str(), localDirs, 1);
    env->ReleaseStringUTFChars(localDirsJstr, localDirs);
    partitionWriterCreator = std::make_shared<LocalPartitionWriterCreator>();
  } else if (partitionWriterType == "celeborn") {
    shuffleWriterOptions.partition_writer_type = PartitionWriterType::kCeleborn;
    jclass celebornPartitionPusherClass =
        createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
    jmethodID celebornPushPartitionDataMethod =
        getMethodIdOrError(env, celebornPartitionPusherClass, "pushPartitionData", "(I[BI)I");
    if (pushBufferMaxSize > 0) {
      shuffleWriterOptions.push_buffer_max_size = pushBufferMaxSize;
    }
    JavaVM* vm;
    if (env->GetJavaVM(&vm) != JNI_OK) {
      throw gluten::GlutenException("Unable to get JavaVM instance");
    }
    std::shared_ptr<CelebornClient> celebornClient =
        std::make_shared<CelebornClient>(vm, partitionPusher, celebornPushPartitionDataMethod);
    partitionWriterCreator = std::make_shared<CelebornPartitionWriterCreator>(std::move(celebornClient));
  } else {
    throw gluten::GlutenException("Unrecognizable partition writer type: " + partitionWriterType);
  }

  return ctx->createShuffleWriter(
      numPartitions, std::move(partitionWriterCreator), std::move(shuffleWriterOptions), memoryManager);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_nativeEvict( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle,
    jlong size,
    jboolean callBySelf) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto shuffleWriter = ctx->getShuffleWriter(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw gluten::GlutenException(errorMessage);
  }
  int64_t evictedSize;
  gluten::arrowAssertOkOrThrow(
      shuffleWriter->evictFixedSize(size, &evictedSize), "(shuffle) nativeEvict: evict failed");
  return (jlong)evictedSize;
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_split( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle,
    jint numRows,
    jlong batchHandle,
    jlong memLimit) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto shuffleWriter = ctx->getShuffleWriter(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw gluten::GlutenException(errorMessage);
  }

  // The column batch maybe VeloxColumnBatch or ArrowCStructColumnarBatch(FallbackRangeShuffleWriter)
  auto batch = ctx->getBatch(batchHandle);
  auto numBytes = batch->numBytes();
  gluten::arrowAssertOkOrThrow(shuffleWriter->split(batch, memLimit), "Native split: shuffle writer split failed");
  return numBytes;
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jobject JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_stop( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto shuffleWriter = ctx->getShuffleWriter(shuffleWriterHandle);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer handle " + std::to_string(shuffleWriterHandle);
    throw gluten::GlutenException(errorMessage);
  }

  gluten::arrowAssertOkOrThrow(shuffleWriter->stop(), "Native split: shuffle writer stop failed");

  const auto& partitionLengths = shuffleWriter->partitionLengths();
  auto partitionLengthArr = env->NewLongArray(partitionLengths.size());
  auto src = reinterpret_cast<const jlong*>(partitionLengths.data());
  env->SetLongArrayRegion(partitionLengthArr, 0, partitionLengths.size(), src);

  const auto& rawPartitionLengths = shuffleWriter->rawPartitionLengths();
  auto rawPartitionLengthArr = env->NewLongArray(rawPartitionLengths.size());
  auto rawSrc = reinterpret_cast<const jlong*>(rawPartitionLengths.data());
  env->SetLongArrayRegion(rawPartitionLengthArr, 0, rawPartitionLengths.size(), rawSrc);

  jobject splitResult = env->NewObject(
      gluten::getJniCommonState()->splitResultClass,
      gluten::getJniCommonState()->splitResultConstructor,
      0L,
      shuffleWriter->totalWriteTime(),
      shuffleWriter->totalEvictTime(),
      shuffleWriter->totalCompressTime(),
      shuffleWriter->totalBytesWritten(),
      shuffleWriter->totalBytesEvicted(),
      shuffleWriter->partitionBufferSize(),
      partitionLengthArr,
      rawPartitionLengthArr);

  return splitResult;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleWriterHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  ctx->releaseShuffleWriter(shuffleWriterHandle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_make( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong cSchema,
    jlong memoryManagerHandle,
    jstring compressionType,
    jstring compressionBackend,
    jstring compressionMode) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  auto pool = memoryManager->getArrowMemoryPool();
  ReaderOptions options = ReaderOptions::defaults();
  options.ipc_read_options.memory_pool = pool;
  options.ipc_read_options.use_threads = false;
  if (compressionType != nullptr) {
    options.compression_type = getCompressionType(env, compressionType);
    options.codec_backend = getCodecBackend(env, compressionBackend);
    options.compression_mode = getCompressionMode(env, compressionMode);
  }
  std::shared_ptr<arrow::Schema> schema =
      gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));

  return ctx->createShuffleReader(schema, options, pool, memoryManager);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_readStream( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle,
    jobject jniIn) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto reader = ctx->getShuffleReader(shuffleReaderHandle);
  std::shared_ptr<arrow::io::InputStream> in = std::make_shared<JavaInputStreamAdaptor>(env, reader->getPool(), jniIn);
  auto outItr = reader->readStream(in);
  return ctx->addResultIterator(outItr);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_populateMetrics( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle,
    jobject metrics) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto reader = ctx->getShuffleReader(shuffleReaderHandle);
  env->CallVoidMethod(
      metrics, gluten::getJniCommonState()->shuffleReaderMetricsSetDecompressTime, reader->getDecompressTime());
  env->CallVoidMethod(metrics, gluten::getJniCommonState()->shuffleReaderMetricsSetIpcTime, reader->getIpcTime());
  env->CallVoidMethod(
      metrics, gluten::getJniCommonState()->shuffleReaderMetricsSetDeserializeTime, reader->getDeserializeTime());

  checkException(env);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong shuffleReaderHandle) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto reader = ctx->getShuffleReader(shuffleReaderHandle);
  GLUTEN_THROW_NOT_OK(reader->close());
  ctx->releaseShuffleReader(shuffleReaderHandle);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
