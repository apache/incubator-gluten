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
#include <filesystem>

#include <glog/logging.h>
#include "compute/ExecutionCtx.h"
#include "compute/ProtobufUtils.h"
#include "config/GlutenConfig.h"
#include "jni/JniCommon.h"
#include "jni/JniError.h"

#include "operators/writer/Datasource.h"

#include <arrow/c/bridge.h>
#include "memory/AllocationListener.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/PartitionWriterCreator.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/Utils.h"
#include "shuffle/rss/CelebornPartitionWriter.h"
#include "utils/ArrowStatus.h"

using namespace gluten;

static jclass serializableObjBuilderClass;

static jclass javaReservationListenerClass;

static jmethodID reserveMemoryMethod;
static jmethodID unreserveMemoryMethod;

static jclass byteArrayClass;

static jclass jniByteInputStreamClass;
static jmethodID jniByteInputStreamRead;
static jmethodID jniByteInputStreamTell;
static jmethodID jniByteInputStreamClose;

static jclass splitResultClass;
static jmethodID splitResultConstructor;

static jclass columnarBatchSerializeResultClass;
static jmethodID columnarBatchSerializeResultConstructor;

static jclass serializedColumnarBatchIteratorClass;
static jclass metricsBuilderClass;
static jmethodID metricsBuilderConstructor;

static jmethodID serializedColumnarBatchIteratorHasNext;
static jmethodID serializedColumnarBatchIteratorNext;

static jclass nativeColumnarToRowInfoClass;
static jmethodID nativeColumnarToRowInfoConstructor;

static jclass veloxColumnarBatchScannerClass;
static jmethodID veloxColumnarBatchScannerHasNext;
static jmethodID veloxColumnarBatchScannerNext;

static jclass shuffleReaderMetricsClass;
static jmethodID shuffleReaderMetricsSetDecompressTime;
static jmethodID shuffleReaderMetricsSetIpcTime;
static jmethodID shuffleReaderMetricsSetDeserializeTime;

class JavaInputStreamAdaptor final : public arrow::io::InputStream {
 public:
  JavaInputStreamAdaptor(JNIEnv* env, arrow::MemoryPool* pool, jobject jniIn) : pool_(pool) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string errorMessage = "Unable to get JavaVM instance";
      throw gluten::GlutenException(errorMessage);
    }
    jniIn_ = env->NewGlobalRef(jniIn);
  }

  ~JavaInputStreamAdaptor() override {
    try {
      auto status = JavaInputStreamAdaptor::Close();
      if (!status.ok()) {
        DEBUG_OUT << __func__ << " call JavaInputStreamAdaptor::Close() failed, status:" << status.ToString()
                  << std::endl;
      }
    } catch (std::exception& e) {
      DEBUG_OUT << __func__ << " call JavaInputStreamAdaptor::Close() got exception:" << e.what() << std::endl;
    }
  }

  // not thread safe
  arrow::Status Close() override {
    if (closed_) {
      return arrow::Status::OK();
    }
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->CallVoidMethod(jniIn_, jniByteInputStreamClose);
    checkException(env);
    env->DeleteGlobalRef(jniIn_);
    vm_->DetachCurrentThread();
    closed_ = true;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jlong told = env->CallLongMethod(jniIn_, jniByteInputStreamTell);
    checkException(env);
    return told;
  }

  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jlong read = env->CallLongMethod(jniIn_, jniByteInputStreamRead, reinterpret_cast<jlong>(out), nbytes);
    checkException(env);
    return read;
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    GLUTEN_ASSIGN_OR_THROW(auto buffer, arrow::AllocateResizableBuffer(nbytes, pool_))
    GLUTEN_ASSIGN_OR_THROW(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))
    GLUTEN_THROW_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    return std::move(buffer);
  }

 private:
  arrow::MemoryPool* pool_;
  JavaVM* vm_;
  jobject jniIn_;
  bool closed_ = false;
};

class JniColumnarBatchIterator : public ColumnarBatchIterator {
 public:
  explicit JniColumnarBatchIterator(
      JNIEnv* env,
      jobject jColumnarBatchItr,
      ExecutionCtx* executionCtx,
      std::shared_ptr<ArrowWriter> writer)
      : executionCtx_(executionCtx), writer_(writer) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string errorMessage = "Unable to get JavaVM instance";
      throw gluten::GlutenException(errorMessage);
    }
    jColumnarBatchItr_ = env->NewGlobalRef(jColumnarBatchItr);
  }

  // singleton
  JniColumnarBatchIterator(const JniColumnarBatchIterator&) = delete;
  JniColumnarBatchIterator(JniColumnarBatchIterator&&) = delete;
  JniColumnarBatchIterator& operator=(const JniColumnarBatchIterator&) = delete;
  JniColumnarBatchIterator& operator=(JniColumnarBatchIterator&&) = delete;

  virtual ~JniColumnarBatchIterator() {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->DeleteGlobalRef(jColumnarBatchItr_);
    vm_->DetachCurrentThread();
  }

  std::shared_ptr<ColumnarBatch> next() override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    if (!env->CallBooleanMethod(jColumnarBatchItr_, serializedColumnarBatchIteratorHasNext)) {
      checkException(env);
      return nullptr; // stream ended
    }

    checkException(env);
    jlong handle = env->CallLongMethod(jColumnarBatchItr_, serializedColumnarBatchIteratorNext);
    checkException(env);
    auto batch = executionCtx_->getBatch(handle);
    if (writer_ != nullptr) {
      // save snapshot of the batch to file
      std::shared_ptr<ArrowSchema> schema = batch->exportArrowSchema();
      std::shared_ptr<ArrowArray> array = batch->exportArrowArray();
      auto rb = gluten::arrowGetOrThrow(arrow::ImportRecordBatch(array.get(), schema.get()));
      GLUTEN_THROW_NOT_OK(writer_->initWriter(*(rb->schema().get())));
      GLUTEN_THROW_NOT_OK(writer_->writeInBatches(rb));
    }
    return batch;
  }

 private:
  JavaVM* vm_;
  jobject jColumnarBatchItr_;
  ExecutionCtx* executionCtx_;
  std::shared_ptr<ArrowWriter> writer_;
};

std::unique_ptr<JniColumnarBatchIterator> makeJniColumnarBatchIterator(
    JNIEnv* env,
    jobject jColumnarBatchItr,
    ExecutionCtx* executionCtx,
    std::shared_ptr<ArrowWriter> writer) {
  return std::make_unique<JniColumnarBatchIterator>(env, jColumnarBatchItr, executionCtx, writer);
}

template <typename T>
T* jniCastOrThrow(ResourceHandle handle) {
  auto instance = reinterpret_cast<T*>(handle);
  GLUTEN_CHECK(instance != nullptr, "FATAL: resource instance should not be null.");
  return instance;
}

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    return JNI_ERR;
  }
  gluten::getJniCommonState()->ensureInitialized(env);
  gluten::getJniErrorState()->ensureInitialized(env);

  serializableObjBuilderClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/NativeSerializableObject;");

  byteArrayClass = createGlobalClassReferenceOrError(env, "[B");

  jniByteInputStreamClass = createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/JniByteInputStream;");
  jniByteInputStreamRead = getMethodIdOrError(env, jniByteInputStreamClass, "read", "(JJ)J");
  jniByteInputStreamTell = getMethodIdOrError(env, jniByteInputStreamClass, "tell", "()J");
  jniByteInputStreamClose = getMethodIdOrError(env, jniByteInputStreamClass, "close", "()V");

  splitResultClass = createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/GlutenSplitResult;");
  splitResultConstructor = getMethodIdOrError(env, splitResultClass, "<init>", "(JJJJJJJ[J[J)V");

  columnarBatchSerializeResultClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/ColumnarBatchSerializeResult;");
  columnarBatchSerializeResultConstructor =
      getMethodIdOrError(env, columnarBatchSerializeResultClass, "<init>", "(J[B)V");

  metricsBuilderClass = createGlobalClassReferenceOrError(env, "Lio/glutenproject/metrics/Metrics;");

  metricsBuilderConstructor =
      getMethodIdOrError(env, metricsBuilderClass, "<init>", "([J[J[J[J[J[J[J[J[J[JJ[J[J[J[J[J[J[J[J[J[J[J[J[J[J[J)V");

  serializedColumnarBatchIteratorClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/ColumnarBatchInIterator;");

  serializedColumnarBatchIteratorHasNext =
      getMethodIdOrError(env, serializedColumnarBatchIteratorClass, "hasNext", "()Z");

  serializedColumnarBatchIteratorNext = getMethodIdOrError(env, serializedColumnarBatchIteratorClass, "next", "()J");

  nativeColumnarToRowInfoClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/NativeColumnarToRowInfo;");
  nativeColumnarToRowInfoConstructor = getMethodIdOrError(env, nativeColumnarToRowInfoClass, "<init>", "([I[IJ)V");

  javaReservationListenerClass = createGlobalClassReference(
      env,
      "Lio/glutenproject/memory/nmm/"
      "ReservationListener;");

  reserveMemoryMethod = getMethodIdOrError(env, javaReservationListenerClass, "reserve", "(J)J");
  unreserveMemoryMethod = getMethodIdOrError(env, javaReservationListenerClass, "unreserve", "(J)J");

  veloxColumnarBatchScannerClass =
      createGlobalClassReference(env, "Lorg/apache/spark/sql/execution/datasources/VeloxColumnarBatchIterator;");

  veloxColumnarBatchScannerHasNext = getMethodId(env, veloxColumnarBatchScannerClass, "hasNext", "()Z");

  veloxColumnarBatchScannerNext = getMethodId(env, veloxColumnarBatchScannerClass, "next", "()J");

  shuffleReaderMetricsClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/ShuffleReaderMetrics;");
  shuffleReaderMetricsSetDecompressTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDecompressTime", "(J)V");
  shuffleReaderMetricsSetIpcTime = getMethodIdOrError(env, shuffleReaderMetricsClass, "setIpcTime", "(J)V");
  shuffleReaderMetricsSetDeserializeTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDeserializeTime", "(J)V");

  return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);
  env->DeleteGlobalRef(serializableObjBuilderClass);
  env->DeleteGlobalRef(jniByteInputStreamClass);
  env->DeleteGlobalRef(splitResultClass);
  env->DeleteGlobalRef(columnarBatchSerializeResultClass);
  env->DeleteGlobalRef(serializedColumnarBatchIteratorClass);
  env->DeleteGlobalRef(nativeColumnarToRowInfoClass);
  env->DeleteGlobalRef(byteArrayClass);
  env->DeleteGlobalRef(veloxColumnarBatchScannerClass);
  env->DeleteGlobalRef(shuffleReaderMetricsClass);
  gluten::getJniErrorState()->close();
  gluten::getJniCommonState()->close();
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_exec_ExecutionCtxJniWrapper_createExecutionCtx( // NOLINT
    JNIEnv* env,
    jclass) {
  JNI_METHOD_START
  auto executionCtx = gluten::createExecutionCtx();
  return reinterpret_cast<jlong>(executionCtx);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_exec_ExecutionCtxJniWrapper_releaseExecutionCtx( // NOLINT
    JNIEnv* env,
    jclass,
    jlong ctxHandle) {
  JNI_METHOD_START
  auto executionCtx = jniCastOrThrow<ExecutionCtx>(ctxHandle);

  gluten::releaseExecutionCtx(executionCtx);
  JNI_METHOD_END()
}

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
    jstring spillDir,
    jbyteArray confArr) {
  JNI_METHOD_START

  auto ctx = gluten::getExecutionCtx(env, wrapper);
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  auto spillDirStr = jStringToCString(env, spillDir);

  auto planData = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArr, nullptr));
  auto planSize = env->GetArrayLength(planArr);

  ctx->parsePlan(planData, planSize, {stageId, partitionId, taskId});

  auto confs = getConfMap(env, confArr);

  // Handle the Java iters
  jsize itersLen = env->GetArrayLength(iterArr);
  std::vector<std::shared_ptr<ResultIterator>> inputIters;
  for (int idx = 0; idx < itersLen; idx++) {
    std::shared_ptr<ArrowWriter> writer = nullptr;
    if (saveInput) {
      auto dir = confs[kGlutenSaveDir];
      std::filesystem::path f{dir};
      if (!std::filesystem::exists(f)) {
        throw gluten::GlutenException("Save input path " + dir + " does not exists");
      }
      auto file = confs[kGlutenSaveDir] + "/input_" + std::to_string(taskId) + "_" + std::to_string(idx) + "_" +
          std::to_string(partitionId) + ".parquet";
      writer = std::make_shared<ArrowWriter>(file);
    }
    jobject iter = env->GetObjectArrayElement(iterArr, idx);
    auto arrayIter = makeJniColumnarBatchIterator(env, iter, ctx, writer);
    auto resultIter = std::make_shared<ResultIterator>(std::move(arrayIter));
    inputIters.push_back(std::move(resultIter));
  }

  return ctx->createResultIterator(memoryManager, spillDirStr, inputIters, confs);
  JNI_METHOD_END(kInvalidResourceHandle)
}

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
      metricsBuilderClass,
      metricsBuilderConstructor,
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

  jobject nativeColumnarToRowInfo =
      env->NewObject(nativeColumnarToRowInfoClass, nativeColumnarToRowInfoConstructor, offsetsArr, lengthsArr, address);
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

// Shuffle
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
      splitResultClass,
      splitResultConstructor,
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

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_OnHeapJniByteInputStream_memCopyFromHeap( // NOLINT
    JNIEnv* env,
    jobject,
    jbyteArray source,
    jlong destAddress,
    jint size) {
  JNI_METHOD_START
  jbyte* bytes = env->GetByteArrayElements(source, nullptr);
  std::memcpy(reinterpret_cast<void*>(destAddress), reinterpret_cast<const void*>(bytes), size);
  env->ReleaseByteArrayElements(source, bytes, JNI_ABORT);
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
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDecompressTime, reader->getDecompressTime());
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetIpcTime, reader->getIpcTime());
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDeserializeTime, reader->getDeserializeTime());

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

JNIEXPORT jlong JNICALL
Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_nativeInitDatasource( // NOLINT
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
    auto sparkOptions = gluten::getConfMap(env, options);
    auto sparkConf = ctx->getConfMap();
    sparkOptions.insert(sparkConf.begin(), sparkConf.end());
    auto schema = gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));
    handle = ctx->createDatasource(jStringToCString(env, filePath), memoryManager, schema);
    auto datasource = ctx->getDatasource(handle);
    datasource->init(sparkOptions);
  }

  return handle;
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_inspectSchema( // NOLINT
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

JNIEXPORT void JNICALL Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_close( // NOLINT
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

JNIEXPORT void JNICALL Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_write( // NOLINT
    JNIEnv* env,
    jobject wrapper,
    jlong dsHandle,
    jobject iter) {
  JNI_METHOD_START
  auto ctx = gluten::getExecutionCtx(env, wrapper);

  auto datasource = ctx->getDatasource(dsHandle);

  while (env->CallBooleanMethod(iter, veloxColumnarBatchScannerHasNext)) {
    checkException(env);
    jlong batchHandle = env->CallLongMethod(iter, veloxColumnarBatchScannerNext);
    checkException(env);
    auto batch = ctx->getBatch(batchHandle);
    datasource->write(batch);
    // fixme this skips the general Java side batch-closing routine
    ctx->releaseBatch(batchHandle);
  }
  checkException(env);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_getAllocator( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jTypeName) {
  JNI_METHOD_START
  std::string typeName = jStringToCString(env, jTypeName);
  std::shared_ptr<MemoryAllocator>* allocator = new std::shared_ptr<MemoryAllocator>;
  if (typeName == "DEFAULT") {
    *allocator = defaultMemoryAllocator();
  } else {
    delete allocator;
    allocator = nullptr;
    throw GlutenException("Unexpected allocator type name: " + typeName);
  }
  return reinterpret_cast<jlong>(allocator);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_releaseAllocator( // NOLINT
    JNIEnv* env,
    jclass,
    jlong allocatorId) {
  JNI_METHOD_START
  delete reinterpret_cast<std::shared_ptr<MemoryAllocator>*>(allocatorId);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_bytesAllocated( // NOLINT
    JNIEnv* env,
    jclass,
    jlong allocatorId) {
  JNI_METHOD_START
  auto* alloc = reinterpret_cast<std::shared_ptr<MemoryAllocator>*>(allocatorId);
  if (alloc == nullptr) {
    throw gluten::GlutenException("Memory allocator instance not found. It may not exist nor has been closed");
  }
  return (*alloc)->getBytes();
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_nmm_NativeMemoryManager_create( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jname,
    jlong allocatorId,
    jlong reservationBlockSize,
    jobject jlistener) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  auto allocator = reinterpret_cast<std::shared_ptr<MemoryAllocator>*>(allocatorId);
  if (allocator == nullptr) {
    throw gluten::GlutenException("Allocator does not exist or has been closed");
  }

  std::unique_ptr<AllocationListener> listener = std::make_unique<SparkAllocationListener>(
      vm, jlistener, reserveMemoryMethod, unreserveMemoryMethod, reservationBlockSize);

  if (gluten::backtrace_allocation) {
    listener = std::make_unique<BacktraceAllocationListener>(std::move(listener));
  }

  auto name = jStringToCString(env, jname);
  // TODO: move memory manager into ExecutionCtx then we can use more general ExecutionCtx.
  auto executionCtx = gluten::createExecutionCtx();
  auto manager = executionCtx->createMemoryManager(name, *allocator, std::move(listener));
  gluten::releaseExecutionCtx(executionCtx);
  return reinterpret_cast<jlong>(manager);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jbyteArray JNICALL Java_io_glutenproject_memory_nmm_NativeMemoryManager_collectMemoryUsage( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  const MemoryUsageStats& stats = memoryManager->collectMemoryUsageStats();
  auto size = stats.ByteSizeLong();
  jbyteArray out = env->NewByteArray(size);
  uint8_t buffer[size];
  GLUTEN_CHECK(
      stats.SerializeToArray(reinterpret_cast<void*>(buffer), size),
      "Serialization failed when collecting memory usage stats");
  env->SetByteArrayRegion(out, 0, size, reinterpret_cast<jbyte*>(buffer));
  return out;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_nmm_NativeMemoryManager_shrink( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerHandle,
    jlong size) {
  JNI_METHOD_START
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);
  return memoryManager->shrink(static_cast<int64_t>(size));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_memory_nmm_NativeMemoryManager_release( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);
  delete memoryManager;
  JNI_METHOD_END()
}

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

  jobject columnarBatchSerializeResult =
      env->NewObject(columnarBatchSerializeResultClass, columnarBatchSerializeResultConstructor, numRows, bufferArr);

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
