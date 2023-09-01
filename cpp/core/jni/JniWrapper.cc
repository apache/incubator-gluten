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
#include "compute/Backend.h"
#include "compute/ProtobufUtils.h"
#include "config/GlutenConfig.h"
#include "jni/ConcurrentMap.h"
#include "jni/JniCommon.h"
#include "jni/JniErrors.h"

#include "operators/writer/Datasource.h"

#include <arrow/c/bridge.h>
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/PartitionWriterCreator.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/reader.h"
#include "shuffle/rss/CelebornPartitionWriter.h"
#include "shuffle/utils.h"
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

static jclass veloxColumnarbatchScannerClass;
static jmethodID veloxColumnarbatchScannerHasNext;
static jmethodID veloxColumnarbatchScannerNext;

static jclass shuffleReaderMetricsClass;
static jmethodID shuffleReaderMetricsSetDecompressTime;

static ConcurrentMap<std::shared_ptr<ColumnarToRowConverter>> columnarToRowConverterHolder;

static ConcurrentMap<std::shared_ptr<RowToColumnarConverter>> rowToColumnarConverterHolder;

static ConcurrentMap<std::shared_ptr<ResultIterator>> resultIteratorHolder;

static ConcurrentMap<std::shared_ptr<ShuffleWriter>> shuffleWriterHolder;

static ConcurrentMap<std::shared_ptr<Reader>> shuffleReaderHolder;

static ConcurrentMap<std::shared_ptr<ColumnarBatch>> columnarBatchHolder;

static ConcurrentMap<std::shared_ptr<Datasource>> glutenDatasourceHolder;

static ConcurrentMap<std::shared_ptr<ColumnarBatchSerializer>> columnarBatchSerializerHolder;

std::shared_ptr<ResultIterator> getArrayIterator(JNIEnv* env, jlong id) {
  auto handler = resultIteratorHolder.lookup(id);
  if (!handler) {
    std::string errorMessage = "invalid handler id " + std::to_string(id);
    throw gluten::GlutenException(errorMessage);
  }
  return handler;
}

class JavaInputStreamAdaptor final : public arrow::io::InputStream {
 public:
  JavaInputStreamAdaptor(JNIEnv* env, std::shared_ptr<arrow::MemoryPool> pool, jobject jniIn) : pool_(pool) {
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
#ifdef GLUTEN_PRINT_DEBUG
        std::cout << __func__ << " call JavaInputStreamAdaptor::Close() failed, status:" << status.ToString()
                  << std::endl;
#endif
      }
    } catch (std::exception& e) {
#ifdef GLUTEN_PRINT_DEBUG
      std::cout << __func__ << " call JavaInputStreamAdaptor::Close() got exception:" << e.what() << std::endl;
#endif
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
    return env->CallLongMethod(jniIn_, jniByteInputStreamTell);
  }

  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    return env->CallLongMethod(jniIn_, jniByteInputStreamRead, reinterpret_cast<jlong>(out), nbytes);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    GLUTEN_ASSIGN_OR_THROW(auto buffer, arrow::AllocateResizableBuffer(nbytes, pool_.get()))
    GLUTEN_ASSIGN_OR_THROW(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))
    GLUTEN_THROW_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    return std::move(buffer);
  }

 private:
  std::shared_ptr<arrow::MemoryPool> pool_;
  JavaVM* vm_;
  jobject jniIn_;
  bool closed_ = false;
};

class JniColumnarBatchIterator : public ColumnarBatchIterator {
 public:
  explicit JniColumnarBatchIterator(JNIEnv* env, jobject jColumnarBatchItr, std::shared_ptr<ArrowWriter> writer)
      : writer_(writer) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string errorMessage = "Unable to get JavaVM instance";
      throw gluten::GlutenException(errorMessage);
    }
    jColumnarBatchItr_ = env->NewGlobalRef(jColumnarBatchItr);
  }

  // singleton, avoid stack instantiation
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
    auto batch = columnarBatchHolder.lookup(handle);
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
  std::shared_ptr<ArrowWriter> writer_;
};

std::unique_ptr<JniColumnarBatchIterator>
makeJniColumnarBatchIterator(JNIEnv* env, jobject jColumnarBatchItr, std::shared_ptr<ArrowWriter> writer) {
  return std::make_unique<JniColumnarBatchIterator>(env, jColumnarBatchItr, writer);
}

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    return JNI_ERR;
  }
  gluten::getJniErrorsState()->initialize(env);

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

  veloxColumnarbatchScannerClass =
      createGlobalClassReference(env, "Lorg/apache/spark/sql/execution/datasources/VeloxColumnarBatchIterator;");

  veloxColumnarbatchScannerHasNext = getMethodId(env, veloxColumnarbatchScannerClass, "hasNext", "()Z");

  veloxColumnarbatchScannerNext = getMethodId(env, veloxColumnarbatchScannerClass, "next", "()J");

  shuffleReaderMetricsClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/ShuffleReaderMetrics;");
  shuffleReaderMetricsSetDecompressTime =
      getMethodIdOrError(env, shuffleReaderMetricsClass, "setDecompressTime", "(J)V");

  return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  resultIteratorHolder.clear();
  columnarToRowConverterHolder.clear();
  shuffleWriterHolder.clear();
  shuffleReaderHolder.clear();
  glutenDatasourceHolder.clear();

  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);
  env->DeleteGlobalRef(serializableObjBuilderClass);
  env->DeleteGlobalRef(jniByteInputStreamClass);
  env->DeleteGlobalRef(splitResultClass);
  env->DeleteGlobalRef(columnarBatchSerializeResultClass);
  env->DeleteGlobalRef(serializedColumnarBatchIteratorClass);
  env->DeleteGlobalRef(nativeColumnarToRowInfoClass);
  env->DeleteGlobalRef(byteArrayClass);
  env->DeleteGlobalRef(veloxColumnarbatchScannerClass);
  env->DeleteGlobalRef(shuffleReaderMetricsClass);
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_PlanEvaluatorJniWrapper_nativeCreateKernelWithIterator( // NOLINT
    JNIEnv* env,
    jobject obj,
    jlong memoryManagerId,
    jbyteArray planArr,
    jobjectArray iterArr,
    jint stageId,
    jint partitionId,
    jlong taskId,
    jboolean saveInput,
    jstring spillDir,
    jbyteArray confArr) {
  JNI_METHOD_START
  arrow::Status msg;

  auto spillDirStr = jStringToCString(env, spillDir);

  auto planData = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArr, nullptr));
  auto planSize = env->GetArrayLength(planArr);

  auto backend = gluten::createBackend();
  backend->parsePlan(planData, planSize, {stageId, partitionId, taskId});

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
    auto arrayIter = makeJniColumnarBatchIterator(env, iter, writer);
    auto resultIter = std::make_shared<ResultIterator>(std::move(arrayIter));
    inputIters.push_back(std::move(resultIter));
  }

  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  GLUTEN_CHECK(memoryManager != nullptr, "MemoryManager should not be null.");
  auto resIter = backend->getResultIterator(memoryManager, spillDirStr, inputIters, confs);
  return resultIteratorHolder.insert(std::move(resIter));
  JNI_METHOD_END(-1)
}

JNIEXPORT jboolean JNICALL
Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeHasNext(JNIEnv* env, jobject obj, jlong id) { // NOLINT
  JNI_METHOD_START
  auto iter = getArrayIterator(env, id);
  if (iter == nullptr) {
    std::string errorMessage = "faked to get batch iterator";
    throw gluten::GlutenException(errorMessage);
  }
  return iter->hasNext();
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeNext(JNIEnv* env, jobject obj, jlong id) { // NOLINT
  JNI_METHOD_START
  auto iter = getArrayIterator(env, id);
  if (!iter->hasNext()) {
    return -1L;
  }

  std::shared_ptr<ColumnarBatch> batch = iter->next();
  jlong batchHandle = columnarBatchHolder.insert(batch);

  iter->setExportNanos(batch->getExportNanos());
  return batchHandle;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeFetchMetrics( // NOLINT
    JNIEnv* env,
    jobject obj,
    jlong id) {
  JNI_METHOD_START
  auto iter = getArrayIterator(env, id);
  std::shared_ptr<Metrics> metrics = iter->getMetrics();

  int numMetrics = 0;
  if (metrics) {
    numMetrics = metrics->numMetrics;
  }
  auto inputRows = env->NewLongArray(numMetrics);
  auto inputVectors = env->NewLongArray(numMetrics);
  auto inputBytes = env->NewLongArray(numMetrics);
  auto rawInputRows = env->NewLongArray(numMetrics);
  auto rawInputBytes = env->NewLongArray(numMetrics);
  auto outputRows = env->NewLongArray(numMetrics);
  auto outputVectors = env->NewLongArray(numMetrics);
  auto outputBytes = env->NewLongArray(numMetrics);
  auto cpuCount = env->NewLongArray(numMetrics);
  auto wallNanos = env->NewLongArray(numMetrics);
  auto peakMemoryBytes = env->NewLongArray(numMetrics);
  auto numMemoryAllocations = env->NewLongArray(numMetrics);
  auto spilledBytes = env->NewLongArray(numMetrics);
  auto spilledRows = env->NewLongArray(numMetrics);
  auto spilledPartitions = env->NewLongArray(numMetrics);
  auto spilledFiles = env->NewLongArray(numMetrics);
  auto numDynamicFiltersProduced = env->NewLongArray(numMetrics);
  auto numDynamicFiltersAccepted = env->NewLongArray(numMetrics);
  auto numReplacedWithDynamicFilterRows = env->NewLongArray(numMetrics);
  auto flushRowCount = env->NewLongArray(numMetrics);
  auto scanTime = env->NewLongArray(numMetrics);
  auto skippedSplits = env->NewLongArray(numMetrics);
  auto processedSplits = env->NewLongArray(numMetrics);
  auto skippedStrides = env->NewLongArray(numMetrics);
  auto processedStrides = env->NewLongArray(numMetrics);

  if (metrics) {
    env->SetLongArrayRegion(inputRows, 0, numMetrics, metrics->inputRows);
    env->SetLongArrayRegion(inputVectors, 0, numMetrics, metrics->inputVectors);
    env->SetLongArrayRegion(inputBytes, 0, numMetrics, metrics->inputBytes);
    env->SetLongArrayRegion(rawInputRows, 0, numMetrics, metrics->rawInputRows);
    env->SetLongArrayRegion(rawInputBytes, 0, numMetrics, metrics->rawInputBytes);
    env->SetLongArrayRegion(outputRows, 0, numMetrics, metrics->outputRows);
    env->SetLongArrayRegion(outputVectors, 0, numMetrics, metrics->outputVectors);
    env->SetLongArrayRegion(outputBytes, 0, numMetrics, metrics->outputBytes);
    env->SetLongArrayRegion(cpuCount, 0, numMetrics, metrics->cpuCount);
    env->SetLongArrayRegion(wallNanos, 0, numMetrics, metrics->wallNanos);
    env->SetLongArrayRegion(peakMemoryBytes, 0, numMetrics, metrics->peakMemoryBytes);
    env->SetLongArrayRegion(numMemoryAllocations, 0, numMetrics, metrics->numMemoryAllocations);
    env->SetLongArrayRegion(spilledBytes, 0, numMetrics, metrics->spilledBytes);
    env->SetLongArrayRegion(spilledRows, 0, numMetrics, metrics->spilledRows);
    env->SetLongArrayRegion(spilledPartitions, 0, numMetrics, metrics->spilledPartitions);
    env->SetLongArrayRegion(spilledFiles, 0, numMetrics, metrics->spilledFiles);
    env->SetLongArrayRegion(numDynamicFiltersProduced, 0, numMetrics, metrics->numDynamicFiltersProduced);
    env->SetLongArrayRegion(numDynamicFiltersAccepted, 0, numMetrics, metrics->numDynamicFiltersAccepted);
    env->SetLongArrayRegion(numReplacedWithDynamicFilterRows, 0, numMetrics, metrics->numReplacedWithDynamicFilterRows);
    env->SetLongArrayRegion(flushRowCount, 0, numMetrics, metrics->flushRowCount);
    env->SetLongArrayRegion(scanTime, 0, numMetrics, metrics->scanTime);
    env->SetLongArrayRegion(skippedSplits, 0, numMetrics, metrics->skippedSplits);
    env->SetLongArrayRegion(processedSplits, 0, numMetrics, metrics->processedSplits);
    env->SetLongArrayRegion(skippedStrides, 0, numMetrics, metrics->skippedStrides);
    env->SetLongArrayRegion(processedStrides, 0, numMetrics, metrics->processedStrides);
  }

  return env->NewObject(
      metricsBuilderClass,
      metricsBuilderConstructor,
      inputRows,
      inputVectors,
      inputBytes,
      rawInputRows,
      rawInputBytes,
      outputRows,
      outputVectors,
      outputBytes,
      cpuCount,
      wallNanos,
      metrics ? metrics->veloxToArrow : -1,
      peakMemoryBytes,
      numMemoryAllocations,
      spilledBytes,
      spilledRows,
      spilledPartitions,
      spilledFiles,
      numDynamicFiltersProduced,
      numDynamicFiltersAccepted,
      numReplacedWithDynamicFilterRows,
      flushRowCount,
      scanTime,
      skippedSplits,
      processedSplits,
      skippedStrides,
      processedStrides);
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeSpill( // NOLINT
    JNIEnv* env,
    jobject thisObj,
    jlong id,
    jlong size) {
  JNI_METHOD_START
  auto it = resultIteratorHolder.lookup(id);
  return it->spillFixedSize(size);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeClose( // NOLINT
    JNIEnv* env,
    jobject thisObj,
    jlong id) {
  JNI_METHOD_START
#ifdef GLUTEN_PRINT_DEBUG
  auto it = resultIteratorHolder.lookup(id);
  if (it.use_count() > 2) {
    std::cout << "ArrowArrayResultIterator Id " << id << " use count is " << it.use_count() << std::endl;
  }
#endif
  resultIteratorHolder.erase(id);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeColumnarToRowInit( // NOLINT
    JNIEnv* env,
    jobject,
    jlong memoryManagerId) {
  JNI_METHOD_START
  // Convert the native batch to Spark unsafe row.
  auto backend = gluten::createBackend();
  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  GLUTEN_CHECK(memoryManager != nullptr, "MemoryManager should not be null.");
  auto columnarToRowConverter = backend->getColumnar2RowConverter(memoryManager);
  int64_t instanceID = columnarToRowConverterHolder.insert(columnarToRowConverter);
  return instanceID;
  JNI_METHOD_END(-1)
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeColumnarToRowConvert( // NOLINT
    JNIEnv* env,
    jobject,
    jlong batchHandle,
    jlong instanceId) {
  JNI_METHOD_START
  auto columnarToRowConverter = columnarToRowConverterHolder.lookup(instanceId);
  std::shared_ptr<ColumnarBatch> cb = columnarBatchHolder.lookup(batchHandle);
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
    jobject,
    jlong instanceId) {
  JNI_METHOD_START
  columnarToRowConverterHolder.erase(instanceId);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_NativeRowToColumnarJniWrapper_init( // NOLINT
    JNIEnv* env,
    jobject,
    jlong cSchema,
    jlong memoryManagerId) {
  JNI_METHOD_START
  auto backend = gluten::createBackend();
  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  GLUTEN_CHECK(memoryManager != nullptr, "MemoryManager should not be null.");
  auto converter = backend->getRowToColumnarConverter(memoryManager, reinterpret_cast<struct ArrowSchema*>(cSchema));
  return rowToColumnarConverterHolder.insert(converter);
  JNI_METHOD_END(-1)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_NativeRowToColumnarJniWrapper_nativeConvertRowToColumnar( // NOLINT
    JNIEnv* env,
    jobject,
    jlong r2cId,
    jlongArray rowLength,
    jlong memoryAddress) {
  JNI_METHOD_START
  if (rowLength == nullptr) {
    throw gluten::GlutenException("Native convert row to columnar: buf_addrs can't be null");
  }
  int numRows = env->GetArrayLength(rowLength);
  jlong* inRowLength = env->GetLongArrayElements(rowLength, nullptr);
  uint8_t* address = reinterpret_cast<uint8_t*>(memoryAddress);

  auto converter = rowToColumnarConverterHolder.lookup(r2cId);
  auto cb = converter->convert(numRows, reinterpret_cast<int64_t*>(inRowLength), address);
  env->ReleaseLongArrayElements(rowLength, inRowLength, JNI_ABORT);
  return columnarBatchHolder.insert(cb);
  JNI_METHOD_END(-1)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_NativeRowToColumnarJniWrapper_close(JNIEnv* env, jobject, jlong r2cId) { // NOLINT
  JNI_METHOD_START
  rowToColumnarConverterHolder.erase(r2cId);
  JNI_METHOD_END()
}

JNIEXPORT jstring JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_getType(JNIEnv* env, jobject, jlong handle) { // NOLINT
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = columnarBatchHolder.lookup(handle);
  return env->NewStringUTF(batch->getType().c_str());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_numBytes(JNIEnv* env, jobject, jlong handle) { // NOLINT
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = columnarBatchHolder.lookup(handle);
  return batch->numBytes();
  JNI_METHOD_END(-1)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_numColumns( // NOLINT
    JNIEnv* env,
    jobject,
    jlong handle) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = columnarBatchHolder.lookup(handle);
  return batch->numColumns();
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_numRows(JNIEnv* env, jobject, jlong handle) { // NOLINT
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = columnarBatchHolder.lookup(handle);
  return batch->numRows();
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_compose( // NOLINT
    JNIEnv* env,
    jobject,
    jlongArray handles) {
  JNI_METHOD_START
  int handleCount = env->GetArrayLength(handles);
  jlong* handleArray = env->GetLongArrayElements(handles, nullptr);

  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  for (int i = 0; i < handleCount; ++i) {
    jlong handle = handleArray[i];
    std::shared_ptr<ColumnarBatch> batch = columnarBatchHolder.lookup(handle);
    batches.push_back(batch);
  }
  auto newBatch = CompositeColumnarBatch::create(std::move(batches));
  env->ReleaseLongArrayElements(handles, handleArray, JNI_ABORT);
  return columnarBatchHolder.insert(newBatch);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_exportToArrow( // NOLINT
    JNIEnv* env,
    jobject,
    jlong handle,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = columnarBatchHolder.lookup(handle);
  std::shared_ptr<ArrowSchema> exportedSchema = batch->exportArrowSchema();
  std::shared_ptr<ArrowArray> exportedArray = batch->exportArrowArray();
  ArrowSchemaMove(exportedSchema.get(), reinterpret_cast<struct ArrowSchema*>(cSchema));
  ArrowArrayMove(exportedArray.get(), reinterpret_cast<struct ArrowArray*>(cArray));
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_createWithArrowArray( // NOLINT
    JNIEnv* env,
    jobject,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  std::unique_ptr<ArrowSchema> targetSchema = std::make_unique<ArrowSchema>();
  std::unique_ptr<ArrowArray> targetArray = std::make_unique<ArrowArray>();
  auto* arrowSchema = reinterpret_cast<ArrowSchema*>(cSchema);
  auto* arrowArray = reinterpret_cast<ArrowArray*>(cArray);
  ArrowArrayMove(arrowArray, targetArray.get());
  ArrowSchemaMove(arrowSchema, targetSchema.get());
  std::shared_ptr<ColumnarBatch> batch =
      std::make_shared<ArrowCStructColumnarBatch>(std::move(targetSchema), std::move(targetArray));
  return columnarBatchHolder.insert(batch);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_close(JNIEnv* env, jobject, jlong handle) { // NOLINT
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = columnarBatchHolder.lookup(handle);
  columnarBatchHolder.erase(handle);
  JNI_METHOD_END()
}

// Shuffle
JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_nativeMake( // NOLINT
    JNIEnv* env,
    jobject,
    jstring partitioningNameJstr,
    jint numPartitions,
    jlong offheapPerTask,
    jint bufferSize,
    jstring codecJstr,
    jstring codecBackendJstr,
    jint bufferCompressThreshold,
    jstring compressionModeJstr,
    jstring dataFileJstr,
    jint numSubDirs,
    jstring localDirsJstr,
    jboolean preferEvict,
    jlong memoryManagerId,
    jboolean writeEOS,
    jlong firstBatchHandle,
    jlong taskAttemptId,
    jint pushBufferMaxSize,
    jobject partitionPusher,
    jstring partitionWriterTypeJstr) {
  JNI_METHOD_START
  if (partitioningNameJstr == nullptr) {
    throw gluten::GlutenException(std::string("Short partitioning name can't be null"));
    return 0;
  }

  auto partitioningName = jStringToCString(env, partitioningNameJstr);

  auto shuffleWriterOptions = ShuffleWriterOptions::defaults();
  shuffleWriterOptions.partitioning_name = partitioningName;
  shuffleWriterOptions.buffered_write = true;
  if (bufferSize > 0) {
    shuffleWriterOptions.buffer_size = bufferSize;
  }
  shuffleWriterOptions.offheap_per_task = offheapPerTask;

  if (codecJstr != NULL) {
    shuffleWriterOptions.compression_type = getCompressionType(env, codecJstr);
    shuffleWriterOptions.codec_backend = getCodecBackend(env, codecBackendJstr);
    shuffleWriterOptions.compression_mode = getCompressionMode(env, compressionModeJstr);
  }

  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  GLUTEN_CHECK(memoryManager != nullptr, "MemoryManager should not be null.");
  shuffleWriterOptions.memory_pool = memoryManager->getArrowMemoryPool();
  shuffleWriterOptions.ipc_memory_pool = shuffleWriterOptions.memory_pool;

  jclass cls = env->FindClass("java/lang/Thread");
  jmethodID mid = env->GetStaticMethodID(cls, "currentThread", "()Ljava/lang/Thread;");
  jobject thread = env->CallStaticObjectMethod(cls, mid);
  if (thread == NULL) {
    std::cerr << "Thread.currentThread() return NULL" << std::endl;
  } else {
    jmethodID midGetid = getMethodIdOrError(env, cls, "getId", "()J");
    jlong sid = env->CallLongMethod(thread, midGetid);
    shuffleWriterOptions.thread_id = (int64_t)sid;
  }

  shuffleWriterOptions.task_attempt_id = (int64_t)taskAttemptId;
  shuffleWriterOptions.buffer_compress_threshold = bufferCompressThreshold;

  auto partitionWriterTypeC = env->GetStringUTFChars(partitionWriterTypeJstr, JNI_FALSE);
  auto partitionWriterType = std::string(partitionWriterTypeC);
  env->ReleaseStringUTFChars(partitionWriterTypeJstr, partitionWriterTypeC);

  std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator;

  if (partitionWriterType == "local") {
    shuffleWriterOptions.partition_writer_type = "local";
    if (dataFileJstr == NULL) {
      throw gluten::GlutenException(std::string("Shuffle DataFile can't be null"));
    }
    if (localDirsJstr == NULL) {
      throw gluten::GlutenException(std::string("Shuffle DataFile can't be null"));
    }

    shuffleWriterOptions.write_eos = writeEOS;
    shuffleWriterOptions.prefer_evict = preferEvict;

    if (numSubDirs > 0) {
      shuffleWriterOptions.num_sub_dirs = numSubDirs;
    }

    auto dataFileC = env->GetStringUTFChars(dataFileJstr, JNI_FALSE);
    shuffleWriterOptions.data_file = std::string(dataFileC);
    env->ReleaseStringUTFChars(dataFileJstr, dataFileC);

    auto localDirs = env->GetStringUTFChars(localDirsJstr, JNI_FALSE);
    setenv(gluten::kGlutenSparkLocalDirs.c_str(), localDirs, 1);
    env->ReleaseStringUTFChars(localDirsJstr, localDirs);
    partitionWriterCreator = std::make_shared<LocalPartitionWriterCreator>(preferEvict);
  } else if (partitionWriterType == "celeborn") {
    shuffleWriterOptions.partition_writer_type = "celeborn";
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

  auto backend = gluten::createBackend();
  auto shuffleWriter = backend->makeShuffleWriter(
      numPartitions, std::move(partitionWriterCreator), std::move(shuffleWriterOptions), memoryManager);
  return shuffleWriterHolder.insert(shuffleWriter);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_nativeEvict( // NOLINT
    JNIEnv* env,
    jobject,
    jlong shuffleWriterId,
    jlong size,
    jboolean callBySelf) {
  JNI_METHOD_START
  auto shuffleWriter = shuffleWriterHolder.lookup(shuffleWriterId);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer id " + std::to_string(shuffleWriterId);
    throw gluten::GlutenException(errorMessage);
  }
  int64_t evictedSize;
  gluten::arrowAssertOkOrThrow(
      shuffleWriter->evictFixedSize(size, &evictedSize), "(shuffle) nativeEvict: evict failed");
  return (jlong)evictedSize;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_split( // NOLINT
    JNIEnv* env,
    jobject,
    jlong shuffleWriterId,
    jint numRows,
    jlong handle) {
  JNI_METHOD_START
  auto shuffleWriter = shuffleWriterHolder.lookup(shuffleWriterId);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer id " + std::to_string(shuffleWriterId);
    throw gluten::GlutenException(errorMessage);
  }

  // The column batch maybe VeloxColumnBatch or ArrowCStructColumnarBatch(FallbackRangeShuffleWriter)
  std::shared_ptr<ColumnarBatch> batch = columnarBatchHolder.lookup(handle);
  auto numBytes = batch->numBytes();
  gluten::arrowAssertOkOrThrow(shuffleWriter->split(batch), "Native split: shuffle writer split failed");
  return numBytes;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_stop(JNIEnv* env, jobject, jlong shuffleWriterId) { // NOLINT
  JNI_METHOD_START
  auto shuffleWriter = shuffleWriterHolder.lookup(shuffleWriterId);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer id " + std::to_string(shuffleWriterId);
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
      shuffleWriter->splitBufferSize(),
      partitionLengthArr,
      rawPartitionLengthArr);

  return splitResult;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_close(JNIEnv* env, jobject, jlong shuffleWriterId) { // NOLINT
  JNI_METHOD_START
  shuffleWriterHolder.erase(shuffleWriterId);
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
    jobject,
    jlong cSchema,
    jlong memoryManagerId,
    jstring compressionType,
    jstring compressionBackend,
    jstring compressionMode) {
  JNI_METHOD_START
  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  GLUTEN_CHECK(memoryManager != nullptr, "MemoryManager should not be null.");
  auto pool = memoryManager->getArrowMemoryPool();
  ReaderOptions options = ReaderOptions::defaults();
  options.ipc_read_options.memory_pool = pool.get();
  options.ipc_read_options.use_threads = false;
  if (compressionType != nullptr) {
    options.compression_type = getCompressionType(env, compressionType);
    options.codec_backend = getCodecBackend(env, compressionBackend);
    options.compression_mode = getCompressionMode(env, compressionMode);
  }
  std::shared_ptr<arrow::Schema> schema =
      gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));

  auto backend = gluten::createBackend();
  auto reader = backend->getShuffleReader(schema, options, pool, memoryManager);
  return shuffleReaderHolder.insert(reader);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_readStream( // NOLINT
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject jniIn) {
  JNI_METHOD_START
  auto reader = shuffleReaderHolder.lookup(handle);
  std::shared_ptr<arrow::io::InputStream> in = std::make_shared<JavaInputStreamAdaptor>(env, reader->getPool(), jniIn);
  auto outItr = reader->readStream(in);
  return resultIteratorHolder.insert(outItr);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_populateMetrics( // NOLINT
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject metrics) {
  JNI_METHOD_START
  auto reader = shuffleReaderHolder.lookup(handle);
  env->CallVoidMethod(metrics, shuffleReaderMetricsSetDecompressTime, reader->getDecompressTime());
  checkException(env);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject,
    jlong handle) {
  JNI_METHOD_START
  auto reader = shuffleReaderHolder.lookup(handle);
  GLUTEN_THROW_NOT_OK(reader->close());
  shuffleReaderHolder.erase(handle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_nativeInitDatasource( // NOLINT
    JNIEnv* env,
    jobject obj,
    jstring filePath,
    jlong cSchema,
    jlong memoryManagerId,
    jbyteArray options) {
  auto backend = gluten::createBackend();
  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  GLUTEN_CHECK(memoryManager != nullptr, "MemoryManager should not be null.");

  std::shared_ptr<Datasource> datasource = nullptr;

  if (cSchema == -1) {
    // Only inspect the schema and not write
    datasource = backend->getDatasource(jStringToCString(env, filePath), memoryManager, nullptr);
  } else {
    auto sparkOptions = gluten::getConfMap(env, options);
    auto sparkConf = backend->getConfMap();
    sparkOptions.insert(sparkConf.begin(), sparkConf.end());
    auto schema = gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));
    datasource = backend->getDatasource(jStringToCString(env, filePath), memoryManager, schema);
    datasource->init(sparkOptions);
  }

  int64_t instanceID = glutenDatasourceHolder.insert(datasource);
  return instanceID;
}

JNIEXPORT void JNICALL
Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_inspectSchema( // NOLINT
    JNIEnv* env,
    jobject obj,
    jlong instanceId,
    jlong cSchema) {
  JNI_METHOD_START
  auto datasource = glutenDatasourceHolder.lookup(instanceId);
  datasource->inspectSchema(reinterpret_cast<struct ArrowSchema*>(cSchema));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_close( // NOLINT
    JNIEnv* env,
    jobject obj,
    jlong instanceId) {
  JNI_METHOD_START
  auto datasource = glutenDatasourceHolder.lookup(instanceId);
  datasource->close();
  glutenDatasourceHolder.erase(instanceId);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_write( // NOLINT
    JNIEnv* env,
    jobject obj,
    jlong instanceId,
    jobject iter) {
  JNI_METHOD_START
  auto datasource = glutenDatasourceHolder.lookup(instanceId);

  while (env->CallBooleanMethod(iter, veloxColumnarbatchScannerHasNext)) {
    jlong handler = env->CallLongMethod(iter, veloxColumnarbatchScannerNext);
    auto batch = columnarBatchHolder.lookup(handler);
    datasource->write(batch);
    // fixme this skips the general Java side batch-closing routine
    columnarBatchHolder.erase(handler);
  }

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
  JNI_METHOD_END(-1L)
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
  JNI_METHOD_END(-1L)
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

  auto name = jStringToCString(env, jname);
  auto backend = createBackend();
  auto listener = std::make_shared<SparkAllocationListener>(
      vm, jlistener, reserveMemoryMethod, unreserveMemoryMethod, reservationBlockSize);
  auto manager = backend->getMemoryManager(name, *allocator, listener);
  return reinterpret_cast<jlong>(manager);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jbyteArray JNICALL Java_io_glutenproject_memory_nmm_NativeMemoryManager_collectMemoryUsage( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerId) {
  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  const MemoryUsageStats& stats = memoryManager->collectMemoryUsageStats();
  auto size = stats.ByteSizeLong();
  jbyteArray out = env->NewByteArray(size);
  uint8_t buffer[size];
  GLUTEN_CHECK(
      stats.SerializeToArray(reinterpret_cast<void*>(buffer), size),
      "Serialization failed when collecting memory usage stats");
  env->SetByteArrayRegion(out, 0, size, reinterpret_cast<jbyte*>(buffer));
  return out;
}

JNIEXPORT void JNICALL Java_io_glutenproject_memory_nmm_NativeMemoryManager_release( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerId) {
  JNI_METHOD_START
  delete reinterpret_cast<MemoryManager*>(memoryManagerId);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL Java_io_glutenproject_vectorized_ColumnarBatchSerializerJniWrapper_serialize( // NOLINT
    JNIEnv* env,
    jobject,
    jlongArray handles,
    jlong memoryManagerId) {
  JNI_METHOD_START
  int32_t numBatches = env->GetArrayLength(handles);
  jlong* batchhandles = env->GetLongArrayElements(handles, nullptr);

  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  GLUTEN_CHECK(memoryManager != nullptr, "MemoryManager should not be null.");
  std::vector<std::shared_ptr<ColumnarBatch>> batches;
  int64_t numRows = 0L;
  for (int32_t i = 0; i < numBatches; i++) {
    auto batch = columnarBatchHolder.lookup(batchhandles[i]);
    GLUTEN_DCHECK(batch != nullptr, "Cannot find the ColumnarBatch with handle " + std::to_string(batchhandles[i]));
    numRows += batch->numRows();
    batches.emplace_back(batch);
  }
  env->ReleaseLongArrayElements(handles, batchhandles, JNI_ABORT);

  auto backend = createBackend();
  auto arrowPool = memoryManager->getArrowMemoryPool();
  auto serializer = backend->getColumnarBatchSerializer(memoryManager, arrowPool, nullptr);
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
    jobject,
    jlong cSchema,
    jlong memoryManagerId) {
  JNI_METHOD_START
  MemoryManager* memoryManager = reinterpret_cast<MemoryManager*>(memoryManagerId);
  GLUTEN_DCHECK(memoryManager != nullptr, "Memory manager does not exist or has been closed");
  auto arrowPool = memoryManager->getArrowMemoryPool();
  auto backend = createBackend();
  auto serializer =
      backend->getColumnarBatchSerializer(memoryManager, arrowPool, reinterpret_cast<struct ArrowSchema*>(cSchema));
  return columnarBatchSerializerHolder.insert(serializer);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ColumnarBatchSerializerJniWrapper_deserialize( // NOLINT
    JNIEnv* env,
    jobject,
    jlong handle,
    jbyteArray data) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatchSerializer> serializer = columnarBatchSerializerHolder.lookup(handle);
  GLUTEN_DCHECK(serializer != nullptr, "ColumnarBatchSerializer cannot be null");
  int32_t size = env->GetArrayLength(data);
  jbyte* serialized = env->GetByteArrayElements(data, nullptr);
  auto batch = serializer->deserialize(reinterpret_cast<uint8_t*>(serialized), size);
  env->ReleaseByteArrayElements(data, serialized, JNI_ABORT);
  return columnarBatchHolder.insert(batch);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ColumnarBatchSerializerJniWrapper_close(JNIEnv* env, jobject, jlong handle) { // NOLINT
  JNI_METHOD_START
  columnarBatchSerializerHolder.erase(handle);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
