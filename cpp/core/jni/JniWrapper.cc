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
#include <malloc.h>
#include <filesystem>

#include "compute/Backend.h"
#include "compute/ProtobufUtils.h"
#include "config/GlutenConfig.h"
#include "jni/ConcurrentMap.h"
#include "jni/JniCommon.h"
#include "jni/JniErrors.h"

#include "operators/writer/Datasource.h"

#include <arrow/c/bridge.h>
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/PartitionWriterCreator.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/reader.h"
#include "shuffle/rss/CelebornPartitionWriter.h"

namespace types {
class ExpressionList;
} // namespace types

using namespace gluten;

static jclass serializableObjBuilderClass;
static jmethodID serializableObjBuilderConstructor;

jclass javaReservationListenerClass;

jmethodID reserveMemoryMethod;
jmethodID unreserveMemoryMethod;

static jclass byteArrayClass;

static jclass jniByteInputStreamClass;
static jmethodID jniByteInputStreamRead;
static jmethodID jniByteInputStreamTell;
static jmethodID jniByteInputStreamClose;

static jclass splitResultClass;
static jmethodID splitResultConstructor;

static jclass serializedArrowArrayIteratorClass;
static jclass metricsBuilderClass;
static jmethodID metricsBuilderConstructor;

static jmethodID serializedArrowArrayIteratorHasNext;
static jmethodID serializedArrowArrayIteratorNext;

static jclass nativeColumnarToRowInfoClass;
static jmethodID nativeColumnarToRowInfoConstructor;

static jclass veloxColumnarbatchScannerClass;
static jmethodID veloxColumnarbatchScannerHasNext;
static jmethodID veloxColumnarbatchScannerNext;

jlong defaultMemoryAllocatorId = -1L;

static ConcurrentMap<std::shared_ptr<ColumnarToRowConverter>> columnarToRowConverterHolder;

static ConcurrentMap<std::shared_ptr<RowToColumnarConverter>> rowToColumnarConverterHolder;

static ConcurrentMap<std::shared_ptr<ResultIterator>> resultIteratorHolder;

static ConcurrentMap<std::shared_ptr<ShuffleWriter>> shuffleWriterHolder;

static ConcurrentMap<std::shared_ptr<Reader>> shuffleReaderHolder;

static ConcurrentMap<std::shared_ptr<ColumnarBatch>> glutenColumnarbatchHolder;

static ConcurrentMap<std::shared_ptr<Datasource>> glutenDatasourceHolder;

std::shared_ptr<ResultIterator> getArrayIterator(JNIEnv* env, jlong id) {
  auto handler = resultIteratorHolder.lookup(id);
  if (!handler) {
    std::string errorMessage = "invalid handler id " + std::to_string(id);
    gluten::jniThrow(errorMessage);
  }
  return handler;
}

class JavaInputStreamAdaptor final : public arrow::io::InputStream {
 public:
  JavaInputStreamAdaptor(JNIEnv* env, jobject jniIn) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string errorMessage = "Unable to get JavaVM instance";
      gluten::jniThrow(errorMessage);
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
    GLUTEN_ASSIGN_OR_THROW(auto buffer, arrow::AllocateResizableBuffer(nbytes, getDefaultArrowMemoryPool().get()))
    GLUTEN_ASSIGN_OR_THROW(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
    GLUTEN_THROW_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    return std::move(buffer);
  }

 private:
  JavaVM* vm_;
  jobject jniIn_;
  bool closed_ = false;
};

class JniColumnarBatchIterator : public ColumnarBatchIterator {
 public:
  explicit JniColumnarBatchIterator(
      JNIEnv* env,
      jobject javaSerializedArrowArrayIterator,
      std::shared_ptr<ArrowWriter> writer)
      : writer_(writer) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string errorMessage = "Unable to get JavaVM instance";
      gluten::jniThrow(errorMessage);
    }
    javaSerializedArrowArrayIterator_ = env->NewGlobalRef(javaSerializedArrowArrayIterator);
  }

  // singleton, avoid stack instantiation
  JniColumnarBatchIterator(const JniColumnarBatchIterator&) = delete;
  JniColumnarBatchIterator(JniColumnarBatchIterator&&) = delete;
  JniColumnarBatchIterator& operator=(const JniColumnarBatchIterator&) = delete;
  JniColumnarBatchIterator& operator=(JniColumnarBatchIterator&&) = delete;

  virtual ~JniColumnarBatchIterator() {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->DeleteGlobalRef(javaSerializedArrowArrayIterator_);
    vm_->DetachCurrentThread();
  }

  std::shared_ptr<ColumnarBatch> next() override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    if (!env->CallBooleanMethod(javaSerializedArrowArrayIterator_, serializedArrowArrayIteratorHasNext)) {
      checkException(env);
      return nullptr; // stream ended
    }

    checkException(env);
    jlong handle = env->CallLongMethod(javaSerializedArrowArrayIterator_, serializedArrowArrayIteratorNext);
    checkException(env);
    auto batch = glutenColumnarbatchHolder.lookup(handle);
    if (writer_ != nullptr) {
      batch->saveToFile(writer_);
    }
    return batch;
  }

 private:
  JavaVM* vm_;
  jobject javaSerializedArrowArrayIterator_;
  std::shared_ptr<ArrowWriter> writer_;
};

std::unique_ptr<JniColumnarBatchIterator> makeJniColumnarBatchIterator(
    JNIEnv* env,
    jobject javaSerializedArrowArrayIterator,
    std::shared_ptr<ArrowWriter> writer) {
  return std::make_unique<JniColumnarBatchIterator>(env, javaSerializedArrowArrayIterator, writer);
}

jmethodID getMethodIdOrError(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  jmethodID ret = getMethodId(env, thisClass, name, sig);
  if (ret == nullptr) {
    std::string errorMessage = "Unable to find method " + std::string(name) + " within signature" + std::string(sig);
    gluten::jniThrow(errorMessage);
  }
  return ret;
}

jclass createGlobalClassReferenceOrError(JNIEnv* env, const char* className) {
  jclass globalClass = createGlobalClassReference(env, className);
  if (globalClass == nullptr) {
    std::string errorMessage = "Unable to CreateGlobalClassReferenceOrError for" + std::string(className);
    gluten::jniThrow(errorMessage);
  }
  return globalClass;
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
  serializableObjBuilderConstructor = getMethodIdOrError(env, serializableObjBuilderClass, "<init>", "([J[I)V");

  byteArrayClass = createGlobalClassReferenceOrError(env, "[B");

  jniByteInputStreamClass = createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/JniByteInputStream;");
  jniByteInputStreamRead = getMethodIdOrError(env, jniByteInputStreamClass, "read", "(JJ)J");
  jniByteInputStreamTell = getMethodIdOrError(env, jniByteInputStreamClass, "tell", "()J");
  jniByteInputStreamClose = getMethodIdOrError(env, jniByteInputStreamClass, "close", "()V");

  splitResultClass = createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/SplitResult;");
  splitResultConstructor = getMethodIdOrError(env, splitResultClass, "<init>", "(JJJJJJ[J[J)V");

  metricsBuilderClass = createGlobalClassReferenceOrError(env, "Lio/glutenproject/metrics/Metrics;");

  metricsBuilderConstructor =
      getMethodIdOrError(env, metricsBuilderClass, "<init>", "([J[J[J[J[J[J[J[J[J[JJ[J[J[J[J[J[J[J[J[J[J[J)V");

  serializedArrowArrayIteratorClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/ColumnarBatchInIterator;");

  serializedArrowArrayIteratorHasNext = getMethodIdOrError(env, serializedArrowArrayIteratorClass, "hasNext", "()Z");

  serializedArrowArrayIteratorNext = getMethodIdOrError(env, serializedArrowArrayIteratorClass, "next", "()J");

  nativeColumnarToRowInfoClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/NativeColumnarToRowInfo;");
  nativeColumnarToRowInfoConstructor = getMethodIdOrError(env, nativeColumnarToRowInfoClass, "<init>", "(J[I[IJ)V");

  javaReservationListenerClass = createGlobalClassReference(
      env,
      "Lio/glutenproject/memory/alloc/"
      "ReservationListener;");

  reserveMemoryMethod = getMethodIdOrError(env, javaReservationListenerClass, "reserveOrThrow", "(J)V");
  unreserveMemoryMethod = getMethodIdOrError(env, javaReservationListenerClass, "unreserve", "(J)J");

  defaultMemoryAllocatorId = reinterpret_cast<jlong>(defaultMemoryAllocator().get());

  veloxColumnarbatchScannerClass =
      createGlobalClassReference(env, "Lorg/apache/spark/sql/execution/datasources/VeloxColumnarBatchIterator;");

  veloxColumnarbatchScannerHasNext = getMethodId(env, veloxColumnarbatchScannerClass, "hasNext", "()Z");

  veloxColumnarbatchScannerNext = getMethodId(env, veloxColumnarbatchScannerClass, "next", "()J");

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
  env->DeleteGlobalRef(serializedArrowArrayIteratorClass);
  env->DeleteGlobalRef(nativeColumnarToRowInfoClass);
  env->DeleteGlobalRef(byteArrayClass);
  env->DeleteGlobalRef(veloxColumnarbatchScannerClass);
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithIterator( // NOLINT
    JNIEnv* env,
    jobject obj,
    jlong allocatorId,
    jbyteArray planArr,
    jobjectArray iterArr,
    jint stageId,
    jint partitionId,
    jlong taskId,
    jboolean saveInput,
    jstring localDir,
    jbyteArray confArr) {
  JNI_METHOD_START
  arrow::Status msg;

  auto localDirStr = jStringToCString(env, localDir);

  auto planData = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArr, 0));
  auto planSize = env->GetArrayLength(planArr);

  auto* allocator = reinterpret_cast<MemoryAllocator*>(allocatorId);
  if (allocator == nullptr) {
    gluten::jniThrow("Memory pool does not exist or has been closed");
  }

  auto backend = gluten::createBackend();
  if (!backend->parsePlan(planData, planSize, stageId, partitionId, taskId)) {
    gluten::jniThrow("Failed to parse plan.");
  }

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
        gluten::jniThrow("Save input path " + dir + " does not exists");
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

  std::shared_ptr<ResultIterator> resIter = backend->getResultIterator(allocator, localDirStr, inputIters, confs);
  return resultIteratorHolder.insert(std::move(resIter));
  JNI_METHOD_END(-1)
}

JNIEXPORT jboolean JNICALL
Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeHasNext(JNIEnv* env, jobject obj, jlong id) { // NOLINT
  JNI_METHOD_START
  auto iter = getArrayIterator(env, id);
  if (iter == nullptr) {
    std::string errorMessage = "faked to get batch iterator";
    gluten::jniThrow(errorMessage);
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
  jlong batchHandle = glutenColumnarbatchHolder.insert(batch);

  iter->setExportNanos(batch->getExportNanos());
  return batchHandle;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeFetchMetrics(
    JNIEnv* env,
    jobject obj,
    jlong id) { // NOLINT
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
      scanTime);
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ColumnarBatchOutIterator_nativeClose(
    JNIEnv* env,
    jobject this_obj,
    jlong id) { // NOLINT
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

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeConvertColumnarToRow( // NOLINT
    JNIEnv* env,
    jobject,
    jlong batchHandle,
    jlong allocatorId) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> cb = glutenColumnarbatchHolder.lookup(batchHandle);
  int64_t numRows = cb->getNumRows();
  // convert the record batch to spark unsafe row.
  auto* allocator = reinterpret_cast<MemoryAllocator*>(allocatorId);
  if (allocator == nullptr) {
    gluten::jniThrow("Memory pool does not exist or has been closed");
  }
  auto backend = gluten::createBackend();
  std::shared_ptr<ColumnarToRowConverter> columnarToRowConverter =
      gluten::jniGetOrThrow(backend->getColumnar2RowConverter(allocator, cb));
  gluten::jniAssertOkOrThrow(
      columnarToRowConverter->init(),
      "Native convert columnar to row: Init "
      "ColumnarToRowConverter failed");
  gluten::jniAssertOkOrThrow(
      columnarToRowConverter->write(), "Native convert columnar to row: ColumnarToRowConverter write failed");

  const auto& offsets = columnarToRowConverter->getOffsets();
  const auto& lengths = columnarToRowConverter->getLengths();
  int64_t instanceID = columnarToRowConverterHolder.insert(columnarToRowConverter);

  auto offsetsArr = env->NewIntArray(numRows);
  auto offsetsSrc = reinterpret_cast<const jint*>(offsets.data());
  env->SetIntArrayRegion(offsetsArr, 0, numRows, offsetsSrc);
  auto lengthsArr = env->NewIntArray(numRows);
  auto lengthsSrc = reinterpret_cast<const jint*>(lengths.data());
  env->SetIntArrayRegion(lengthsArr, 0, numRows, lengthsSrc);
  long address = reinterpret_cast<long>(columnarToRowConverter->getBufferAddress());

  jobject nativeColumnarToRowInfo = env->NewObject(
      nativeColumnarToRowInfoClass, nativeColumnarToRowInfoConstructor, instanceID, offsetsArr, lengthsArr, address);
  return nativeColumnarToRowInfo;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeClose(
    JNIEnv* env,
    jobject,
    jlong instanceId) { // NOLINT
  JNI_METHOD_START
  columnarToRowConverterHolder.erase(instanceId);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_NativeRowToColumnarJniWrapper_init(
    JNIEnv* env,
    jobject,
    jlong cSchema,
    long allocId) { // NOLINT
  JNI_METHOD_START
  auto* allocator = reinterpret_cast<MemoryAllocator*>(allocId);
  if (allocator == nullptr) {
    gluten::jniThrow("Memory pool does not exist or has been closed");
  }
  auto backend = gluten::createBackend();
  auto converter = backend->getRowToColumnarConverter(allocator, reinterpret_cast<struct ArrowSchema*>(cSchema));
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
    gluten::jniThrow("Native convert row to columnar: buf_addrs can't be null");
  }
  int numRows = env->GetArrayLength(rowLength);
  jlong* inRowLength = env->GetLongArrayElements(rowLength, JNI_FALSE);
  uint8_t* address = reinterpret_cast<uint8_t*>(memoryAddress);

  auto converter = rowToColumnarConverterHolder.lookup(r2cId);
  auto cb = converter->convert(numRows, inRowLength, address);
  return glutenColumnarbatchHolder.insert(cb);
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
  std::shared_ptr<ColumnarBatch> batch = glutenColumnarbatchHolder.lookup(handle);
  return env->NewStringUTF(batch->getType().c_str());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_getBytes(JNIEnv* env, jobject, jlong handle) { // NOLINT
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = glutenColumnarbatchHolder.lookup(handle);
  return batch->getBytes();
  JNI_METHOD_END(-1)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_getNumColumns(
    JNIEnv* env,
    jobject,
    jlong handle) { // NOLINT
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = glutenColumnarbatchHolder.lookup(handle);
  return batch->getNumColumns();
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_getNumRows(JNIEnv* env, jobject, jlong handle) { // NOLINT
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = glutenColumnarbatchHolder.lookup(handle);
  return batch->getNumRows();
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_addColumn( // NOLINT
    JNIEnv* env,
    jobject,
    jlong handle,
    jint index,
    jlong colHandle) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = glutenColumnarbatchHolder.lookup(handle);
  std::shared_ptr<ColumnarBatch> col = glutenColumnarbatchHolder.lookup(colHandle);
#ifdef DEBUG
  if (col->GetNumColumns() != 1) {
    throw GlutenException("Add column should add one col");
  }
#endif
  auto newBatch = batch->addColumn(index, col);
  glutenColumnarbatchHolder.erase(handle);
  glutenColumnarbatchHolder.erase(colHandle);
  return glutenColumnarbatchHolder.insert(newBatch);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_exportToArrow( // NOLINT
    JNIEnv* env,
    jobject,
    jlong handle,
    jlong cSchema,
    jlong cArray) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = glutenColumnarbatchHolder.lookup(handle);
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
  return glutenColumnarbatchHolder.insert(batch);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_close(JNIEnv* env, jobject, jlong handle) { // NOLINT
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = glutenColumnarbatchHolder.lookup(handle);
  glutenColumnarbatchHolder.erase(handle);
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
    jstring compressionTypeJstr,
    jint batchCompressThreshold,
    jstring dataFileJstr,
    jint numSubDirs,
    jstring localDirsJstr,
    jboolean preferEvict,
    jlong allocatorId,
    jboolean writeSchema,
    jlong firstBatchHandle,
    jlong taskAttemptId,
    jint pushBufferMaxSize,
    jobject partitionPusher,
    jstring partitionWriterTypeJstr) {
  JNI_METHOD_START
  if (partitioningNameJstr == nullptr) {
    gluten::jniThrow(std::string("Short partitioning name can't be null"));
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

  if (compressionTypeJstr != NULL) {
    auto compressionTypeResult = getCompressionType(env, compressionTypeJstr);
    if (compressionTypeResult.status().ok()) {
      shuffleWriterOptions.compression_type = compressionTypeResult.MoveValueUnsafe();
    }
  }

  auto* allocator = reinterpret_cast<MemoryAllocator*>(allocatorId);
  if (allocator == nullptr) {
    gluten::jniThrow("Memory pool does not exist or has been closed");
  }
  shuffleWriterOptions.memory_pool = asWrappedArrowMemoryPool(allocator);

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
  shuffleWriterOptions.batch_compress_threshold = batchCompressThreshold;

  auto partitionWriterTypeC = env->GetStringUTFChars(partitionWriterTypeJstr, JNI_FALSE);
  auto partitionWriterType = std::string(partitionWriterTypeC);
  env->ReleaseStringUTFChars(partitionWriterTypeJstr, partitionWriterTypeC);

  std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator;

  if (partitionWriterType == "local") {
    shuffleWriterOptions.partition_writer_type = "local";
    if (dataFileJstr == NULL) {
      gluten::jniThrow(std::string("Shuffle DataFile can't be null"));
    }
    if (localDirsJstr == NULL) {
      gluten::jniThrow(std::string("Shuffle DataFile can't be null"));
    }

    shuffleWriterOptions.write_schema = writeSchema;
    shuffleWriterOptions.prefer_evict = preferEvict;

    if (numSubDirs > 0) {
      shuffleWriterOptions.num_sub_dirs = numSubDirs;
    }

    auto dataFileC = env->GetStringUTFChars(dataFileJstr, JNI_FALSE);
    shuffleWriterOptions.data_file = std::string(dataFileC);
    env->ReleaseStringUTFChars(dataFileJstr, dataFileC);

    auto localDirs = env->GetStringUTFChars(localDirsJstr, JNI_FALSE);
    setenv("NATIVESQL_SPARK_LOCAL_DIRS", localDirs, 1);
    env->ReleaseStringUTFChars(localDirsJstr, localDirs);
    partitionWriterCreator = std::make_shared<LocalPartitionWriterCreator>();
  } else if (partitionWriterType == "celeborn") {
    shuffleWriterOptions.partition_writer_type = "celeborn";
    jclass celebornPartitionPusherClass =
        createGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
    jmethodID celebornPushPartitionDataMethod =
        getMethodIdOrError(env, celebornPartitionPusherClass, "pushPartitionData", "(I[B)I");
    if (pushBufferMaxSize > 0) {
      shuffleWriterOptions.push_buffer_max_size = pushBufferMaxSize;
    }
    JavaVM* vm;
    if (env->GetJavaVM(&vm) != JNI_OK) {
      gluten::jniThrow("Unable to get JavaVM instance");
    }
    std::shared_ptr<CelebornClient> celebornClient =
        std::make_shared<CelebornClient>(vm, partitionPusher, celebornPushPartitionDataMethod);
    partitionWriterCreator = std::make_shared<CelebornPartitionWriterCreator>(std::move(celebornClient));
  } else {
    gluten::jniThrow("Unrecognizable partition writer type: " + partitionWriterType);
  }

  auto backend = gluten::createBackend();
  auto batch = glutenColumnarbatchHolder.lookup(firstBatchHandle);
  auto shuffleWriter = backend->makeShuffleWriter(
      numPartitions, std::move(partitionWriterCreator), std::move(shuffleWriterOptions), batch->getType());

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
    gluten::jniThrow(errorMessage);
  }
  jlong evictedSize;
  gluten::jniAssertOkOrThrow(shuffleWriter->evictFixedSize(size, &evictedSize), "(shuffle) nativeEvict: evict failed");
  return evictedSize;
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
    gluten::jniThrow(errorMessage);
  }

  // The column batch maybe VeloxColumnBatch or ArrowCStructColumnarBatch(FallbackRangeShuffleWriter)
  std::shared_ptr<ColumnarBatch> batch = glutenColumnarbatchHolder.lookup(handle);
  auto numBytes = batch->getBytes();
  gluten::jniAssertOkOrThrow(shuffleWriter->split(batch.get()), "Native split: shuffle writer split failed");
  return numBytes;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_stop(JNIEnv* env, jobject, jlong shuffleWriterId) { // NOLINT
  JNI_METHOD_START
  auto shuffleWriter = shuffleWriterHolder.lookup(shuffleWriterId);
  if (!shuffleWriter) {
    std::string errorMessage = "Invalid shuffle writer id " + std::to_string(shuffleWriterId);
    gluten::jniThrow(errorMessage);
  }

  gluten::jniAssertOkOrThrow(shuffleWriter->stop(), "Native split: shuffle writer stop failed");

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
    jclass,
    jobject jniIn,
    jlong cSchema,
    jlong allocId) {
  JNI_METHOD_START
  auto* allocator = reinterpret_cast<MemoryAllocator*>(allocId);
  if (allocator == nullptr) {
    gluten::jniThrow("Memory pool does not exist or has been closed");
  }
  auto pool = asWrappedArrowMemoryPool(allocator);
  std::shared_ptr<arrow::io::InputStream> in = std::make_shared<JavaInputStreamAdaptor>(env, jniIn);
  ReaderOptions options = ReaderOptions::defaults();
  options.ipc_read_options.memory_pool = pool.get();
  options.ipc_read_options.use_threads = false;
  std::shared_ptr<arrow::Schema> schema =
      gluten::jniGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));
  auto reader = std::make_shared<Reader>(in, schema, options, pool);
  return shuffleReaderHolder.insert(reader);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_next(JNIEnv* env, jclass, jlong handle) { // NOLINT
  JNI_METHOD_START
  auto reader = shuffleReaderHolder.lookup(handle);
  GLUTEN_ASSIGN_OR_THROW(auto gluten_batch, reader->next())
  if (gluten_batch == nullptr) {
    return -1L;
  }

  return glutenColumnarbatchHolder.insert(gluten_batch);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_close(JNIEnv* env, jclass, jlong handle) { // NOLINT
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
    jstring fileName,
    jlong cSchema) {
  auto backend = gluten::createBackend();

  std::shared_ptr<Datasource> datasource = nullptr;

  if (cSchema == -1) {
    // Only inspect the schema and not write
    datasource = backend->getDatasource(jStringToCString(env, filePath), jStringToCString(env, fileName), nullptr);
  } else {
    auto schema = gluten::jniGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema)));
    datasource = backend->getDatasource(jStringToCString(env, filePath), jStringToCString(env, fileName), schema);
    datasource->init(backend->getConfMap());
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
  auto schema = datasource->inspectSchema();
  GLUTEN_THROW_NOT_OK(arrow::ExportSchema(*schema.get(), reinterpret_cast<struct ArrowSchema*>(cSchema)));
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
  auto backend = gluten::createBackend();

  while (env->CallBooleanMethod(iter, veloxColumnarbatchScannerHasNext)) {
    jlong handler = env->CallLongMethod(iter, veloxColumnarbatchScannerNext);
    auto batch = glutenColumnarbatchHolder.lookup(handler);
    glutenDatasourceHolder.lookup(instanceId)->write(batch);
    glutenColumnarbatchHolder.erase(handler);
  }

  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_getDefaultAllocator(JNIEnv* env, jclass) { // NOLINT
  JNI_METHOD_START
  return defaultMemoryAllocatorId;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_createListenableAllocator( // NOLINT
    JNIEnv* env,
    jclass,
    jobject jlistener) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    gluten::jniThrow("Unable to get JavaVM instance");
  }
  std::shared_ptr<AllocationListener> listener = std::make_shared<SparkAllocationListener>(
      vm, jlistener, reserveMemoryMethod, unreserveMemoryMethod, 8L << 10 << 10);
  auto allocator = new ListenableMemoryAllocator(defaultMemoryAllocator().get(), listener);
  return (jlong)(allocator);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_releaseAllocator(
    JNIEnv* env,
    jclass,
    jlong allocatorId) { // NOLINT
  JNI_METHOD_START
  if (allocatorId == defaultMemoryAllocatorId) {
    return;
  }
  delete (MemoryAllocator*)(allocatorId);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_bytesAllocated(
    JNIEnv* env,
    jclass,
    jlong allocatorId) { // NOLINT
  JNI_METHOD_START
  auto* alloc = (MemoryAllocator*)(allocatorId);
  if (alloc == nullptr) {
    gluten::jniThrow("Memory allocator instance not found. It may not exist nor has been closed");
  }
  return alloc->getBytes();
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL Java_io_glutenproject_tpc_MallocUtils_mallocTrim(JNIEnv* env, jobject obj) { // NOLINT
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_trim... " << std::endl;
  malloc_trim(0);
}

JNIEXPORT void JNICALL Java_io_glutenproject_tpc_MallocUtils_mallocStats(JNIEnv* env, jobject obj) { // NOLINT
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_stats... " << std::endl;
  malloc_stats();
}

#ifdef __cplusplus
}
#endif
