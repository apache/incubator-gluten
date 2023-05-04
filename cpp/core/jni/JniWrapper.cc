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
#include "shuffle/ShuffleWriter.h"
#include "shuffle/reader.h"

namespace types {
class ExpressionList;
} // namespace types

using namespace gluten;

static jclass serializable_obj_builder_class;
static jmethodID serializable_obj_builder_constructor;

jclass java_reservation_listener_class;

jmethodID reserve_memory_method;
jmethodID unreserve_memory_method;

static jclass byte_array_class;

static jclass jni_byte_input_stream_class;
static jmethodID jni_byte_input_stream_read;
static jmethodID jni_byte_input_stream_tell;
static jmethodID jni_byte_input_stream_close;

static jclass split_result_class;
static jmethodID split_result_constructor;

static jclass serialized_arrow_array_iterator_class;
static jclass metrics_builder_class;
static jmethodID metrics_builder_constructor;

static jmethodID serialized_arrow_array_iterator_hasNext;
static jmethodID serialized_arrow_array_iterator_next;

static jclass native_columnar_to_row_info_class;
static jmethodID native_columnar_to_row_info_constructor;

static jclass velox_columnarbatch_scanner_class;
static jmethodID velox_columnarbatch_scanner_hasNext;
static jmethodID velox_columnarbatch_scanner_next;

jlong default_memory_allocator_id = -1L;

static ConcurrentMap<std::shared_ptr<ColumnarToRowConverter>> columnar_to_row_converter_holder_;

static ConcurrentMap<std::shared_ptr<RowToColumnarConverter>> row_to_columnar_converter_holder_;

static ConcurrentMap<std::shared_ptr<ResultIterator>> result_iterator_holder_;

static ConcurrentMap<std::shared_ptr<ShuffleWriter>> shuffle_writer_holder_;

static ConcurrentMap<std::shared_ptr<Reader>> shuffle_reader_holder_;

static ConcurrentMap<std::shared_ptr<ColumnarBatch>> gluten_columnarbatch_holder_;

static ConcurrentMap<std::shared_ptr<Datasource>> gluten_datasource_holder_;

std::shared_ptr<ResultIterator> GetArrayIterator(JNIEnv* env, jlong id) {
  auto handler = result_iterator_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    gluten::JniThrow(error_message);
  }
  return handler;
}

class JavaInputStreamAdaptor final : public arrow::io::InputStream {
 public:
  JavaInputStreamAdaptor(JNIEnv* env, jobject jni_in) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string error_message = "Unable to get JavaVM instance";
      gluten::JniThrow(error_message);
    }
    jni_in_ = env->NewGlobalRef(jni_in);
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
    AttachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->CallVoidMethod(jni_in_, jni_byte_input_stream_close);
    CheckException(env);
    env->DeleteGlobalRef(jni_in_);
    vm_->DetachCurrentThread();
    closed_ = true;
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    JNIEnv* env;
    AttachCurrentThreadAsDaemonOrThrow(vm_, &env);
    return env->CallLongMethod(jni_in_, jni_byte_input_stream_tell);
  }

  bool closed() const override {
    return closed_;
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    JNIEnv* env;
    AttachCurrentThreadAsDaemonOrThrow(vm_, &env);
    return env->CallLongMethod(jni_in_, jni_byte_input_stream_read, reinterpret_cast<jlong>(out), nbytes);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    GLUTEN_ASSIGN_OR_THROW(
        auto buffer, arrow::AllocateResizableBuffer(nbytes, GetDefaultWrappedArrowMemoryPool().get()))
    GLUTEN_ASSIGN_OR_THROW(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
    GLUTEN_THROW_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    return std::move(buffer);
  }

 private:
  JavaVM* vm_;
  jobject jni_in_;
  bool closed_ = false;
};

class JavaArrowArrayIterator {
 public:
  explicit JavaArrowArrayIterator(
      JNIEnv* env,
      jobject java_serialized_arrow_array_iterator,
      std::shared_ptr<ArrowWriter> writer)
      : writer_(writer) {
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
      std::string error_message = "Unable to get JavaVM instance";
      gluten::JniThrow(error_message);
    }
    java_serialized_arrow_array_iterator_ = env->NewGlobalRef(java_serialized_arrow_array_iterator);
  }

  // singleton, avoid stack instantiation
  JavaArrowArrayIterator(const JavaArrowArrayIterator&) = delete;
  JavaArrowArrayIterator(JavaArrowArrayIterator&&) = delete;
  JavaArrowArrayIterator& operator=(const JavaArrowArrayIterator&) = delete;
  JavaArrowArrayIterator& operator=(JavaArrowArrayIterator&&) = delete;

  virtual ~JavaArrowArrayIterator() {
    JNIEnv* env;
    AttachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->DeleteGlobalRef(java_serialized_arrow_array_iterator_);
    vm_->DetachCurrentThread();
  }

  std::shared_ptr<ColumnarBatch> Next() {
    JNIEnv* env;
    AttachCurrentThreadAsDaemonOrThrow(vm_, &env);
    if (!env->CallBooleanMethod(java_serialized_arrow_array_iterator_, serialized_arrow_array_iterator_hasNext)) {
      CheckException(env);
      return nullptr; // stream ended
    }

    CheckException(env);
    jlong handle = env->CallLongMethod(java_serialized_arrow_array_iterator_, serialized_arrow_array_iterator_next);
    CheckException(env);
    auto batch = gluten_columnarbatch_holder_.Lookup(handle);
    if (writer_ != nullptr) {
      batch->saveToFile(writer_);
    }
    return batch;
  }

 private:
  JavaVM* vm_;
  jobject java_serialized_arrow_array_iterator_;
  std::shared_ptr<ArrowWriter> writer_;
};

// See Java class
// org/apache/arrow/dataset/jni/NativeSerializedRecordBatchIterator
//
std::unique_ptr<JavaArrowArrayIterator> MakeJavaArrowArrayIterator(
    JNIEnv* env,
    jobject java_serialized_arrow_array_iterator,
    std::shared_ptr<ArrowWriter> writer) {
  return std::make_unique<JavaArrowArrayIterator>(env, java_serialized_arrow_array_iterator, writer);
}

jmethodID GetMethodIDOrError(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = GetMethodID(env, this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) + " within signature" + std::string(sig);
    gluten::JniThrow(error_message);
  }
  return ret;
}

jclass CreateGlobalClassReferenceOrError(JNIEnv* env, const char* class_name) {
  jclass global_class = CreateGlobalClassReference(env, class_name);
  if (global_class == nullptr) {
    std::string error_message = "Unable to CreateGlobalClassReferenceOrError for" + std::string(class_name);
    gluten::JniThrow(error_message);
  }
  return global_class;
}

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  gluten::GetJniErrorsState()->Initialize(env);

  serializable_obj_builder_class =
      CreateGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/NativeSerializableObject;");
  serializable_obj_builder_constructor = GetMethodIDOrError(env, serializable_obj_builder_class, "<init>", "([J[I)V");

  byte_array_class = CreateGlobalClassReferenceOrError(env, "[B");

  jni_byte_input_stream_class =
      CreateGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/JniByteInputStream;");
  jni_byte_input_stream_read = GetMethodIDOrError(env, jni_byte_input_stream_class, "read", "(JJ)J");
  jni_byte_input_stream_tell = GetMethodIDOrError(env, jni_byte_input_stream_class, "tell", "()J");
  jni_byte_input_stream_close = GetMethodIDOrError(env, jni_byte_input_stream_class, "close", "()V");

  split_result_class = CreateGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/SplitResult;");
  split_result_constructor = GetMethodIDOrError(env, split_result_class, "<init>", "(JJJJJJ[J[J)V");

  metrics_builder_class = CreateGlobalClassReferenceOrError(env, "Lio/glutenproject/metrics/Metrics;");

  metrics_builder_constructor =
      GetMethodIDOrError(env, metrics_builder_class, "<init>", "([J[J[J[J[J[J[J[J[J[JJ[J[J[J[J[J[J[J[J[J[J[J)V");

  serialized_arrow_array_iterator_class =
      CreateGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/ArrowInIterator;");

  serialized_arrow_array_iterator_hasNext =
      GetMethodIDOrError(env, serialized_arrow_array_iterator_class, "hasNext", "()Z");

  serialized_arrow_array_iterator_next = GetMethodIDOrError(env, serialized_arrow_array_iterator_class, "next", "()J");

  native_columnar_to_row_info_class =
      CreateGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/NativeColumnarToRowInfo;");
  native_columnar_to_row_info_constructor =
      GetMethodIDOrError(env, native_columnar_to_row_info_class, "<init>", "(J[I[IJ)V");

  java_reservation_listener_class = CreateGlobalClassReference(
      env,
      "Lio/glutenproject/memory/alloc/"
      "ReservationListener;");

  reserve_memory_method = GetMethodIDOrError(env, java_reservation_listener_class, "reserveOrThrow", "(J)V");
  unreserve_memory_method = GetMethodIDOrError(env, java_reservation_listener_class, "unreserve", "(J)J");

  default_memory_allocator_id = reinterpret_cast<jlong>(DefaultMemoryAllocator().get());

  velox_columnarbatch_scanner_class =
      CreateGlobalClassReference(env, "Lorg/apache/spark/sql/execution/datasources/VeloxColumnarBatchIterator;");

  velox_columnarbatch_scanner_hasNext = GetMethodID(env, velox_columnarbatch_scanner_class, "hasNext", "()Z");

  velox_columnarbatch_scanner_next = GetMethodID(env, velox_columnarbatch_scanner_class, "next", "()J");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  result_iterator_holder_.Clear();
  columnar_to_row_converter_holder_.Clear();
  shuffle_writer_holder_.Clear();
  shuffle_reader_holder_.Clear();
  gluten_datasource_holder_.Clear();

  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(serializable_obj_builder_class);
  env->DeleteGlobalRef(jni_byte_input_stream_class);
  env->DeleteGlobalRef(split_result_class);
  env->DeleteGlobalRef(serialized_arrow_array_iterator_class);
  env->DeleteGlobalRef(native_columnar_to_row_info_class);
  env->DeleteGlobalRef(byte_array_class);
  env->DeleteGlobalRef(velox_columnarbatch_scanner_class);
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithIterator(
    JNIEnv* env,
    jobject obj,
    jlong allocator_id,
    jbyteArray plan_arr,
    jobjectArray iter_arr,
    jint stage_id,
    jint partition_id,
    jlong task_id,
    jboolean save_input,
    jstring local_dir,
    jbyteArray conf_arr) {
  JNI_METHOD_START
  arrow::Status msg;

  auto local_dir_str = JStringToCString(env, local_dir);

  auto plan_data = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(plan_arr, 0));
  auto plan_size = env->GetArrayLength(plan_arr);

  auto* allocator = reinterpret_cast<MemoryAllocator*>(allocator_id);
  if (allocator == nullptr) {
    gluten::JniThrow("Memory pool does not exist or has been closed");
  }

  auto backend = gluten::CreateBackend();
  if (!backend->ParsePlan(plan_data, plan_size, stage_id, partition_id, task_id)) {
    gluten::JniThrow("Failed to parse plan.");
  }

  auto confs = getConfMap(env, conf_arr);

  // Handle the Java iters
  jsize iters_len = env->GetArrayLength(iter_arr);
  std::vector<std::shared_ptr<ResultIterator>> input_iters;
  for (int idx = 0; idx < iters_len; idx++) {
    std::shared_ptr<ArrowWriter> writer = nullptr;
    if (save_input) {
      auto dir = confs[kGlutenSaveDir];
      std::filesystem::path f{dir};
      if (!std::filesystem::exists(f)) {
        gluten::JniThrow("Save input path " + dir + " does not exists");
      }
      auto file = confs[kGlutenSaveDir] + "/input_" + std::to_string(task_id) + "_" + std::to_string(idx) + "_" +
          std::to_string(partition_id) + ".parquet";
      writer = std::make_shared<ArrowWriter>(file);
    }
    jobject iter = env->GetObjectArrayElement(iter_arr, idx);
    auto array_iter = MakeJavaArrowArrayIterator(env, iter, writer);
    auto result_iter = std::make_shared<ResultIterator>(std::move(array_iter));
    input_iters.push_back(std::move(result_iter));
  }

  std::shared_ptr<ResultIterator> res_iter = backend->GetResultIterator(allocator, local_dir_str, input_iters, confs);
  return result_iterator_holder_.Insert(std::move(res_iter));
  JNI_METHOD_END(-1)
}

JNIEXPORT jboolean JNICALL
Java_io_glutenproject_vectorized_ArrowOutIterator_nativeHasNext(JNIEnv* env, jobject obj, jlong id) {
  JNI_METHOD_START
  auto iter = GetArrayIterator(env, id);
  if (iter == nullptr) {
    std::string error_message = "faked to get batch iterator";
    gluten::JniThrow(error_message);
  }
  return iter->HasNext();
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ArrowOutIterator_nativeNext(JNIEnv* env, jobject obj, jlong id) {
  JNI_METHOD_START
  auto iter = GetArrayIterator(env, id);
  if (!iter->HasNext()) {
    return -1L;
  }

  std::shared_ptr<ColumnarBatch> batch = iter->Next();
  jlong batch_handle = gluten_columnarbatch_holder_.Insert(batch);

  iter->setExportNanos(batch->getExportNanos());
  return batch_handle;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_ArrowOutIterator_nativeFetchMetrics(JNIEnv* env, jobject obj, jlong id) {
  JNI_METHOD_START
  auto iter = GetArrayIterator(env, id);
  std::shared_ptr<Metrics> metrics = iter->GetMetrics();

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
      metrics_builder_class,
      metrics_builder_constructor,
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

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ArrowOutIterator_nativeClose(JNIEnv* env, jobject this_obj, jlong id) {
  JNI_METHOD_START
#ifdef GLUTEN_PRINT_DEBUG
  auto it = result_iterator_holder_.Lookup(id);
  if (it.use_count() > 2) {
    std::cout << "ArrowArrayResultIterator Id " << id << " use count is " << it.use_count() << std::endl;
  }
#endif
  result_iterator_holder_.Erase(id);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeConvertColumnarToRow(
    JNIEnv* env,
    jobject,
    jlong batch_handle,
    jlong allocator_id) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> cb = gluten_columnarbatch_holder_.Lookup(batch_handle);
  int64_t num_rows = cb->GetNumRows();
  // convert the record batch to spark unsafe row.
  auto* allocator = reinterpret_cast<MemoryAllocator*>(allocator_id);
  if (allocator == nullptr) {
    gluten::JniThrow("Memory pool does not exist or has been closed");
  }
  auto backend = gluten::CreateBackend();
  std::shared_ptr<ColumnarToRowConverter> columnar_to_row_converter =
      gluten::JniGetOrThrow(backend->getColumnar2RowConverter(allocator, cb));
  gluten::JniAssertOkOrThrow(
      columnar_to_row_converter->Init(),
      "Native convert columnar to row: Init "
      "ColumnarToRowConverter failed");
  gluten::JniAssertOkOrThrow(
      columnar_to_row_converter->Write(), "Native convert columnar to row: ColumnarToRowConverter write failed");

  const auto& offsets = columnar_to_row_converter->GetOffsets();
  const auto& lengths = columnar_to_row_converter->GetLengths();
  int64_t instanceID = columnar_to_row_converter_holder_.Insert(columnar_to_row_converter);

  auto offsets_arr = env->NewIntArray(num_rows);
  auto offsets_src = reinterpret_cast<const jint*>(offsets.data());
  env->SetIntArrayRegion(offsets_arr, 0, num_rows, offsets_src);
  auto lengths_arr = env->NewIntArray(num_rows);
  auto lengths_src = reinterpret_cast<const jint*>(lengths.data());
  env->SetIntArrayRegion(lengths_arr, 0, num_rows, lengths_src);
  long address = reinterpret_cast<long>(columnar_to_row_converter->GetBufferAddress());

  jobject native_columnar_to_row_info = env->NewObject(
      native_columnar_to_row_info_class,
      native_columnar_to_row_info_constructor,
      instanceID,
      offsets_arr,
      lengths_arr,
      address);
  return native_columnar_to_row_info;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeClose(JNIEnv* env, jobject, jlong instance_id) {
  JNI_METHOD_START
  columnar_to_row_converter_holder_.Erase(instance_id);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_NativeRowToColumnarJniWrapper_init(JNIEnv* env, jobject, jlong cSchema, long allocId) {
  JNI_METHOD_START
  auto* allocator = reinterpret_cast<MemoryAllocator*>(allocId);
  if (allocator == nullptr) {
    gluten::JniThrow("Memory pool does not exist or has been closed");
  }
  auto backend = gluten::CreateBackend();
  auto converter = backend->getRowToColumnarConverter(allocator, reinterpret_cast<struct ArrowSchema*>(cSchema));
  return row_to_columnar_converter_holder_.Insert(converter);
  JNI_METHOD_END(-1)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_NativeRowToColumnarJniWrapper_nativeConvertRowToColumnar(
    JNIEnv* env,
    jobject,
    jlong r2cId,
    jlongArray row_length,
    jlong memory_address) {
  JNI_METHOD_START
  if (row_length == nullptr) {
    gluten::JniThrow("Native convert row to columnar: buf_addrs can't be null");
  }
  int num_rows = env->GetArrayLength(row_length);
  jlong* in_row_length = env->GetLongArrayElements(row_length, JNI_FALSE);
  uint8_t* address = reinterpret_cast<uint8_t*>(memory_address);

  auto converter = row_to_columnar_converter_holder_.Lookup(r2cId);
  auto cb = converter->convert(num_rows, in_row_length, address);
  return gluten_columnarbatch_holder_.Insert(cb);
  JNI_METHOD_END(-1)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_NativeRowToColumnarJniWrapper_close(JNIEnv* env, jobject, jlong r2cId) {
  JNI_METHOD_START
  row_to_columnar_converter_holder_.Erase(r2cId);
  JNI_METHOD_END()
}

JNIEXPORT jstring JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_getType(JNIEnv* env, jobject, jlong handle) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = gluten_columnarbatch_holder_.Lookup(handle);
  return env->NewStringUTF(batch->GetType().c_str());
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_getBytes(JNIEnv* env, jobject, jlong handle) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = gluten_columnarbatch_holder_.Lookup(handle);
  return batch->GetBytes();
  JNI_METHOD_END(-1)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_getNumColumns(JNIEnv* env, jobject, jlong handle) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = gluten_columnarbatch_holder_.Lookup(handle);
  return batch->GetNumColumns();
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_getNumRows(JNIEnv* env, jobject, jlong handle) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = gluten_columnarbatch_holder_.Lookup(handle);
  return batch->GetNumRows();
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_addColumn(
    JNIEnv* env,
    jobject,
    jlong handle,
    jint index,
    jlong colHandle) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = gluten_columnarbatch_holder_.Lookup(handle);
  std::shared_ptr<ColumnarBatch> col = gluten_columnarbatch_holder_.Lookup(colHandle);
#ifdef DEBUG
  if (col->GetNumColumns() != 1) {
    throw GlutenException("Add column should add one col");
  }
#endif
  auto newBatch = batch->addColumn(index, col);
  gluten_columnarbatch_holder_.Erase(handle);
  gluten_columnarbatch_holder_.Erase(colHandle);
  return gluten_columnarbatch_holder_.Insert(newBatch);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_exportToArrow(
    JNIEnv* env,
    jobject,
    jlong handle,
    jlong c_schema,
    jlong c_array) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = gluten_columnarbatch_holder_.Lookup(handle);
  std::shared_ptr<ArrowSchema> exported_schema = batch->exportArrowSchema();
  std::shared_ptr<ArrowArray> exported_array = batch->exportArrowArray();
  ArrowSchemaMove(exported_schema.get(), reinterpret_cast<struct ArrowSchema*>(c_schema));
  ArrowArrayMove(exported_array.get(), reinterpret_cast<struct ArrowArray*>(c_array));
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_createWithArrowArray(
    JNIEnv* env,
    jobject,
    jlong c_schema,
    jlong c_array) {
  JNI_METHOD_START
  std::unique_ptr<ArrowSchema> target_schema = std::make_unique<ArrowSchema>();
  std::unique_ptr<ArrowArray> target_array = std::make_unique<ArrowArray>();
  auto* arrow_schema = reinterpret_cast<ArrowSchema*>(c_schema);
  auto* arrow_array = reinterpret_cast<ArrowArray*>(c_array);
  ArrowArrayMove(arrow_array, target_array.get());
  ArrowSchemaMove(arrow_schema, target_schema.get());
  std::shared_ptr<ColumnarBatch> batch =
      std::make_shared<ArrowCStructColumnarBatch>(std::move(target_schema), std::move(target_array));
  return gluten_columnarbatch_holder_.Insert(batch);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_columnarbatch_ColumnarBatchJniWrapper_close(JNIEnv* env, jobject, jlong handle) {
  JNI_METHOD_START
  std::shared_ptr<ColumnarBatch> batch = gluten_columnarbatch_holder_.Lookup(handle);
  gluten_columnarbatch_holder_.Erase(handle);
  JNI_METHOD_END()
}

// Shuffle
JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_nativeMake(
    JNIEnv* env,
    jobject,
    jstring partitioning_name_jstr,
    jint num_partitions,
    jlong offheap_per_task,
    jint buffer_size,
    jstring compression_type_jstr,
    jint batch_compress_threshold,
    jstring data_file_jstr,
    jint num_sub_dirs,
    jstring local_dirs_jstr,
    jboolean prefer_evict,
    jlong allocator_id,
    jboolean write_schema,
    jlong firstBatchHandle,
    jlong taskAttemptId,
    jint push_buffer_max_size,
    jobject celeborn_partition_pusher,
    jstring partition_writer_type_jstr) {
  JNI_METHOD_START
  if (partitioning_name_jstr == nullptr) {
    gluten::JniThrow(std::string("Short partitioning name can't be null"));
    return 0;
  }

  auto partitioning_name = JStringToCString(env, partitioning_name_jstr);

  auto splitOptions = SplitOptions::Defaults();
  splitOptions.partitioning_name = partitioning_name;
  splitOptions.buffered_write = true;
  if (buffer_size > 0) {
    splitOptions.buffer_size = buffer_size;
  }
  splitOptions.offheap_per_task = offheap_per_task;

  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      splitOptions.compression_type = compression_type_result.MoveValueUnsafe();
    }
  }

  auto* allocator = reinterpret_cast<MemoryAllocator*>(allocator_id);
  if (allocator == nullptr) {
    gluten::JniThrow("Memory pool does not exist or has been closed");
  }
  splitOptions.memory_pool = AsWrappedArrowMemoryPool(allocator);

  jclass cls = env->FindClass("java/lang/Thread");
  jmethodID mid = env->GetStaticMethodID(cls, "currentThread", "()Ljava/lang/Thread;");
  jobject thread = env->CallStaticObjectMethod(cls, mid);
  if (thread == NULL) {
    std::cerr << "Thread.currentThread() return NULL" << std::endl;
  } else {
    jmethodID mid_getid = GetMethodIDOrError(env, cls, "getId", "()J");
    jlong sid = env->CallLongMethod(thread, mid_getid);
    splitOptions.thread_id = (int64_t)sid;
  }

  splitOptions.task_attempt_id = (int64_t)taskAttemptId;
  splitOptions.batch_compress_threshold = batch_compress_threshold;

  auto partition_writer_type_c = env->GetStringUTFChars(partition_writer_type_jstr, JNI_FALSE);
  auto partition_writer_type = std::string(partition_writer_type_c);
  env->ReleaseStringUTFChars(partition_writer_type_jstr, partition_writer_type_c);
  if (partition_writer_type == "local") {
    splitOptions.partition_writer_type = "local";
    if (data_file_jstr == NULL) {
      gluten::JniThrow(std::string("Shuffle DataFile can't be null"));
    }
    if (local_dirs_jstr == NULL) {
      gluten::JniThrow(std::string("Shuffle DataFile can't be null"));
    }

    splitOptions.write_schema = write_schema;
    splitOptions.prefer_evict = prefer_evict;

    if (num_sub_dirs > 0) {
      splitOptions.num_sub_dirs = num_sub_dirs;
    }

    auto data_file_c = env->GetStringUTFChars(data_file_jstr, JNI_FALSE);
    splitOptions.data_file = std::string(data_file_c);
    env->ReleaseStringUTFChars(data_file_jstr, data_file_c);

    auto local_dirs = env->GetStringUTFChars(local_dirs_jstr, JNI_FALSE);
    setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs, 1);
    env->ReleaseStringUTFChars(local_dirs_jstr, local_dirs);
  } else if (partition_writer_type == "celeborn") {
    splitOptions.partition_writer_type = "celeborn";
    jclass celeborn_partition_pusher_class =
        CreateGlobalClassReferenceOrError(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
    jmethodID celeborn_push_partition_data_method =
        GetMethodIDOrError(env, celeborn_partition_pusher_class, "pushPartitionData", "(I[B)I");
    if (push_buffer_max_size > 0) {
      splitOptions.push_buffer_max_size = push_buffer_max_size;
    }
    JavaVM* vm;
    if (env->GetJavaVM(&vm) != JNI_OK) {
      gluten::JniThrow("Unable to get JavaVM instance");
    }
    std::shared_ptr<CelebornClient> celeborn_client =
        std::make_shared<CelebornClient>(vm, celeborn_partition_pusher, celeborn_push_partition_data_method);
    splitOptions.celeborn_client = std::move(celeborn_client);
  }

  auto backend = gluten::CreateBackend();
  auto batch = gluten_columnarbatch_holder_.Lookup(firstBatchHandle);
  auto shuffle_writer = backend->makeShuffleWriter(num_partitions, std::move(splitOptions), batch->GetType());

  return shuffle_writer_holder_.Insert(shuffle_writer);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_nativeEvict(
    JNIEnv* env,
    jobject,
    jlong shuffle_writer_id,
    jlong size,
    jboolean callBySelf) {
  JNI_METHOD_START
  auto shuffle_writer = shuffle_writer_holder_.Lookup(shuffle_writer_id);
  if (!shuffle_writer) {
    std::string error_message = "Invalid shuffle writer id " + std::to_string(shuffle_writer_id);
    gluten::JniThrow(error_message);
  }
  jlong evicted_size;
  gluten::JniAssertOkOrThrow(
      shuffle_writer->EvictFixedSize(size, &evicted_size), "(shuffle) nativeEvict: evict failed");
  return evicted_size;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_split(
    JNIEnv* env,
    jobject,
    jlong shuffle_writer_id,
    jint num_rows,
    jlong handle) {
  JNI_METHOD_START
  auto shuffle_writer = shuffle_writer_holder_.Lookup(shuffle_writer_id);
  if (!shuffle_writer) {
    std::string error_message = "Invalid shuffle writer id " + std::to_string(shuffle_writer_id);
    gluten::JniThrow(error_message);
  }

  // The column batch maybe VeloxColumnBatch or ArrowCStructColumnarBatch(FallbackRangeShuffleWriter)
  std::shared_ptr<ColumnarBatch> batch = gluten_columnarbatch_holder_.Lookup(handle);
  auto numBytes = batch->GetBytes();
  gluten::JniAssertOkOrThrow(shuffle_writer->Split(batch.get()), "Native split: shuffle writer split failed");
  return numBytes;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_stop(JNIEnv* env, jobject, jlong shuffle_writer_id) {
  JNI_METHOD_START
  auto shuffle_writer = shuffle_writer_holder_.Lookup(shuffle_writer_id);
  if (!shuffle_writer) {
    std::string error_message = "Invalid shuffle writer id " + std::to_string(shuffle_writer_id);
    gluten::JniThrow(error_message);
  }

  gluten::JniAssertOkOrThrow(shuffle_writer->Stop(), "Native split: shuffle writer stop failed");

  const auto& partition_lengths = shuffle_writer->PartitionLengths();
  auto partition_length_arr = env->NewLongArray(partition_lengths.size());
  auto src = reinterpret_cast<const jlong*>(partition_lengths.data());
  env->SetLongArrayRegion(partition_length_arr, 0, partition_lengths.size(), src);

  const auto& raw_partition_lengths = shuffle_writer->RawPartitionLengths();
  auto raw_partition_length_arr = env->NewLongArray(raw_partition_lengths.size());
  auto raw_src = reinterpret_cast<const jlong*>(raw_partition_lengths.data());
  env->SetLongArrayRegion(raw_partition_length_arr, 0, raw_partition_lengths.size(), raw_src);

  jobject split_result = env->NewObject(
      split_result_class,
      split_result_constructor,
      0L,
      shuffle_writer->TotalWriteTime(),
      shuffle_writer->TotalEvictTime(),
      shuffle_writer->TotalCompressTime(),
      shuffle_writer->TotalBytesWritten(),
      shuffle_writer->TotalBytesEvicted(),
      partition_length_arr,
      raw_partition_length_arr);

  return split_result;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ShuffleWriterJniWrapper_close(JNIEnv* env, jobject, jlong shuffle_writer_id) {
  JNI_METHOD_START
  shuffle_writer_holder_.Erase(shuffle_writer_id);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_OnHeapJniByteInputStream_memCopyFromHeap(
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

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_make(JNIEnv* env, jclass, jobject jni_in, jlong c_schema) {
  JNI_METHOD_START
  std::shared_ptr<arrow::io::InputStream> in = std::make_shared<JavaInputStreamAdaptor>(env, jni_in);
  ReaderOptions options = ReaderOptions::Defaults();
  options.ipc_read_options.use_threads = false;
  std::shared_ptr<arrow::Schema> schema =
      gluten::JniGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(c_schema)));
  auto reader = std::make_shared<Reader>(in, schema, options);
  return shuffle_reader_holder_.Insert(reader);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_next(JNIEnv* env, jclass, jlong handle) {
  JNI_METHOD_START
  auto reader = shuffle_reader_holder_.Lookup(handle);
  GLUTEN_ASSIGN_OR_THROW(auto gluten_batch, reader->Next())
  if (gluten_batch == nullptr) {
    return -1L;
  }

  return gluten_columnarbatch_holder_.Insert(gluten_batch);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ShuffleReaderJniWrapper_close(JNIEnv* env, jclass, jlong handle) {
  JNI_METHOD_START
  auto reader = shuffle_reader_holder_.Lookup(handle);
  GLUTEN_THROW_NOT_OK(reader->Close());
  shuffle_reader_holder_.Erase(handle);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_nativeInitDatasource(
    JNIEnv* env,
    jobject obj,
    jstring file_path,
    jstring file_name,
    jlong c_schema) {
  auto backend = gluten::CreateBackend();

  std::shared_ptr<Datasource> datasource = nullptr;

  if (c_schema == -1) {
    // Only inspect the schema and not write
    datasource = backend->GetDatasource(JStringToCString(env, file_path), JStringToCString(env, file_name), nullptr);
  } else {
    auto schema = gluten::JniGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(c_schema)));
    datasource = backend->GetDatasource(JStringToCString(env, file_path), JStringToCString(env, file_name), schema);
    datasource->Init(backend->GetConfMap());
  }

  int64_t instanceID = gluten_datasource_holder_.Insert(datasource);
  return instanceID;
}

JNIEXPORT void JNICALL Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_inspectSchema(
    JNIEnv* env,
    jobject obj,
    jlong instanceId,
    jlong cSchema) {
  JNI_METHOD_START
  auto datasource = gluten_datasource_holder_.Lookup(instanceId);
  auto schema = datasource->InspectSchema();
  GLUTEN_THROW_NOT_OK(arrow::ExportSchema(*schema.get(), reinterpret_cast<struct ArrowSchema*>(cSchema)));
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_close(
    JNIEnv* env,
    jobject obj,
    jlong instanceId) {
  JNI_METHOD_START
  auto datasource = gluten_datasource_holder_.Lookup(instanceId);
  datasource->Close();
  gluten_datasource_holder_.Erase(instanceId);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_spark_sql_execution_datasources_velox_DatasourceJniWrapper_write(
    JNIEnv* env,
    jobject obj,
    jlong instanceId,
    jobject iter) {
  JNI_METHOD_START
  auto backend = gluten::CreateBackend();

  while (env->CallBooleanMethod(iter, velox_columnarbatch_scanner_hasNext)) {
    jlong handler = env->CallLongMethod(iter, velox_columnarbatch_scanner_next);
    auto batch = gluten_columnarbatch_holder_.Lookup(handler);
    gluten_datasource_holder_.Lookup(instanceId)->Write(batch);
    gluten_columnarbatch_holder_.Erase(handler);
  }

  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_getDefaultAllocator(JNIEnv* env, jclass) {
  JNI_METHOD_START
  return default_memory_allocator_id;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_createListenableAllocator(
    JNIEnv* env,
    jclass,
    jobject jlistener) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    gluten::JniThrow("Unable to get JavaVM instance");
  }
  std::shared_ptr<AllocationListener> listener = std::make_shared<SparkAllocationListener>(
      vm, jlistener, reserve_memory_method, unreserve_memory_method, 8L << 10 << 10);
  auto allocator = new ListenableMemoryAllocator(DefaultMemoryAllocator().get(), listener);
  return (jlong)(allocator);
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_releaseAllocator(JNIEnv* env, jclass, jlong allocator_id) {
  JNI_METHOD_START
  if (allocator_id == default_memory_allocator_id) {
    return;
  }
  delete (MemoryAllocator*)(allocator_id);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_memory_alloc_NativeMemoryAllocator_bytesAllocated(JNIEnv* env, jclass, jlong allocator_id) {
  JNI_METHOD_START
  auto* alloc = (MemoryAllocator*)(allocator_id);
  if (alloc == nullptr) {
    gluten::JniThrow("Memory allocator instance not found. It may not exist nor has been closed");
  }
  return alloc->GetBytes();
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL Java_io_glutenproject_tpc_MallocUtils_mallocTrim(JNIEnv* env, jobject obj) {
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_trim... " << std::endl;
  malloc_trim(0);
}

JNIEXPORT void JNICALL Java_io_glutenproject_tpc_MallocUtils_mallocStats(JNIEnv* env, jobject obj) {
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_stats... " << std::endl;
  malloc_stats();
}

#ifdef __cplusplus
}
#endif
