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

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/jniutil/jni_util.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/util/compression.h>
#include <arrow/util/iterator.h>
#include <jni.h>
#include <malloc.h>

#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "compute/protobuf_utils.h"
#include "jni/concurrent_map.h"
#include "jni/jni_common.h"
#include "operators/c2r/columnar_to_row_converter.h"
#include "operators/c2r/velox_to_row_converter.h"
#include "operators/shuffle/splitter.h"
#include "utils/exception.h"
#include "utils/result_iterator.h"

namespace {

#define JNI_METHOD_START try {
// macro ended

#define JNI_METHOD_END(fallback_expr)                 \
  }                                                   \
  catch (JniPendingException & e) {                   \
    env->ThrowNew(runtime_exception_class, e.what()); \
    return fallback_expr;                             \
  }
// macro ended

void ThrowPendingException(const std::string& message) {
  throw JniPendingException(message);
}

template <typename T>
T JniGetOrThrow(arrow::Result<T> result) {
  if (!result.status().ok()) {
    ThrowPendingException(result.status().message());
  }
  return std::move(result).ValueOrDie();
}

template <typename T>
T JniGetOrThrow(arrow::Result<T> result, const std::string& message) {
  if (!result.status().ok()) {
    ThrowPendingException(message + " - " + result.status().message());
  }
  return std::move(result).ValueOrDie();
}

void JniAssertOkOrThrow(arrow::Status status) {
  if (!status.ok()) {
    ThrowPendingException(status.message());
  }
}

void JniAssertOkOrThrow(arrow::Status status, const std::string& message) {
  if (!status.ok()) {
    ThrowPendingException(message + " - " + status.message());
  }
}

void JniThrow(const std::string& message) { ThrowPendingException(message); }

}  // namespace

namespace types {
class ExpressionList;
}  // namespace types

static jclass serializable_obj_builder_class;
static jmethodID serializable_obj_builder_constructor;

static jclass byte_array_class;

static jclass split_result_class;
static jmethodID split_result_constructor;

static jclass serialized_record_batch_iterator_class;
static jclass metrics_builder_class;
static jmethodID metrics_builder_constructor;

static jmethodID serialized_record_batch_iterator_hasNext;
static jmethodID serialized_record_batch_iterator_next;

static jclass columnar_to_row_info_class;
static jmethodID columnar_to_row_info_constructor;

using arrow::jni::ConcurrentMap;

static jint JNI_VERSION = JNI_VERSION_1_8;

using ColumnarToRowConverter = gazellejni::columnartorow::ColumnarToRowConverter;
using VeloxToRowConverter = gazellejni::columnartorow::VeloxToRowConverter;
static arrow::jni::ConcurrentMap<std::shared_ptr<ResultIteratorBase>>
    batch_iterator_holder_;

static arrow::jni::ConcurrentMap<std::shared_ptr<ColumnarToRowConverter>>
    columnar_to_row_converter_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<VeloxToRowConverter>>
    velox_to_row_converter_holder_;

using gazellejni::shuffle::SplitOptions;
using gazellejni::shuffle::Splitter;
static arrow::jni::ConcurrentMap<std::shared_ptr<Splitter>> shuffle_splitter_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<arrow::Schema>>
    decompression_schema_holder_;

std::shared_ptr<ResultIteratorBase> GetBatchIterator(JNIEnv* env, jlong id) {
  auto handler = batch_iterator_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  return handler;
}

template <typename T>
std::shared_ptr<ResultIterator<T>> GetBatchIterator(JNIEnv* env, jlong id) {
  auto handler = GetBatchIterator(env, id);
  return std::dynamic_pointer_cast<ResultIterator<T>>(handler);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> FromBytes(
    JNIEnv* env, std::shared_ptr<arrow::Schema> schema, jbyteArray bytes) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch,
                        arrow::jniutil::DeserializeUnsafeFromJava(env, schema, bytes))
  return batch;
}

arrow::Result<jbyteArray> ToBytes(JNIEnv* env,
                                  std::shared_ptr<arrow::RecordBatch> batch) {
  ARROW_ASSIGN_OR_RAISE(jbyteArray bytes,
                        arrow::jniutil::SerializeUnsafeFromNative(env, batch))
  return bytes;
}

class JavaRecordBatchIterator {
 public:
  explicit JavaRecordBatchIterator(JavaVM* vm,
                                   jobject java_serialized_record_batch_iterator,
                                   std::shared_ptr<arrow::Schema> schema)
      : vm_(vm),
        java_serialized_record_batch_iterator_(java_serialized_record_batch_iterator),
        schema_(std::move(schema)) {}

  // singleton, avoid stack instantiation
  JavaRecordBatchIterator(const JavaRecordBatchIterator& itr) = delete;
  JavaRecordBatchIterator(JavaRecordBatchIterator&& itr) = delete;

  virtual ~JavaRecordBatchIterator() {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) == JNI_OK) {
#ifdef DEBUG
      std::cout << "DELETING GLOBAL ITERATOR REF "
                << reinterpret_cast<long>(java_serialized_record_batch_iterator_) << "..."
                << std::endl;
#endif
      env->DeleteGlobalRef(java_serialized_record_batch_iterator_);
    }
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    JNIEnv* env;
    int getEnvStat = vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
    if (getEnvStat == JNI_EDETACHED) {
#ifdef DEBUG
      std::cout << "JNIEnv was not attached to current thread." << std::endl;
#endif
      if (vm_->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL) != 0) {
        return arrow::Status::Invalid("Failed to attach thread.");
      } else {
#ifdef DEBUG
        std::cout << "Succeeded attaching current thread." << std::endl;
#endif
      }
    } else if (getEnvStat != JNI_OK) {
      return arrow::Status::Invalid("JNIEnv was not attached to current thread");
    }
#ifdef DEBUG
    std::cout << "PICKING ITERATOR REF "
              << reinterpret_cast<long>(java_serialized_record_batch_iterator_) << "..."
              << std::endl;
#endif
    if (!env->CallBooleanMethod(java_serialized_record_batch_iterator_,
                                serialized_record_batch_iterator_hasNext)) {
      return nullptr;  // stream ended
    }
    auto bytes = (jbyteArray)env->CallObjectMethod(java_serialized_record_batch_iterator_,
                                                   serialized_record_batch_iterator_next);
    RETURN_NOT_OK(arrow::jniutil::CheckException(env));
    ARROW_ASSIGN_OR_RAISE(auto batch, FromBytes(env, schema_, bytes));
    // vm_->DetachCurrentThread();
    return batch;
  }

 private:
  JavaVM* vm_;
  jobject java_serialized_record_batch_iterator_;
  std::shared_ptr<arrow::Schema> schema_;
};

class JavaRecordBatchIteratorWrapper {
 public:
  explicit JavaRecordBatchIteratorWrapper(
      std::shared_ptr<JavaRecordBatchIterator> delegated)
      : delegated_(std::move(delegated)) {}

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() { return delegated_->Next(); }

 private:
  std::shared_ptr<JavaRecordBatchIterator> delegated_;
};

// See Java class
// org/apache/arrow/dataset/jni/NativeSerializedRecordBatchIterator
//
arrow::Result<arrow::RecordBatchIterator> MakeJavaRecordBatchIterator(
    JavaVM* vm, jobject java_serialized_record_batch_iterator,
    std::shared_ptr<arrow::Schema> schema) {
  std::shared_ptr<arrow::Schema> schema_moved = std::move(schema);
  arrow::RecordBatchIterator itr = arrow::Iterator<std::shared_ptr<arrow::RecordBatch>>(
      JavaRecordBatchIteratorWrapper(std::make_shared<JavaRecordBatchIterator>(
          vm, java_serialized_record_batch_iterator, schema_moved)));
  return itr;
}

using FileSystem = arrow::fs::FileSystem;

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }

  io_exception_class = CreateGlobalClassReference(env, "Ljava/io/IOException;");
  runtime_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");
  unsupportedoperation_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/UnsupportedOperationException;");
  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");

  serializable_obj_builder_class = CreateGlobalClassReference(
      env, "Lcom/intel/oap/vectorized/NativeSerializableObject;");
  serializable_obj_builder_constructor =
      GetMethodID(env, serializable_obj_builder_class, "<init>", "([J[I)V");

  byte_array_class = CreateGlobalClassReference(env, "[B");
  split_result_class =
      CreateGlobalClassReference(env, "Lcom/intel/oap/vectorized/SplitResult;");
  split_result_constructor =
      GetMethodID(env, split_result_class, "<init>", "(JJJJJJ[J[J)V");

  metrics_builder_class =
      CreateGlobalClassReference(env, "Lcom/intel/oap/vectorized/MetricsObject;");
  metrics_builder_constructor =
      GetMethodID(env, metrics_builder_class, "<init>", "([J[J)V");

  serialized_record_batch_iterator_class =
      CreateGlobalClassReference(env, "Lcom/intel/oap/execution/ColumnarNativeIterator;");
  serialized_record_batch_iterator_hasNext =
      GetMethodID(env, serialized_record_batch_iterator_class, "hasNext", "()Z");
  serialized_record_batch_iterator_next =
      GetMethodID(env, serialized_record_batch_iterator_class, "next", "()[B");

  columnar_to_row_info_class =
      CreateGlobalClassReference(env, "Lcom/intel/oap/vectorized/ColumnarToRowInfo;");
  columnar_to_row_info_constructor =
      GetMethodID(env, columnar_to_row_info_class, "<init>", "(J[J[JJ)V");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  env->DeleteGlobalRef(io_exception_class);
  env->DeleteGlobalRef(runtime_exception_class);
  env->DeleteGlobalRef(unsupportedoperation_exception_class);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);

  env->DeleteGlobalRef(serializable_obj_builder_class);
  env->DeleteGlobalRef(split_result_class);
  env->DeleteGlobalRef(serialized_record_batch_iterator_class);
  env->DeleteGlobalRef(columnar_to_row_info_class);

  env->DeleteGlobalRef(byte_array_class);

  batch_iterator_holder_.Clear();
  columnar_to_row_converter_holder_.Clear();
  velox_to_row_converter_holder_.Clear();
  shuffle_splitter_holder_.Clear();
  decompression_schema_holder_.Clear();
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetJavaTmpDir(
    JNIEnv* env, jobject obj, jstring pathObj) {
  JNI_METHOD_START
  jboolean ifCopy;
  auto path = env->GetStringUTFChars(pathObj, &ifCopy);
  setenv("NATIVESQL_TMP_DIR", path, 1);
  env->ReleaseStringUTFChars(pathObj, path);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetBatchSize(
    JNIEnv* env, jobject obj, jint batch_size) {
  setenv("NATIVESQL_BATCH_SIZE", std::to_string(batch_size).c_str(), 1);
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMetricsTime(
    JNIEnv* env, jobject obj, jboolean is_enable) {
  setenv("NATIVESQL_METRICS_TIME", (is_enable ? "true" : "false"), 1);
}

JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithIterator(
    JNIEnv* env, jobject obj, jlong memory_pool_id, jbyteArray ws_exprs_arr,
    jobjectArray iter_arr) {
  JNI_METHOD_START
  arrow::Status msg;
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    std::string error_message = "Unable to get JavaVM instance";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  // Get Substrait Plan.
  substrait::Plan subPlan;
  getSubstraitPlan<substrait::Plan>(env, ws_exprs_arr, &subPlan);
  // Parse the plan and get the input schema for Java iters.
  jsize iters_len = env->GetArrayLength(iter_arr);
  std::vector<arrow::RecordBatchIterator> arrow_iters;
  if (iters_len > 0) {
    // Construct a map between iter index and input schema.
    std::unordered_map<uint64_t, std::shared_ptr<arrow::Schema>> schemaMap;
    // Get input schema from Substrait plan.
    getIterInputSchema(subPlan, schemaMap);
    for (uint64_t idx = 0; idx < iters_len; idx++) {
      jobject iter = env->GetObjectArrayElement(iter_arr, idx);
      // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
      // TODO Release this in JNI Unload or dependent object's destructor
      jobject ref_iter = env->NewGlobalRef(iter);
      // FIXME: Schema should be obtained from Substrait plan.
      std::shared_ptr<arrow::Schema> inputSchema;
      if (schemaMap.find(idx) == schemaMap.end()) {
        std::cout << "Not found the schema for index: " << idx << std::endl;
      } else {
        inputSchema = schemaMap[idx];
      }
      arrow::RecordBatchIterator arrow_iter =
          JniGetOrThrow(MakeJavaRecordBatchIterator(vm, ref_iter, inputSchema),
                        "nativeCreateKernelWithIterator: error making java iterator");
      arrow_iters.push_back(std::move(arrow_iter));
    }
  }
  // Get the ws iter
  gandiva::ExpressionVector ws_expr_vector;
  gandiva::FieldVector ws_ret_types;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> res_iter;
  getSubstraitPlanIter(subPlan, std::move(arrow_iters), &res_iter);
  auto ws_result_iterator = std::dynamic_pointer_cast<ResultIteratorBase>(res_iter);
  return batch_iterator_holder_.Insert(std::move(ws_result_iterator));
  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative(
    JNIEnv* env, jobject obj) {
  JNI_METHOD_START
  InitVelox();
  JNI_METHOD_END()
}

JNIEXPORT jboolean JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeHasNext(
    JNIEnv* env, jobject obj, jlong id) {
  JNI_METHOD_START
  auto iter = GetBatchIterator(env, id);
  if (iter == nullptr) {
    std::string error_message = "faked to get batch iterator";
    JniThrow(error_message);
  }
  return iter->HasNext();
  JNI_METHOD_END(false)
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeNext(
    JNIEnv* env, jobject obj, jlong id) {
  JNI_METHOD_START
  auto iter = GetBatchIterator<arrow::RecordBatch>(env, id);
  std::shared_ptr<arrow::RecordBatch> out;
  if (!iter->HasNext()) return nullptr;
  JniAssertOkOrThrow(iter->Next(&out), "nativeNext: get Next() failed");
  // arrow::PrettyPrint(*out.get(), 2, &std::cout);
  jbyteArray serialized_record_batch =
      JniGetOrThrow(ToBytes(env, out), "Error deserializing message");
  return serialized_record_batch;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeClose(
    JNIEnv* env, jobject this_obj, jlong id) {
  JNI_METHOD_START
#ifdef DEBUG
  auto it = batch_iterator_holder_.Lookup(id);
  if (it.use_count() > 2) {
    std::cout << it->ToString() << " ptr use count is " << it.use_count() << std::endl;
  }
#endif
  batch_iterator_holder_.Erase(id);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ColumnarToRowJniWrapper_nativeConvertArrowColumnarToRow(
    JNIEnv* env, jobject, jbyteArray schema_arr, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jlong memory_pool_id) {
  JNI_METHOD_START
  if (schema_arr == NULL) {
    JniThrow("Native convert columnar to row schema can't be null");
  }
  if (buf_addrs == NULL) {
    JniThrow("Native convert columnar to row: buf_addrs can't be null");
  }
  if (buf_sizes == NULL) {
    JniThrow("Native convert columnar to row: buf_sizes can't be null");
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    JniThrow(
        "Native convert columnar to row: length of buf_addrs and buf_sizes mismatch");
  }

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> rb;
  JniAssertOkOrThrow(MakeRecordBatch(schema, num_rows, (int64_t*)in_buf_addrs,
                                     (int64_t*)in_buf_sizes, in_bufs_len, &rb),
                     "Native convert columnar to row: make record batch failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  // convert the record batch to spark unsafe row.
  auto* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    JniThrow("Memory pool does not exist or has been closed");
  }

  std::shared_ptr<ColumnarToRowConverter> columnar_to_row_converter =
      std::make_shared<ColumnarToRowConverter>(rb, pool);
  JniAssertOkOrThrow(columnar_to_row_converter->Init(),
                     "Native convert columnar to row: Init "
                     "ColumnarToRowConverter failed");
  JniAssertOkOrThrow(
      columnar_to_row_converter->Write(),
      "Native convert columnar to row: ColumnarToRowConverter write failed");

  const auto& offsets = columnar_to_row_converter->GetOffsets();
  const auto& lengths = columnar_to_row_converter->GetLengths();
  int64_t instanceID =
      columnar_to_row_converter_holder_.Insert(columnar_to_row_converter);

  auto offsets_arr = env->NewLongArray(num_rows);
  auto offsets_src = reinterpret_cast<const jlong*>(offsets.data());
  env->SetLongArrayRegion(offsets_arr, 0, num_rows, offsets_src);
  auto lengths_arr = env->NewLongArray(num_rows);
  auto lengths_src = reinterpret_cast<const jlong*>(lengths.data());
  env->SetLongArrayRegion(lengths_arr, 0, num_rows, lengths_src);
  long address = reinterpret_cast<long>(columnar_to_row_converter->GetBufferAddress());

  jobject arrow_columnar_to_row_info =
      env->NewObject(columnar_to_row_info_class, columnar_to_row_info_constructor,
                     instanceID, offsets_arr, lengths_arr, address);
  return arrow_columnar_to_row_info;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ColumnarToRowJniWrapper_nativeConvertVeloxColumnarToRow(
    JNIEnv* env, jobject, jbyteArray schema_arr, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jlong memory_pool_id) {
  JNI_METHOD_START
  if (schema_arr == NULL) {
    JniThrow("Native convert columnar to row schema can't be null");
  }
  if (buf_addrs == NULL) {
    JniThrow("Native convert columnar to row: buf_addrs can't be null");
  }
  if (buf_sizes == NULL) {
    JniThrow("Native convert columnar to row: buf_sizes can't be null");
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    JniThrow(
        "Native convert columnar to row: length of buf_addrs and buf_sizes mismatch");
  }

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> rb;
  JniAssertOkOrThrow(MakeRecordBatch(schema, num_rows, (int64_t*)in_buf_addrs,
                                     (int64_t*)in_buf_sizes, in_bufs_len, &rb),
                     "Native convert columnar to row: make record batch failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  // convert the record batch to spark unsafe row.

  auto* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    JniThrow("Memory pool does not exist or has been closed");
  }

  std::shared_ptr<VeloxToRowConverter> velox_to_row_converter =
      std::make_shared<VeloxToRowConverter>(rb, pool);
  JniAssertOkOrThrow(velox_to_row_converter->Init(),
                     "Native convert columnar to row: Init "
                     "ColumnarToRowConverter failed");
  velox_to_row_converter->Write();
  const auto& offsets = velox_to_row_converter->GetOffsets();
  const auto& lengths = velox_to_row_converter->GetLengths();
  int64_t instanceID = velox_to_row_converter_holder_.Insert(velox_to_row_converter);

  auto offsets_arr = env->NewLongArray(num_rows);
  auto offsets_src = reinterpret_cast<const jlong*>(offsets.data());
  env->SetLongArrayRegion(offsets_arr, 0, num_rows, offsets_src);
  auto lengths_arr = env->NewLongArray(num_rows);
  auto lengths_src = reinterpret_cast<const jlong*>(lengths.data());
  env->SetLongArrayRegion(lengths_arr, 0, num_rows, lengths_src);
  long address = reinterpret_cast<long>(velox_to_row_converter->GetBufferAddress());

  jobject velox_columnar_to_row_info =
      env->NewObject(columnar_to_row_info_class, columnar_to_row_info_constructor,
                     instanceID, offsets_arr, lengths_arr, address);
  return velox_columnar_to_row_info;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ColumnarToRowJniWrapper_nativeClose(
    JNIEnv* env, jobject, jlong instance_id) {
  JNI_METHOD_START
  velox_to_row_converter_holder_.Erase(instance_id);
  JNI_METHOD_END()
}

// Shuffle
JNIEXPORT jlong JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_nativeMake(
    JNIEnv* env, jobject, jstring partitioning_name_jstr, jint num_partitions,
    jbyteArray schema_arr, jbyteArray expr_arr, jlong offheap_per_task, jint buffer_size,
    jstring compression_type_jstr, jint batch_compress_threshold, jstring data_file_jstr,
    jint num_sub_dirs, jstring local_dirs_jstr, jboolean prefer_spill,
    jlong memory_pool_id, jboolean write_schema) {
  JNI_METHOD_START
  if (partitioning_name_jstr == NULL) {
    JniThrow(std::string("Short partitioning name can't be null"));
    return 0;
  }
  if (schema_arr == NULL) {
    JniThrow(std::string("Make splitter schema can't be null"));
  }
  if (data_file_jstr == NULL) {
    JniThrow(std::string("Shuffle DataFile can't be null"));
  }
  if (local_dirs_jstr == NULL) {
    JniThrow(std::string("Shuffle DataFile can't be null"));
  }

  auto partitioning_name_c = env->GetStringUTFChars(partitioning_name_jstr, JNI_FALSE);
  auto partitioning_name = std::string(partitioning_name_c);
  env->ReleaseStringUTFChars(partitioning_name_jstr, partitioning_name_c);

  auto splitOptions = SplitOptions::Defaults();
  splitOptions.write_schema = write_schema;
  splitOptions.prefer_spill = prefer_spill;
  splitOptions.buffered_write = true;
  if (buffer_size > 0) {
    splitOptions.buffer_size = buffer_size;
  }
  splitOptions.offheap_per_task = offheap_per_task;

  if (num_sub_dirs > 0) {
    splitOptions.num_sub_dirs = num_sub_dirs;
  }

  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      splitOptions.compression_type = compression_type_result.MoveValueUnsafe();
    }
  }

  auto data_file_c = env->GetStringUTFChars(data_file_jstr, JNI_FALSE);
  splitOptions.data_file = std::string(data_file_c);
  env->ReleaseStringUTFChars(data_file_jstr, data_file_c);

  auto* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    JniThrow("Memory pool does not exist or has been closed");
  }
  splitOptions.memory_pool = pool;

  auto local_dirs = env->GetStringUTFChars(local_dirs_jstr, JNI_FALSE);
  setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs, 1);
  env->ReleaseStringUTFChars(local_dirs_jstr, local_dirs);

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  substrait::Rel subRel;
  if (expr_arr != NULL) {
    JniAssertOkOrThrow(getSubstraitPlan<substrait::Rel>(env, expr_arr, &subRel),
                       "Failed to parse expressions protobuf");
  }
  jclass cls = env->FindClass("java/lang/Thread");
  jmethodID mid = env->GetStaticMethodID(cls, "currentThread", "()Ljava/lang/Thread;");
  jobject thread = env->CallStaticObjectMethod(cls, mid);
  if (thread == NULL) {
    std::cout << "Thread.currentThread() return NULL" << std::endl;
  } else {
    jmethodID mid_getid = env->GetMethodID(cls, "getId", "()J");
    jlong sid = env->CallLongMethod(thread, mid_getid);
    splitOptions.thread_id = (int64_t)sid;
  }

  jclass tc_cls = env->FindClass("org/apache/spark/TaskContext");
  jmethodID get_tc_mid =
      env->GetStaticMethodID(tc_cls, "get", "()Lorg/apache/spark/TaskContext;");
  jobject tc_obj = env->CallStaticObjectMethod(tc_cls, get_tc_mid);
  if (tc_obj == NULL) {
    std::cout << "TaskContext.get() return NULL" << std::endl;
  } else {
    jmethodID get_tsk_attmpt_mid = env->GetMethodID(tc_cls, "taskAttemptId", "()J");
    jlong attmpt_id = env->CallLongMethod(tc_obj, get_tsk_attmpt_mid);
    splitOptions.task_attempt_id = (int64_t)attmpt_id;
  }
  splitOptions.batch_compress_threshold = batch_compress_threshold;

  auto splitter =
      JniGetOrThrow(Splitter::Make(partitioning_name, std::move(schema), num_partitions,
                                   subRel, std::move(splitOptions)),
                    "Failed create native shuffle splitter");

  return shuffle_splitter_holder_.Insert(std::shared_ptr<Splitter>(splitter));

  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_setCompressType(
    JNIEnv* env, jobject, jlong splitter_id, jstring compression_type_jstr) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    JniThrow(error_message);
  }

  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      JniAssertOkOrThrow(
          splitter->SetCompressType(compression_type_result.MoveValueUnsafe()));
    }
  }
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_split(
    JNIEnv* env, jobject, jlong splitter_id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jboolean first_record_batch) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    JniThrow(error_message);
  }
  if (buf_addrs == NULL) {
    JniThrow("Native split: buf_addrs can't be null");
  }
  if (buf_sizes == NULL) {
    JniThrow("Native split: buf_sizes can't be null");
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    JniThrow("Native split: length of buf_addrs and buf_sizes mismatch");
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> in;
  JniAssertOkOrThrow(
      MakeRecordBatch(splitter->input_schema(), num_rows, (int64_t*)in_buf_addrs,
                      (int64_t*)in_buf_sizes, in_bufs_len, &in),
      "Native split: make record batch failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  if (first_record_batch) {
    return splitter->CompressedSize(*in);
  }
  JniAssertOkOrThrow(splitter->Split(*in), "Native split: splitter split failed");
  return -1L;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_stop(
    JNIEnv* env, jobject, jlong splitter_id) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    JniThrow(error_message);
  }

  JniAssertOkOrThrow(splitter->Stop(), "Native split: splitter stop failed");

  const auto& partition_lengths = splitter->PartitionLengths();
  auto partition_length_arr = env->NewLongArray(partition_lengths.size());
  auto src = reinterpret_cast<const jlong*>(partition_lengths.data());
  env->SetLongArrayRegion(partition_length_arr, 0, partition_lengths.size(), src);

  const auto& raw_partition_lengths = splitter->RawPartitionLengths();
  auto raw_partition_length_arr = env->NewLongArray(raw_partition_lengths.size());
  auto raw_src = reinterpret_cast<const jlong*>(raw_partition_lengths.data());
  env->SetLongArrayRegion(raw_partition_length_arr, 0, raw_partition_lengths.size(),
                          raw_src);

  jobject split_result = env->NewObject(
      split_result_class, split_result_constructor, splitter->TotalComputePidTime(),
      splitter->TotalWriteTime(), splitter->TotalSpillTime(),
      splitter->TotalCompressTime(), splitter->TotalBytesWritten(),
      splitter->TotalBytesSpilled(), partition_length_arr, raw_partition_length_arr);

  return split_result;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ShuffleSplitterJniWrapper_close(
    JNIEnv* env, jobject, jlong splitter_id) {
  JNI_METHOD_START
  shuffle_splitter_holder_.Erase(splitter_id);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_make(
    JNIEnv* env, jobject, jbyteArray schema_arr) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  JniAssertOkOrThrow(MakeSchema(env, schema_arr, &schema));

  return decompression_schema_holder_.Insert(schema);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_decompress(
    JNIEnv* env, jobject obj, jlong schema_holder_id, jstring compression_type_jstr,
    jint num_rows, jlongArray buf_addrs, jlongArray buf_sizes, jlongArray buf_mask) {
  JNI_METHOD_START
  auto schema = decompression_schema_holder_.Lookup(schema_holder_id);
  if (!schema) {
    std::string error_message =
        "Invalid schema holder id " + std::to_string(schema_holder_id);
    JniThrow(error_message);
  }
  if (buf_addrs == NULL) {
    JniThrow("Native decompress: buf_addrs can't be null");
  }
  if (buf_sizes == NULL) {
    JniThrow("Native decompress: buf_sizes can't be null");
  }
  if (buf_mask == NULL) {
    JniThrow("Native decompress: buf_mask can't be null");
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    JniThrow("Native decompress: length of buf_addrs and buf_sizes mismatch");
  }

  auto compression_type = arrow::Compression::UNCOMPRESSED;
  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      compression_type = compression_type_result.MoveValueUnsafe();
    }
  }

  // make buffers from raws
  auto in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  auto in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);
  auto in_buf_mask = env->GetLongArrayElements(buf_mask, JNI_FALSE);

  std::vector<std::shared_ptr<arrow::Buffer>> input_buffers;
  input_buffers.reserve(in_bufs_len);
  for (auto buffer_idx = 0; buffer_idx < in_bufs_len; buffer_idx++) {
    input_buffers.push_back(std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(in_buf_addrs[buffer_idx]),
        in_buf_sizes[buffer_idx]));
  }

  // decompress buffers
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.use_threads = false;
  JniAssertOkOrThrow(
      DecompressBuffers(compression_type, options, (uint8_t*)in_buf_mask, input_buffers,
                        schema->fields()),
      "ShuffleDecompressionJniWrapper_decompress, failed to decompress buffers");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_mask, in_buf_mask, JNI_ABORT);

  // make arrays from buffers
  std::shared_ptr<arrow::RecordBatch> rb;
  JniAssertOkOrThrow(
      MakeRecordBatch(schema, num_rows, input_buffers, input_buffers.size(), &rb),
      "ShuffleDecompressionJniWrapper_decompress, failed to MakeRecordBatch upon "
      "buffers");
  jbyteArray serialized_record_batch =
      JniGetOrThrow(ToBytes(env, rb), "Error deserializing message");

  return serialized_record_batch;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_ShuffleDecompressionJniWrapper_close(
    JNIEnv* env, jobject, jlong schema_holder_id) {
  decompression_schema_holder_.Erase(schema_holder_id);
}

JNIEXPORT void JNICALL Java_com_intel_oap_tpc_MallocUtils_mallocTrim(JNIEnv* env,
                                                                     jobject obj) {
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_trim... " << std::endl;
  malloc_trim(0);
}

JNIEXPORT void JNICALL Java_com_intel_oap_tpc_MallocUtils_mallocStats(JNIEnv* env,
                                                                      jobject obj) {
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_stats... " << std::endl;
  malloc_stats();
}

#ifdef __cplusplus
}
#endif
