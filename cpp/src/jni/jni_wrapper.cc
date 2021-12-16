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

#include "common/result_iterator.h"
#include "jni/concurrent_map.h"
#include "jni/jni_common.h"
#include "operators/columnar_to_row_converter.h"
#include "proto/protobuf_utils.h"

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

class JniPendingException : public std::runtime_error {
 public:
  explicit JniPendingException(const std::string& arg) : runtime_error(arg) {}
};

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

static jclass arrow_columnar_to_row_info_class;
static jmethodID arrow_columnar_to_row_info_constructor;

using arrow::jni::ConcurrentMap;

static jint JNI_VERSION = JNI_VERSION_1_8;

using ColumnarToRowConverter = sparkcolumnarplugin::columnartorow::ColumnarToRowConverter;
static arrow::jni::ConcurrentMap<std::shared_ptr<ResultIteratorBase>>
    batch_iterator_holder_;

static arrow::jni::ConcurrentMap<std::shared_ptr<ColumnarToRowConverter>>
    columnar_to_row_converter_holder_;

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
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
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
      GetMethodID(env, split_result_class, "<init>", "(JJJJJJ[J)V");

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

  arrow_columnar_to_row_info_class = CreateGlobalClassReference(
      env, "Lcom/intel/oap/vectorized/ArrowColumnarToRowInfo;");
  arrow_columnar_to_row_info_constructor =
      GetMethodID(env, arrow_columnar_to_row_info_class, "<init>", "(J[J[JJ)V");

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
  env->DeleteGlobalRef(arrow_columnar_to_row_info_class);

  env->DeleteGlobalRef(byte_array_class);

  batch_iterator_holder_.Clear();
  columnar_to_row_converter_holder_.Clear();
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
    JNIEnv* env, jobject obj, jlong memory_pool_id, jbyteArray ws_in_schema_arr,
    jbyteArray ws_exprs_arr, jbyteArray ws_res_schema_arr, jbyteArray in_exprs_arr,
    jobject itr, jlongArray dep_ids, jboolean return_when_finish = false) {
  arrow::Status msg;
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    std::string error_message = "Unable to get JavaVM instance";
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  // Get input schema
  std::shared_ptr<arrow::Schema> ws_in_schema;
  msg = MakeSchema(env, ws_in_schema_arr, &ws_in_schema);
  if (!msg.ok()) {
    std::string error_message = "failed to readSchema, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  // Get the ws iter
  gandiva::ExpressionVector ws_expr_vector;
  gandiva::FieldVector ws_ret_types;
  std::cout << "start to parse" << std::endl;
  std::shared_ptr<ResultIterator<arrow::RecordBatch>> res_iter;
  msg = ParseSubstraitPlan(env, ws_exprs_arr, &ws_expr_vector, &ws_ret_types, &res_iter);
  if (!msg.ok()) {
    std::string error_message =
        "failed to parse expressions protobuf, err msg is " + msg.message();
    env->ThrowNew(io_exception_class, error_message.c_str());
  }
  auto ws_result_iterator = std::dynamic_pointer_cast<ResultIteratorBase>(res_iter);
  return batch_iterator_holder_.Insert(std::move(ws_result_iterator));
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
  jbyteArray serialized_record_batch =
      JniGetOrThrow(ToBytes(env, out), "Error deserializing message");
  return serialized_record_batch;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_com_intel_oap_vectorized_BatchIterator_nativeClose(
    JNIEnv* env, jobject this_obj, jlong id) {
#ifdef DEBUG
  auto it = batch_iterator_holder_.Lookup(id);
  if (it.use_count() > 2) {
    std::cout << it->ToString() << " ptr use count is " << it.use_count() << std::endl;
  }
#endif
  batch_iterator_holder_.Erase(id);
}

JNIEXPORT jobject JNICALL
Java_com_intel_oap_vectorized_ArrowColumnarToRowJniWrapper_nativeConvertColumnarToRow(
    JNIEnv* env, jobject, jbyteArray schema_arr, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jlong memory_pool_id) {
  if (schema_arr == NULL) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native convert columnar to row schema can't be null").c_str());
    return NULL;
  }
  if (buf_addrs == NULL) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native convert columnar to row: buf_addrs can't be null").c_str());
    return NULL;
  }
  if (buf_sizes == NULL) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string("Native convert columnar to row: buf_sizes can't be null").c_str());
    return NULL;
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    env->ThrowNew(
        illegal_argument_exception_class,
        std::string(
            "Native convert columnar to row: length of buf_addrs and buf_sizes mismatch")
            .c_str());
    return NULL;
  }

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> rb;
  auto status = MakeRecordBatch(schema, num_rows, (int64_t*)in_buf_addrs,
                                (int64_t*)in_buf_sizes, in_bufs_len, &rb);

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  if (!status.ok()) {
    env->ThrowNew(illegal_argument_exception_class,
                  std::string("Native convert columnar to row: make record batch failed, "
                              "error message is " +
                              status.message())
                      .c_str());
    return NULL;
  }

  // convert the record batch to spark unsafe row.
  try {
    auto* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
    if (pool == nullptr) {
      env->ThrowNew(illegal_argument_exception_class,
                    "Memory pool does not exist or has been closed");
      return NULL;
    }

    std::shared_ptr<ColumnarToRowConverter> columnar_to_row_converter =
        std::make_shared<ColumnarToRowConverter>(rb, pool);
    auto status = columnar_to_row_converter->Init();
    if (!status.ok()) {
      env->ThrowNew(illegal_argument_exception_class,
                    std::string("Native convert columnar to row: Init "
                                "ColumnarToRowConverter failed, error message is " +
                                status.message())
                        .c_str());
      return NULL;
    }
    status = columnar_to_row_converter->Write();
    if (!status.ok()) {
      env->ThrowNew(
          illegal_argument_exception_class,
          std::string("Native convert columnar to row: ColumnarToRowConverter write "
                      "failed, error message is " +
                      status.message())
              .c_str());
      return NULL;
    }

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

    jobject arrow_columnar_to_row_info = env->NewObject(
        arrow_columnar_to_row_info_class, arrow_columnar_to_row_info_constructor,
        instanceID, offsets_arr, lengths_arr, address);
    return arrow_columnar_to_row_info;
  } catch (const std::runtime_error& error) {
    env->ThrowNew(unsupportedoperation_exception_class, error.what());
  } catch (const std::exception& error) {
    env->ThrowNew(io_exception_class, error.what());
  }
  return NULL;
}

JNIEXPORT void JNICALL
Java_com_intel_oap_vectorized_ArrowColumnarToRowJniWrapper_nativeClose(
    JNIEnv* env, jobject, jlong instance_id) {
  columnar_to_row_converter_holder_.Erase(instance_id);
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
