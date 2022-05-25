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
#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/util/compression.h>
#include <arrow/util/iterator.h>
#include <jni.h>
#include <jni/dataset/jni_util.h>
#include <malloc.h>

#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "compute/protobuf_utils.h"
#include "jni/concurrent_map.h"
#include "jni/exec_backend.h"
#include "jni/jni_common.h"
#include "jni/jni_errors.h"
#include "operators/c2r/columnar_to_row_base.h"
#include "operators/shuffle/splitter.h"
#include "utils/exception.h"
#include "utils/result_iterator.h"

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

static jclass native_columnar_to_row_info_class;
static jmethodID native_columnar_to_row_info_constructor;

using arrow::jni::ConcurrentMap;

static jint JNI_VERSION = JNI_VERSION_1_8;

static arrow::jni::ConcurrentMap<
    std::shared_ptr<gluten::columnartorow::ColumnarToRowConverterBase>>
    columnar_to_row_converter_holder_;

using gluten::RecordBatchResultIterator;
static arrow::jni::ConcurrentMap<std::shared_ptr<RecordBatchResultIterator>>
    batch_iterator_holder_;

using gluten::shuffle::SplitOptions;
using gluten::shuffle::Splitter;
static arrow::jni::ConcurrentMap<std::shared_ptr<Splitter>> shuffle_splitter_holder_;
static arrow::jni::ConcurrentMap<std::shared_ptr<arrow::Schema>>
    decompression_schema_holder_;

std::shared_ptr<RecordBatchResultIterator> GetBatchIterator(JNIEnv* env, jlong id) {
  auto handler = batch_iterator_holder_.Lookup(id);
  if (!handler) {
    std::string error_message = "invalid handler id " + std::to_string(id);
    throw gluten::GlutenException(error_message);
  }
  return handler;
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
    int getEnvStat = vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
    if (getEnvStat == JNI_EDETACHED) {
      if (vm_->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL) != 0) {
        std::cout << "Failed to deconstruct due to thread not being attached."
                  << std::endl;
        return;
      }
#ifdef DEBUG
      std::cout << "DELETING GLOBAL ITERATOR REF "
                << reinterpret_cast<long>(java_serialized_record_batch_iterator_) << "..."
                << std::endl;
#endif
      env->DeleteGlobalRef(java_serialized_record_batch_iterator_);
      vm_->DetachCurrentThread();
    } else if (getEnvStat != JNI_OK) {
      std::cout << "Failed to deconstruct due to thread not being attached." << std::endl;
    }
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    JNIEnv* env;
    int getEnvStat = vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
    if (getEnvStat == JNI_EDETACHED) {
#ifdef DEBUG
      std::cout << "JNIEnv was not attached to current thread." << std::endl;
#endif
      if (vm_->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&env), NULL) != 0) {
        return arrow::Status::Invalid("Failed to attach thread.");
      }
#ifdef DEBUG
      std::cout << "Succeeded attaching current thread." << std::endl;
#endif
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
    ArrowSchema c_schema{};
    ArrowArray c_array{};
    env->CallObjectMethod(
        java_serialized_record_batch_iterator_, serialized_record_batch_iterator_next,
        reinterpret_cast<jlong>(&c_schema), reinterpret_cast<jlong>(&c_array));
    RETURN_NOT_OK(arrow::dataset::jni::CheckException(env));
    ARROW_ASSIGN_OR_RAISE(auto batch, arrow::ImportRecordBatch(&c_array, &c_schema))
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
std::shared_ptr<JavaRecordBatchIterator> MakeJavaRecordBatchIterator(
    JavaVM* vm, jobject java_serialized_record_batch_iterator,
    std::shared_ptr<arrow::Schema> schema) {
  std::shared_ptr<arrow::Schema> schema_moved = std::move(schema);
  return std::make_shared<JavaRecordBatchIterator>(
      vm, java_serialized_record_batch_iterator, schema_moved);
}

jmethodID GetMethodIDOrError(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = GetMethodID(env, this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
        " within signature" + std::string(sig);
    throw gluten::GlutenException(error_message);
  }
  return ret;
}

jclass CreateGlobalClassReferenceOrError(JNIEnv* env, const char* class_name) {
  jclass global_class = CreateGlobalClassReference(env, class_name);
  if (global_class == nullptr) {
    std::string error_message =
        "Unable to CreateGlobalClassReferenceOrError for" + std::string(class_name);
    throw gluten::GlutenException(error_message);
  }
  return global_class;
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
  gluten::GetJniErrorsState()->Initialize(env);

  serializable_obj_builder_class = CreateGlobalClassReferenceOrError(
      env, "Lio/glutenproject/vectorized/NativeSerializableObject;");
  serializable_obj_builder_constructor =
      GetMethodIDOrError(env, serializable_obj_builder_class, "<init>", "([J[I)V");

  byte_array_class = CreateGlobalClassReferenceOrError(env, "[B");
  split_result_class =
      CreateGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/SplitResult;");
  split_result_constructor =
      GetMethodIDOrError(env, split_result_class, "<init>", "(JJJJJJ[J[J)V");

  metrics_builder_class =
      CreateGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/MetricsObject;");
  metrics_builder_constructor =
      GetMethodIDOrError(env, metrics_builder_class, "<init>", "([J[J)V");

  serialized_record_batch_iterator_class =
      CreateGlobalClassReferenceOrError(env, "Lio/glutenproject/vectorized/VeloxInIterator;");
  serialized_record_batch_iterator_hasNext =
      GetMethodIDOrError(env, serialized_record_batch_iterator_class, "hasNext", "()Z");
  serialized_record_batch_iterator_next =
      GetMethodIDOrError(env, serialized_record_batch_iterator_class, "next", "(JJ)V");

  native_columnar_to_row_info_class = CreateGlobalClassReferenceOrError(
      env, "Lio/glutenproject/vectorized/NativeColumnarToRowInfo;");
  native_columnar_to_row_info_constructor =
      GetMethodIDOrError(env, native_columnar_to_row_info_class, "<init>", "(J[J[JJ)V");

  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  std::cerr << "JNI_OnUnload" << std::endl;
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

  env->DeleteGlobalRef(serializable_obj_builder_class);
  env->DeleteGlobalRef(split_result_class);
  env->DeleteGlobalRef(serialized_record_batch_iterator_class);
  env->DeleteGlobalRef(native_columnar_to_row_info_class);

  env->DeleteGlobalRef(byte_array_class);

  batch_iterator_holder_.Clear();
  columnar_to_row_converter_holder_.Clear();
  shuffle_splitter_holder_.Clear();
  decompression_schema_holder_.Clear();
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetJavaTmpDir(
    JNIEnv* env, jobject obj, jstring pathObj) {
  JNI_METHOD_START
  jboolean ifCopy;
  auto path = env->GetStringUTFChars(pathObj, &ifCopy);
  setenv("NATIVESQL_TMP_DIR", path, 1);
  env->ReleaseStringUTFChars(pathObj, path);
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetBatchSize(
    JNIEnv* env, jobject obj, jint batch_size) {
  setenv("NATIVESQL_BATCH_SIZE", std::to_string(batch_size).c_str(), 1);
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeSetMetricsTime(
    JNIEnv* env, jobject obj, jboolean is_enable) {
  setenv("NATIVESQL_METRICS_TIME", (is_enable ? "true" : "false"), 1);
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeCreateKernelWithIterator(
    JNIEnv* env, jobject obj, jlong memory_pool_id, jbyteArray plan_arr,
    jobjectArray iter_arr) {
  JNI_METHOD_START
  arrow::Status msg;
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    std::string error_message = "Unable to get JavaVM instance";
    gluten::JniThrow(error_message);
  }

  auto plan_data =
      reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(plan_arr, 0));
  auto plan_size = env->GetArrayLength(plan_arr);

  auto backend = gluten::CreateBackend();
  if (!backend->ParsePlan(plan_data, plan_size)) {
    gluten::JniThrow("Failed to parse plan.");
  }

  // Handle the Java iters
  jsize iters_len = env->GetArrayLength(iter_arr);
  std::vector<std::shared_ptr<RecordBatchResultIterator>> input_iters;
  if (iters_len > 0) {
    // Get input schema from Substrait plan.
    const auto& schema_map = backend->GetInputSchemaMap();
    for (int idx = 0; idx < iters_len; idx++) {
      jobject iter = env->GetObjectArrayElement(iter_arr, idx);
      // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
      // TODO Release this in JNI Unload or dependent object's destructor
      jobject ref_iter = env->NewGlobalRef(iter);
      auto it = schema_map.find(idx);
      if (it == schema_map.end()) {
        gluten::JniThrow("Schema not found for input batch iterator " +
                              std::to_string(idx));
      }

      auto rb_iter = std::make_shared<JavaRecordBatchIterator>(vm, ref_iter, it->second);
      input_iters.push_back(
          std::make_shared<RecordBatchResultIterator>(std::move(rb_iter)));
    }
  }

  std::shared_ptr<RecordBatchResultIterator> res_iter;
  if (input_iters.empty()) {
    res_iter = backend->GetResultIterator();
  } else {
    res_iter = backend->GetResultIterator(input_iters);
  }
  return batch_iterator_holder_.Insert(std::move(res_iter));
  JNI_METHOD_END(-1)
}

JNIEXPORT jboolean JNICALL
Java_io_glutenproject_vectorized_VeloxOutIterator_nativeHasNext(JNIEnv* env, jobject obj,
                                                                jlong id) {
  JNI_METHOD_START
  auto iter = GetBatchIterator(env, id);
  if (iter == nullptr) {
    std::string error_message = "faked to get batch iterator";
    gluten::JniThrow(error_message);
  }
  return iter->HasNext();
  JNI_METHOD_END(false)
}

JNIEXPORT jboolean JNICALL Java_io_glutenproject_vectorized_VeloxOutIterator_nativeNext(
    JNIEnv* env, jobject obj, jlong id, jlong c_schema, jlong c_array) {
  JNI_METHOD_START
  auto iter = GetBatchIterator(env, id);
  if (!iter->HasNext()) {
    return false;
  }
  auto batch = std::move(iter->Next());
  gluten::JniAssertOkOrThrow(
      arrow::ExportRecordBatch(*batch, reinterpret_cast<struct ArrowArray*>(c_array),
                               reinterpret_cast<struct ArrowSchema*>(c_schema)));
  return true;
  JNI_METHOD_END(false)
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_VeloxOutIterator_nativeClose(
    JNIEnv* env, jobject this_obj, jlong id) {
  JNI_METHOD_START
#ifdef DEBUG
  auto it = batch_iterator_holder_.Lookup(id);
  if (it.use_count() > 2) {
    std::cout << "RecordBatchResultIterator Id " << id << " use count is "
              << it.use_count() << std::endl;
  }
  std::cout << "BatchIterator nativeClose." << std::endl;
#endif
  batch_iterator_holder_.Erase(id);
  JNI_METHOD_END()
}

JNIEXPORT jobject JNICALL
Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeConvertColumnarToRow(
    JNIEnv* env, jobject, jbyteArray schema_arr, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jlong memory_pool_id, jboolean wsChild) {
  JNI_METHOD_START
  if (schema_arr == NULL) {
    gluten::JniThrow("Native convert columnar to row schema can't be null");
  }
  if (buf_addrs == NULL) {
    gluten::JniThrow("Native convert columnar to row: buf_addrs can't be null");
  }
  if (buf_sizes == NULL) {
    gluten::JniThrow("Native convert columnar to row: buf_sizes can't be null");
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    gluten::JniThrow(
        "Native convert columnar to row: length of buf_addrs and buf_sizes mismatch");
  }

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> rb;
  gluten::JniAssertOkOrThrow(MakeRecordBatch(schema, num_rows, (int64_t*)in_buf_addrs,
                                     (int64_t*)in_buf_sizes, in_bufs_len, &rb),
                     "Native convert columnar to row: make record batch failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  // convert the record batch to spark unsafe row.
  auto* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    gluten::JniThrow("Memory pool does not exist or has been closed");
  }
  auto backend = gluten::CreateBackend();
  std::shared_ptr<gluten::columnartorow::ColumnarToRowConverterBase>
      columnar_to_row_converter = backend->getColumnarConverter(rb, pool, wsChild);
  gluten::JniAssertOkOrThrow(columnar_to_row_converter->Init(),
                     "Native convert columnar to row: Init "
                     "ColumnarToRowConverter failed");
  gluten::JniAssertOkOrThrow(
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

  jobject native_columnar_to_row_info = env->NewObject(
      native_columnar_to_row_info_class, native_columnar_to_row_info_constructor,
      instanceID, offsets_arr, lengths_arr, address);
  return native_columnar_to_row_info;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_NativeColumnarToRowJniWrapper_nativeClose(
    JNIEnv* env, jobject, jlong instance_id) {
  JNI_METHOD_START
  columnar_to_row_converter_holder_.Erase(instance_id);
  JNI_METHOD_END()
}

// Shuffle
JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ShuffleSplitterJniWrapper_nativeMake(
    JNIEnv* env, jobject, jstring partitioning_name_jstr, jint num_partitions,
    jbyteArray schema_arr, jbyteArray expr_arr, jlong offheap_per_task, jint buffer_size,
    jstring compression_type_jstr, jint batch_compress_threshold, jstring data_file_jstr,
    jint num_sub_dirs, jstring local_dirs_jstr, jboolean prefer_spill,
    jlong memory_pool_id, jboolean write_schema) {
  JNI_METHOD_START
  if (partitioning_name_jstr == NULL) {
    gluten::JniThrow(std::string("Short partitioning name can't be null"));
    return 0;
  }
  if (schema_arr == NULL) {
    gluten::JniThrow(std::string("Make splitter schema can't be null"));
  }
  if (data_file_jstr == NULL) {
    gluten::JniThrow(std::string("Shuffle DataFile can't be null"));
  }
  if (local_dirs_jstr == NULL) {
    gluten::JniThrow(std::string("Shuffle DataFile can't be null"));
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
    gluten::JniThrow("Memory pool does not exist or has been closed");
  }
  splitOptions.memory_pool = pool;

  auto local_dirs = env->GetStringUTFChars(local_dirs_jstr, JNI_FALSE);
  setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs, 1);
  env->ReleaseStringUTFChars(local_dirs_jstr, local_dirs);

  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  MakeSchema(env, schema_arr, &schema);

  jclass cls = env->FindClass("java/lang/Thread");
  jmethodID mid = env->GetStaticMethodID(cls, "currentThread", "()Ljava/lang/Thread;");
  jobject thread = env->CallStaticObjectMethod(cls, mid);
  if (thread == NULL) {
    std::cout << "Thread.currentThread() return NULL" << std::endl;
  } else {
    jmethodID mid_getid = GetMethodIDOrError(env, cls, "getId", "()J");
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
    jmethodID get_tsk_attmpt_mid = GetMethodIDOrError(env, tc_cls, "taskAttemptId", "()J");
    jlong attmpt_id = env->CallLongMethod(tc_obj, get_tsk_attmpt_mid);
    splitOptions.task_attempt_id = (int64_t)attmpt_id;
  }
  splitOptions.batch_compress_threshold = batch_compress_threshold;

  // Get the hash expressions.
  const uint8_t* expr_data = nullptr;
  int expr_size = 0;
  if (expr_arr != NULL) {
    expr_data = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(expr_arr, 0));
    expr_size = env->GetArrayLength(expr_arr);
  }

  auto splitter =
      gluten::JniGetOrThrow(Splitter::Make(partitioning_name, std::move(schema), num_partitions,
                                   expr_data, expr_size, std::move(splitOptions)),
                    "Failed create native shuffle splitter");

  return shuffle_splitter_holder_.Insert(std::shared_ptr<Splitter>(splitter));

  JNI_METHOD_END(-1L)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ShuffleSplitterJniWrapper_setCompressType(
    JNIEnv* env, jobject, jlong splitter_id, jstring compression_type_jstr) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    gluten::JniThrow(error_message);
  }

  if (compression_type_jstr != NULL) {
    auto compression_type_result = GetCompressionType(env, compression_type_jstr);
    if (compression_type_result.status().ok()) {
      gluten::JniAssertOkOrThrow(
          splitter->SetCompressType(compression_type_result.MoveValueUnsafe()));
    }
  }
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_vectorized_ShuffleSplitterJniWrapper_split(
    JNIEnv* env, jobject, jlong splitter_id, jint num_rows, jlongArray buf_addrs,
    jlongArray buf_sizes, jboolean first_record_batch) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    gluten::JniThrow(error_message);
  }
  if (buf_addrs == NULL) {
    gluten::JniThrow("Native split: buf_addrs can't be null");
  }
  if (buf_sizes == NULL) {
    gluten::JniThrow("Native split: buf_sizes can't be null");
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    gluten::JniThrow("Native split: length of buf_addrs and buf_sizes mismatch");
  }

  jlong* in_buf_addrs = env->GetLongArrayElements(buf_addrs, JNI_FALSE);
  jlong* in_buf_sizes = env->GetLongArrayElements(buf_sizes, JNI_FALSE);

  std::shared_ptr<arrow::RecordBatch> in;
  gluten::JniAssertOkOrThrow(
      MakeRecordBatch(splitter->input_schema(), num_rows, (int64_t*)in_buf_addrs,
                      (int64_t*)in_buf_sizes, in_bufs_len, &in),
      "Native split: make record batch failed");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);

  if (first_record_batch) {
    return splitter->CompressedSize(*in);
  }
  gluten::JniAssertOkOrThrow(splitter->Split(*in), "Native split: splitter split failed");
  return -1L;
  JNI_METHOD_END(-1L)
}

JNIEXPORT jobject JNICALL Java_io_glutenproject_vectorized_ShuffleSplitterJniWrapper_stop(
    JNIEnv* env, jobject, jlong splitter_id) {
  JNI_METHOD_START
  auto splitter = shuffle_splitter_holder_.Lookup(splitter_id);
  if (!splitter) {
    std::string error_message = "Invalid splitter id " + std::to_string(splitter_id);
    gluten::JniThrow(error_message);
  }

  gluten::JniAssertOkOrThrow(splitter->Stop(), "Native split: splitter stop failed");

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

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ShuffleSplitterJniWrapper_close(
    JNIEnv* env, jobject, jlong splitter_id) {
  JNI_METHOD_START
  shuffle_splitter_holder_.Erase(splitter_id);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_vectorized_ShuffleDecompressionJniWrapper_make(
    JNIEnv* env, jobject, jbyteArray schema_arr) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema;
  // ValueOrDie in MakeSchema
  gluten::JniAssertOkOrThrow(MakeSchema(env, schema_arr, &schema));

  return decompression_schema_holder_.Insert(schema);
  JNI_METHOD_END(-1L)
}

JNIEXPORT jboolean JNICALL
Java_io_glutenproject_vectorized_ShuffleDecompressionJniWrapper_decompress(
    JNIEnv* env, jobject obj, jlong schema_holder_id, jstring compression_type_jstr,
    jint num_rows, jlongArray buf_addrs, jlongArray buf_sizes, jlongArray buf_mask,
    jlong c_schema, jlong c_array) {
  JNI_METHOD_START
  auto schema = decompression_schema_holder_.Lookup(schema_holder_id);
  if (!schema) {
    std::string error_message =
        "Invalid schema holder id " + std::to_string(schema_holder_id);
    gluten::JniThrow(error_message);
  }
  if (buf_addrs == NULL) {
    gluten::JniThrow("Native decompress: buf_addrs can't be null");
  }
  if (buf_sizes == NULL) {
    gluten::JniThrow("Native decompress: buf_sizes can't be null");
  }
  if (buf_mask == NULL) {
    gluten::JniThrow("Native decompress: buf_mask can't be null");
  }

  int in_bufs_len = env->GetArrayLength(buf_addrs);
  if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
    gluten::JniThrow("Native decompress: length of buf_addrs and buf_sizes mismatch");
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
  gluten::JniAssertOkOrThrow(
      DecompressBuffers(compression_type, options, (uint8_t*)in_buf_mask, input_buffers,
                        schema->fields()),
      "ShuffleDecompressionJniWrapper_decompress, failed to decompress buffers");

  env->ReleaseLongArrayElements(buf_addrs, in_buf_addrs, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_sizes, in_buf_sizes, JNI_ABORT);
  env->ReleaseLongArrayElements(buf_mask, in_buf_mask, JNI_ABORT);

  // make arrays from buffers
  std::shared_ptr<arrow::RecordBatch> rb;
  gluten::JniAssertOkOrThrow(
      MakeRecordBatch(schema, num_rows, input_buffers, input_buffers.size(), &rb),
      "ShuffleDecompressionJniWrapper_decompress, failed to MakeRecordBatch upon "
      "buffers");

  gluten::JniAssertOkOrThrow(
      arrow::ExportRecordBatch(*rb, reinterpret_cast<struct ArrowArray*>(c_array),
                               reinterpret_cast<struct ArrowSchema*>(c_schema)));
  return true;
  JNI_METHOD_END(false)
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ShuffleDecompressionJniWrapper_close(
    JNIEnv* env, jobject, jlong schema_holder_id) {
  decompression_schema_holder_.Erase(schema_holder_id);
}

JNIEXPORT void JNICALL Java_io_glutenproject_tpc_MallocUtils_mallocTrim(JNIEnv* env,
                                                                        jobject obj) {
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_trim... " << std::endl;
  malloc_trim(0);
}

JNIEXPORT void JNICALL Java_io_glutenproject_tpc_MallocUtils_mallocStats(JNIEnv* env,
                                                                         jobject obj) {
  //  malloc_stats_print(statsPrint, nullptr, nullptr);
  std::cout << "Calling malloc_stats... " << std::endl;
  malloc_stats();
}

#ifdef __cplusplus
}
#endif
