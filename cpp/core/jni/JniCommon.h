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

#pragma once

#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/parallel.h>
#include <jni.h>

#include "compute/ProtobufUtils.h"
#include "memory/ArrowMemoryPool.h"
#include "utils/exception.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/qat_util.h"
#endif

#ifdef GLUTEN_ENABLE_IAA
#include "utils/qpl/qpl_codec.h"
#endif

static jint JNI_VERSION = JNI_VERSION_1_8;

static inline jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

static inline jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  return ret;
}

static inline jmethodID GetStaticMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig) {
  jmethodID ret = env->GetStaticMethodID(this_class, name, sig);
  return ret;
}

static inline std::shared_ptr<arrow::DataType> GetOffsetDataType(std::shared_ptr<arrow::DataType> parent_type) {
  switch (parent_type->id()) {
    case arrow::BinaryType::type_id:
      return std::make_shared<arrow::TypeTraits<arrow::BinaryType>::OffsetType>();
    case arrow::LargeBinaryType::type_id:
      return std::make_shared<arrow::TypeTraits<arrow::LargeBinaryType>::OffsetType>();
    case arrow::ListType::type_id:
      return std::make_shared<arrow::TypeTraits<arrow::ListType>::OffsetType>();
    case arrow::LargeListType::type_id:
      return std::make_shared<arrow::TypeTraits<arrow::LargeListType>::OffsetType>();
    default:
      return nullptr;
  }
}

template <typename T>
inline bool is_fixed_width_type(T _) {
  return std::is_base_of<arrow::FixedWidthType, T>::value;
}

static inline arrow::Status AppendNodes(
    std::shared_ptr<arrow::Array> column,
    std::vector<std::pair<int64_t, int64_t>>* nodes) {
  auto type = column->type();
  (*nodes).push_back(std::make_pair(column->length(), column->null_count()));
  switch (type->id()) {
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(column);
      RETURN_NOT_OK(AppendNodes(list_array->values(), nodes));
    } break;
    default: {
    } break;
  }
  return arrow::Status::OK();
}

static inline arrow::Status AppendBuffers(
    std::shared_ptr<arrow::Array> column,
    std::vector<std::shared_ptr<arrow::Buffer>>* buffers) {
  auto type = column->type();
  switch (type->id()) {
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(column);
      (*buffers).push_back(list_array->null_bitmap());
      (*buffers).push_back(list_array->value_offsets());
      RETURN_NOT_OK(AppendBuffers(list_array->values(), buffers));
    } break;
    default: {
      for (auto& buffer : column->data()->buffers) {
        (*buffers).push_back(buffer);
      }
    } break;
  }
  return arrow::Status::OK();
}

static inline arrow::Status FIXOffsetBuffer(std::shared_ptr<arrow::Buffer>* in_buf, int fix_row) {
  if ((*in_buf) == nullptr || (*in_buf)->size() == 0)
    return arrow::Status::OK();
  if ((*in_buf)->size() * 8 <= fix_row) {
    ARROW_ASSIGN_OR_RAISE(auto valid_copy, arrow::AllocateBuffer((*in_buf)->size() + 1));
    std::memcpy(valid_copy->mutable_data(), (*in_buf)->data(), static_cast<size_t>((*in_buf)->size()));
    (*in_buf) = std::move(valid_copy);
  }
  arrow::bit_util::SetBitsTo(const_cast<uint8_t*>((*in_buf)->data()), fix_row, 1, true);
  return arrow::Status::OK();
}

static inline arrow::Status MakeArrayData(
    std::shared_ptr<arrow::DataType> type,
    int num_rows,
    std::vector<std::shared_ptr<arrow::Buffer>> in_bufs,
    int in_bufs_len,
    std::shared_ptr<arrow::ArrayData>* arr_data,
    int* buf_idx_ptr) {
  if (arrow::is_nested(type->id())) {
    // Maybe ListType, MapType, StructType or UnionType
    switch (type->id()) {
      case arrow::Type::LIST:
      case arrow::Type::LARGE_LIST: {
        auto offset_data_type = GetOffsetDataType(type);
        auto list_type = std::dynamic_pointer_cast<arrow::ListType>(type);
        auto child_type = list_type->value_type();
        std::shared_ptr<arrow::ArrayData> child_array_data, offset_array_data;
        // create offset array
        // Chendi: For some reason, for ListArray::FromArrays will remove last
        // row from offset array, refer to array_nested.cc CleanListOffsets
        // function
        RETURN_NOT_OK(FIXOffsetBuffer(&in_bufs[*buf_idx_ptr], num_rows));
        RETURN_NOT_OK(
            MakeArrayData(offset_data_type, num_rows + 1, in_bufs, in_bufs_len, &offset_array_data, buf_idx_ptr));
        auto offset_array = arrow::MakeArray(offset_array_data);
        // create child data array
        RETURN_NOT_OK(MakeArrayData(child_type, -1, in_bufs, in_bufs_len, &child_array_data, buf_idx_ptr));
        auto child_array = arrow::MakeArray(child_array_data);
        auto list_array = arrow::ListArray::FromArrays(*offset_array, *child_array).ValueOrDie();
        *arr_data = list_array->data();

      } break;
      default:
        return arrow::Status::NotImplemented("MakeArrayData for type ", type->ToString(), " is not supported yet.");
    }

  } else {
    int64_t null_count = arrow::kUnknownNullCount;
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
    if (*buf_idx_ptr >= in_bufs_len) {
      return arrow::Status::Invalid("insufficient number of in_buf_addrs");
    }
    if (in_bufs[*buf_idx_ptr]->size() == 0) {
      null_count = 0;
    }
    buffers.push_back(in_bufs[*buf_idx_ptr]);
    *buf_idx_ptr += 1;

    if (arrow::is_binary_like(type->id())) {
      if (*buf_idx_ptr >= in_bufs_len) {
        return arrow::Status::Invalid("insufficient number of in_buf_addrs");
      }

      buffers.push_back(in_bufs[*buf_idx_ptr]);
      auto offsets_size = in_bufs[*buf_idx_ptr]->size();
      *buf_idx_ptr += 1;
      if (num_rows == -1)
        num_rows = offsets_size / 4 - 1;
    }

    if (*buf_idx_ptr >= in_bufs_len) {
      return arrow::Status::Invalid("insufficient number of in_buf_addrs");
    }
    auto value_size = in_bufs[*buf_idx_ptr]->size();
    buffers.push_back(in_bufs[*buf_idx_ptr]);
    *buf_idx_ptr += 1;
    if (num_rows == -1) {
      num_rows = value_size * 8 / arrow::bit_width(type->id());
    }

    *arr_data = arrow::ArrayData::Make(type, num_rows, std::move(buffers), null_count);
  }
  return arrow::Status::OK();
}

static inline arrow::Status MakeRecordBatch(
    const std::shared_ptr<arrow::Schema>& schema,
    int num_rows,
    std::vector<std::shared_ptr<arrow::Buffer>> in_bufs,
    int in_bufs_len,
    std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
  auto num_fields = schema->num_fields();
  int buf_idx = 0;

  for (int i = 0; i < num_fields; i++) {
    auto field = schema->field(i);
    std::shared_ptr<arrow::ArrayData> array_data;
    RETURN_NOT_OK(MakeArrayData(field->type(), num_rows, in_bufs, in_bufs_len, &array_data, &buf_idx));
    arrays.push_back(array_data);
  }

  *batch = arrow::RecordBatch::Make(schema, num_rows, arrays);
  return arrow::Status::OK();
}

static inline arrow::Status MakeRecordBatch(
    const std::shared_ptr<arrow::Schema>& schema,
    int num_rows,
    int64_t* in_buf_addrs,
    int64_t* in_buf_sizes,
    int in_bufs_len,
    std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < in_bufs_len; i++) {
    if (in_buf_addrs[i] != 0) {
      auto data = std::shared_ptr<arrow::Buffer>(
          new arrow::Buffer(reinterpret_cast<uint8_t*>(in_buf_addrs[i]), in_buf_sizes[i]));
      buffers.push_back(data);
    } else {
      buffers.push_back(std::make_shared<arrow::Buffer>(nullptr, 0));
    }
  }
  return MakeRecordBatch(schema, num_rows, buffers, in_bufs_len, batch);
}

static inline std::string JStringToCString(JNIEnv* env, jstring string) {
  int32_t jlen, clen;
  clen = env->GetStringUTFLength(string);
  jlen = env->GetStringLength(string);
  char buffer[clen];
  env->GetStringUTFRegion(string, 0, jlen, buffer);
  return std::string(buffer, clen);
}

static inline jbyteArray ToSchemaByteArray(JNIEnv* env, std::shared_ptr<arrow::Schema> schema) {
  arrow::Status status;
  // std::shared_ptr<arrow::Buffer> buffer;
  arrow::Result<std::shared_ptr<arrow::Buffer>> maybe_buffer;
  maybe_buffer = arrow::ipc::SerializeSchema(*schema.get(), gluten::GetDefaultWrappedArrowMemoryPool().get());
  if (!status.ok()) {
    std::string error_message = "Unable to convert schema to byte array, err is " + status.message();
    throw gluten::GlutenException(error_message);
  }
  auto buffer = *std::move(maybe_buffer);
  jbyteArray out = env->NewByteArray(buffer->size());
  auto src = reinterpret_cast<const jbyte*>(buffer->data());
  env->SetByteArrayRegion(out, 0, buffer->size(), src);
  return out;
}

static inline arrow::Result<arrow::Compression::type> GetCompressionType(JNIEnv* env, jstring codec_jstr) {
  auto codec_u = env->GetStringUTFChars(codec_jstr, JNI_FALSE);

  std::string codec_l;
  std::transform(codec_u, codec_u + std::strlen(codec_u), std::back_inserter(codec_l), ::tolower);
#ifdef GLUTEN_ENABLE_QAT
  // TODO: Support more codec.
  static const std::string qat_codec_prefix = "gluten_qat_";
  if (codec_l.rfind(qat_codec_prefix, 0) == 0) {
    auto codec = codec_l.substr(qat_codec_prefix.size());
    if (gluten::qat::SupportsCodec(codec)) {
      gluten::qat::EnsureQatCodecRegistered(codec);
      codec_l = "custom";
    } else {
      std::string error_message = "Unrecognized compression codec: " + codec_l;
      env->ReleaseStringUTFChars(codec_jstr, codec_u);
      throw gluten::GlutenException(error_message);
    }
  }
#endif

#ifdef GLUTEN_ENABLE_IAA
  static const std::string qpl_codec_prefix = "gluten_iaa_";
  if (codec_l.rfind(qpl_codec_prefix, 0) == 0) {
    auto codec = codec_l.substr(qpl_codec_prefix.size());
    if (gluten::qpl::SupportsCodec(codec)) {
      gluten::qpl::EnsureQplCodecRegistered(codec);
      codec_l = "custom";
    } else {
      std::string error_message = "Unrecognized compression codec: " + codec_l;
      env->ReleaseStringUTFChars(codec_jstr, codec_u);
      throw gluten::GlutenException(error_message);
    }
  }
#endif

  ARROW_ASSIGN_OR_RAISE(auto compression_type, arrow::util::Codec::GetCompressionType(codec_l));

  if (compression_type == arrow::Compression::LZ4) {
    compression_type = arrow::Compression::LZ4_FRAME;
  }
  env->ReleaseStringUTFChars(codec_jstr, codec_u);
  return compression_type;
}

static inline arrow::Status DecompressBuffer(
    const arrow::Buffer& buffer,
    arrow::util::Codec* codec,
    std::shared_ptr<arrow::Buffer>* out,
    arrow::MemoryPool* pool) {
  const uint8_t* data = buffer.data();
  int64_t compressed_size = buffer.size() - sizeof(int64_t);
  int64_t uncompressed_size = arrow::bit_util::FromLittleEndian(arrow::util::SafeLoadAs<int64_t>(data));
  ARROW_ASSIGN_OR_RAISE(auto uncompressed, AllocateBuffer(uncompressed_size, pool));

  int64_t actual_decompressed;
  ARROW_ASSIGN_OR_RAISE(
      actual_decompressed,
      codec->Decompress(compressed_size, data + sizeof(int64_t), uncompressed_size, uncompressed->mutable_data()));
  if (actual_decompressed != uncompressed_size) {
    return arrow::Status::Invalid(
        "Failed to fully decompress buffer, expected ",
        uncompressed_size,
        " bytes but decompressed ",
        actual_decompressed);
  }
  *out = std::move(uncompressed);
  return arrow::Status::OK();
}

static inline arrow::Status DecompressBuffers(
    arrow::Compression::type compression,
    const arrow::ipc::IpcReadOptions& options,
    const uint8_t* buf_mask,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    const std::vector<std::shared_ptr<arrow::Field>>& schema_fields) {
  std::unique_ptr<arrow::util::Codec> codec;
  ARROW_ASSIGN_OR_RAISE(codec, arrow::util::Codec::Create(compression));

  auto DecompressOne = [&buffers, &buf_mask, &codec, &options](int i) {
    if (buffers[i] == nullptr || buffers[i]->size() == 0) {
      return arrow::Status::OK();
    }
    // if the buffer has been rebuilt to uncompressed on java side, return
    if (arrow::bit_util::GetBit(buf_mask, i)) {
      ARROW_ASSIGN_OR_RAISE(auto valid_copy, buffers[i]->CopySlice(0, buffers[i]->size()));
      buffers[i] = valid_copy;
      return arrow::Status::OK();
    }

    if (buffers[i]->size() < 8) {
      return arrow::Status::Invalid(
          "Likely corrupted message, compressed buffers "
          "are larger than 8 bytes by construction");
    }
    RETURN_NOT_OK(DecompressBuffer(*buffers[i], codec.get(), &buffers[i], options.memory_pool));
    return arrow::Status::OK();
  };

  return ::arrow::internal::OptionalParallelFor(options.use_threads, static_cast<int>(buffers.size()), DecompressOne);
}

static inline void AttachCurrentThreadAsDaemonOrThrow(JavaVM* vm, JNIEnv** out) {
  int getEnvStat = vm->GetEnv(reinterpret_cast<void**>(out), JNI_VERSION);
  if (getEnvStat == JNI_EDETACHED) {
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "JNIEnv was not attached to current thread." << std::endl;
#endif
    // Reattach current thread to JVM
    getEnvStat = vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(out), NULL);
    if (getEnvStat != JNI_OK) {
      throw gluten::GlutenException("Failed to reattach current thread to JVM.");
    }
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "Succeeded attaching current thread." << std::endl;
#endif
    return;
  }
  if (getEnvStat != JNI_OK) {
    throw gluten::GlutenException("Failed to attach current thread to JVM.");
  }
}

static inline void CheckException(JNIEnv* env) {
  if (env->ExceptionCheck()) {
    jthrowable t = env->ExceptionOccurred();
    env->ExceptionClear();
    jclass describer_class = env->FindClass("io/glutenproject/exception/JniExceptionDescriber");
    jmethodID describe_method =
        env->GetStaticMethodID(describer_class, "describe", "(Ljava/lang/Throwable;)Ljava/lang/String;");
    std::string description =
        JStringToCString(env, (jstring)env->CallStaticObjectMethod(describer_class, describe_method, t));
    throw gluten::GlutenException("Error during calling Java code from native code: " + description);
  }
}

class SparkAllocationListener final : public gluten::AllocationListener {
 public:
  SparkAllocationListener(
      JavaVM* vm,
      jobject java_listener,
      jmethodID java_reserve_method,
      jmethodID java_unreserve_method,
      int64_t block_size)
      : vm_(vm),
        java_reserve_method_(java_reserve_method),
        java_unreserve_method_(java_unreserve_method),
        block_size_(block_size) {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }
    java_listener_ = env->NewGlobalRef(java_listener);
  }

  ~SparkAllocationListener() override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      std::cerr << "SparkAllocationListener#~SparkAllocationListener(): "
                << "JNIEnv was not attached to current thread" << std::endl;
      return;
    }
    env->DeleteGlobalRef(java_listener_);
  }

  void AllocationChanged(int64_t size) override {
    UpdateReservation(size);
  };

 private:
  int64_t Reserve(int64_t diff) {
    std::lock_guard<std::mutex> lock(mutex_);
    bytes_reserved_ += diff;
    int64_t new_block_count;
    if (bytes_reserved_ == 0) {
      new_block_count = 0;
    } else {
      // ceil to get the required block number
      new_block_count = (bytes_reserved_ - 1) / block_size_ + 1;
    }
    int64_t bytes_granted = (new_block_count - blocks_reserved_) * block_size_;
    blocks_reserved_ = new_block_count;
    if (bytes_reserved_ > max_bytes_reserved_) {
      max_bytes_reserved_ = bytes_reserved_;
    }
    return bytes_granted;
  }

  void UpdateReservation(int64_t diff) {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }
    int64_t granted = Reserve(diff);
    if (granted == 0) {
      return;
    }
    if (granted < 0) {
      env->CallObjectMethod(java_listener_, java_unreserve_method_, -granted);
      CheckException(env);
      return;
    }
    env->CallObjectMethod(java_listener_, java_reserve_method_, granted);
    CheckException(env);
  }

  JavaVM* vm_;
  jobject java_listener_;
  jmethodID java_reserve_method_;
  jmethodID java_unreserve_method_;
  int64_t block_size_;
  int64_t blocks_reserved_ = 0L;
  int64_t bytes_reserved_ = 0L;
  int64_t max_bytes_reserved_ = 0L;
  std::mutex mutex_;
};

class CelebornClient {
 public:
  CelebornClient(JavaVM* vm, jobject java_celeborn_shuffle_writer, jmethodID java_celeborn_push_partition_data_method)
      : vm_(vm), java_celeborn_push_partition_data_(java_celeborn_push_partition_data_method) {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }

    java_celeborn_shuffle_writer_ = env->NewGlobalRef(java_celeborn_shuffle_writer);
  }

  ~CelebornClient() {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      std::cerr << "CelebornClient#~CelebornClient(): "
                << "JNIEnv was not attached to current thread" << std::endl;
      return;
    }
    env->DeleteGlobalRef(java_celeborn_shuffle_writer_);
  }

  void PushPartitonData(int32_t partition_id, char* bytes, int64_t size) {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }
    jbyteArray array = env->NewByteArray(size);
    env->SetByteArrayRegion(array, 0, size, reinterpret_cast<jbyte*>(bytes));
    env->CallIntMethod(java_celeborn_shuffle_writer_, java_celeborn_push_partition_data_, partition_id, array);
    CheckException(env);
  }

  JavaVM* vm_;
  jobject java_celeborn_shuffle_writer_;
  jmethodID java_celeborn_push_partition_data_;
};