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

static jint jniVersion = JNI_VERSION_1_8;

static inline jclass createGlobalClassReference(JNIEnv* env, const char* className) {
  jclass localClass = env->FindClass(className);
  jclass globalClass = (jclass)env->NewGlobalRef(localClass);
  env->DeleteLocalRef(localClass);
  return globalClass;
}

static inline jmethodID getMethodId(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(thisClass, name, sig);
  return ret;
}

static inline jmethodID getStaticMethodId(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  jmethodID ret = env->GetStaticMethodID(thisClass, name, sig);
  return ret;
}

static inline std::shared_ptr<arrow::DataType> getOffsetDataType(std::shared_ptr<arrow::DataType> parentType) {
  switch (parentType->id()) {
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
inline bool isFixedWidthType(T _) {
  return std::is_base_of<arrow::FixedWidthType, T>::value;
}

static inline arrow::Status appendNodes(
    std::shared_ptr<arrow::Array> column,
    std::vector<std::pair<int64_t, int64_t>>* nodes) {
  auto type = column->type();
  (*nodes).push_back(std::make_pair(column->length(), column->null_count()));
  switch (type->id()) {
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      auto listArray = std::dynamic_pointer_cast<arrow::ListArray>(column);
      RETURN_NOT_OK(appendNodes(listArray->values(), nodes));
    } break;
    default: {
    } break;
  }
  return arrow::Status::OK();
}

static inline arrow::Status appendBuffers(
    std::shared_ptr<arrow::Array> column,
    std::vector<std::shared_ptr<arrow::Buffer>>* buffers) {
  auto type = column->type();
  switch (type->id()) {
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
      auto listArray = std::dynamic_pointer_cast<arrow::ListArray>(column);
      (*buffers).push_back(listArray->null_bitmap());
      (*buffers).push_back(listArray->value_offsets());
      RETURN_NOT_OK(appendBuffers(listArray->values(), buffers));
    } break;
    default: {
      for (auto& buffer : column->data()->buffers) {
        (*buffers).push_back(buffer);
      }
    } break;
  }
  return arrow::Status::OK();
}

static inline arrow::Status fixOffsetBuffer(std::shared_ptr<arrow::Buffer>* inBuf, int fixRow) {
  if ((*inBuf) == nullptr || (*inBuf)->size() == 0)
    return arrow::Status::OK();
  if ((*inBuf)->size() * 8 <= fixRow) {
    ARROW_ASSIGN_OR_RAISE(auto valid_copy, arrow::AllocateBuffer((*inBuf)->size() + 1));
    std::memcpy(valid_copy->mutable_data(), (*inBuf)->data(), static_cast<size_t>((*inBuf)->size()));
    (*inBuf) = std::move(valid_copy);
  }
  arrow::bit_util::SetBitsTo(const_cast<uint8_t*>((*inBuf)->data()), fixRow, 1, true);
  return arrow::Status::OK();
}

static inline arrow::Status makeArrayData(
    std::shared_ptr<arrow::DataType> type,
    int numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> inBufs,
    int inBufsLen,
    std::shared_ptr<arrow::ArrayData>* arrData,
    int* bufIdxPtr) {
  if (arrow::is_nested(type->id())) {
    // Maybe ListType, MapType, StructType or UnionType
    switch (type->id()) {
      case arrow::Type::LIST:
      case arrow::Type::LARGE_LIST: {
        auto offsetDataType = getOffsetDataType(type);
        auto listType = std::dynamic_pointer_cast<arrow::ListType>(type);
        auto childType = listType->value_type();
        std::shared_ptr<arrow::ArrayData> childArrayData, offsetArrayData;
        // create offset array
        // Chendi: For some reason, for ListArray::FromArrays will remove last
        // row from offset array, refer to array_nested.cc CleanListOffsets
        // function
        RETURN_NOT_OK(fixOffsetBuffer(&inBufs[*bufIdxPtr], numRows));
        RETURN_NOT_OK(makeArrayData(offsetDataType, numRows + 1, inBufs, inBufsLen, &offsetArrayData, bufIdxPtr));
        auto offsetArray = arrow::MakeArray(offsetArrayData);
        // create child data array
        RETURN_NOT_OK(makeArrayData(childType, -1, inBufs, inBufsLen, &childArrayData, bufIdxPtr));
        auto childArray = arrow::MakeArray(childArrayData);
        auto listArray = arrow::ListArray::FromArrays(*offsetArray, *childArray).ValueOrDie();
        *arrData = listArray->data();

      } break;
      default:
        return arrow::Status::NotImplemented("MakeArrayData for type ", type->ToString(), " is not supported yet.");
    }

  } else {
    int64_t nullCount = arrow::kUnknownNullCount;
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
    if (*bufIdxPtr >= inBufsLen) {
      return arrow::Status::Invalid("insufficient number of in_buf_addrs");
    }
    if (inBufs[*bufIdxPtr]->size() == 0) {
      nullCount = 0;
    }
    buffers.push_back(inBufs[*bufIdxPtr]);
    *bufIdxPtr += 1;

    if (arrow::is_binary_like(type->id())) {
      if (*bufIdxPtr >= inBufsLen) {
        return arrow::Status::Invalid("insufficient number of in_buf_addrs");
      }

      buffers.push_back(inBufs[*bufIdxPtr]);
      auto offsetsSize = inBufs[*bufIdxPtr]->size();
      *bufIdxPtr += 1;
      if (numRows == -1)
        numRows = offsetsSize / 4 - 1;
    }

    if (*bufIdxPtr >= inBufsLen) {
      return arrow::Status::Invalid("insufficient number of in_buf_addrs");
    }
    auto valueSize = inBufs[*bufIdxPtr]->size();
    buffers.push_back(inBufs[*bufIdxPtr]);
    *bufIdxPtr += 1;
    if (numRows == -1) {
      numRows = valueSize * 8 / arrow::bit_width(type->id());
    }

    *arrData = arrow::ArrayData::Make(type, numRows, std::move(buffers), nullCount);
  }
  return arrow::Status::OK();
}

static inline arrow::Status makeRecordBatch(
    const std::shared_ptr<arrow::Schema>& schema,
    int numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> inBufs,
    int inBufsLen,
    std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
  auto numFields = schema->num_fields();
  int bufIdx = 0;

  for (int i = 0; i < numFields; i++) {
    auto field = schema->field(i);
    std::shared_ptr<arrow::ArrayData> arrayData;
    RETURN_NOT_OK(makeArrayData(field->type(), numRows, inBufs, inBufsLen, &arrayData, &bufIdx));
    arrays.push_back(arrayData);
  }

  *batch = arrow::RecordBatch::Make(schema, numRows, arrays);
  return arrow::Status::OK();
}

static inline arrow::Status makeRecordBatch(
    const std::shared_ptr<arrow::Schema>& schema,
    int numRows,
    int64_t* inBufAddrs,
    int64_t* inBufSizes,
    int inBufsLen,
    std::shared_ptr<arrow::RecordBatch>* batch) {
  std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < inBufsLen; i++) {
    if (inBufAddrs[i] != 0) {
      auto data =
          std::shared_ptr<arrow::Buffer>(new arrow::Buffer(reinterpret_cast<uint8_t*>(inBufAddrs[i]), inBufSizes[i]));
      buffers.push_back(data);
    } else {
      buffers.push_back(std::make_shared<arrow::Buffer>(nullptr, 0));
    }
  }
  return makeRecordBatch(schema, numRows, buffers, inBufsLen, batch);
}

static inline std::string jStringToCString(JNIEnv* env, jstring string) {
  int32_t jlen, clen;
  clen = env->GetStringUTFLength(string);
  jlen = env->GetStringLength(string);
  char buffer[clen];
  env->GetStringUTFRegion(string, 0, jlen, buffer);
  return std::string(buffer, clen);
}

static inline arrow::Result<arrow::Compression::type> getCompressionType(JNIEnv* env, jstring codecJstr) {
  auto codecU = env->GetStringUTFChars(codecJstr, JNI_FALSE);

  std::string codecL;
  std::transform(codecU, codecU + std::strlen(codecU), std::back_inserter(codecL), ::tolower);
#ifdef GLUTEN_ENABLE_QAT
  // TODO: Support more codec.
  static const std::string qat_codec_prefix = "gluten_qat_";
  if (codecL.rfind(qat_codec_prefix, 0) == 0) {
    auto codec = codecL.substr(qat_codec_prefix.size());
    if (gluten::qat::SupportsCodec(codec)) {
      gluten::qat::EnsureQatCodecRegistered(codec);
      codecL = "custom";
    } else {
      std::string error_message = "Unrecognized compression codec: " + codecL;
      env->ReleaseStringUTFChars(codecJstr, codecU);
      throw gluten::GlutenException(error_message);
    }
  }
#endif

#ifdef GLUTEN_ENABLE_IAA
  static const std::string qpl_codec_prefix = "gluten_iaa_";
  if (codecL.rfind(qpl_codec_prefix, 0) == 0) {
    auto codec = codecL.substr(qpl_codec_prefix.size());
    if (gluten::qpl::SupportsCodec(codec)) {
      gluten::qpl::EnsureQplCodecRegistered(codec);
      codecL = "custom";
    } else {
      std::string error_message = "Unrecognized compression codec: " + codecL;
      env->ReleaseStringUTFChars(codecJstr, codecU);
      throw gluten::GlutenException(error_message);
    }
  }
#endif

  ARROW_ASSIGN_OR_RAISE(auto compression_type, arrow::util::Codec::GetCompressionType(codecL));

  if (compression_type == arrow::Compression::LZ4) {
    compression_type = arrow::Compression::LZ4_FRAME;
  }
  env->ReleaseStringUTFChars(codecJstr, codecU);
  return compression_type;
}

static inline arrow::Status decompressBuffer(
    const arrow::Buffer& buffer,
    arrow::util::Codec* codec,
    std::shared_ptr<arrow::Buffer>* out,
    arrow::MemoryPool* pool) {
  const uint8_t* data = buffer.data();
  int64_t compressedSize = buffer.size() - sizeof(int64_t);
  int64_t uncompressedSize = arrow::bit_util::FromLittleEndian(arrow::util::SafeLoadAs<int64_t>(data));
  ARROW_ASSIGN_OR_RAISE(auto uncompressed, AllocateBuffer(uncompressedSize, pool));

  int64_t actualDecompressed;
  ARROW_ASSIGN_OR_RAISE(
      actualDecompressed,
      codec->Decompress(compressedSize, data + sizeof(int64_t), uncompressedSize, uncompressed->mutable_data()));
  if (actualDecompressed != uncompressedSize) {
    return arrow::Status::Invalid(
        "Failed to fully decompress buffer, expected ",
        uncompressedSize,
        " bytes but decompressed ",
        actualDecompressed);
  }
  *out = std::move(uncompressed);
  return arrow::Status::OK();
}

static inline arrow::Status decompressBuffers(
    arrow::Compression::type compression,
    const arrow::ipc::IpcReadOptions& options,
    const uint8_t* bufMask,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    const std::vector<std::shared_ptr<arrow::Field>>& schemaFields) {
  std::unique_ptr<arrow::util::Codec> codec;
  ARROW_ASSIGN_OR_RAISE(codec, arrow::util::Codec::Create(compression));

  auto decompressOne = [&buffers, &bufMask, &codec, &options](int i) {
    if (buffers[i] == nullptr || buffers[i]->size() == 0) {
      return arrow::Status::OK();
    }
    // if the buffer has been rebuilt to uncompressed on java side, return
    if (arrow::bit_util::GetBit(bufMask, i)) {
      ARROW_ASSIGN_OR_RAISE(auto valid_copy, buffers[i]->CopySlice(0, buffers[i]->size()));
      buffers[i] = valid_copy;
      return arrow::Status::OK();
    }

    if (buffers[i]->size() < 8) {
      return arrow::Status::Invalid(
          "Likely corrupted message, compressed buffers "
          "are larger than 8 bytes by construction");
    }
    RETURN_NOT_OK(decompressBuffer(*buffers[i], codec.get(), &buffers[i], options.memory_pool));
    return arrow::Status::OK();
  };

  return ::arrow::internal::OptionalParallelFor(options.use_threads, static_cast<int>(buffers.size()), decompressOne);
}

static inline void attachCurrentThreadAsDaemonOrThrow(JavaVM* vm, JNIEnv** out) {
  int getEnvStat = vm->GetEnv(reinterpret_cast<void**>(out), jniVersion);
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

static inline void checkException(JNIEnv* env) {
  if (env->ExceptionCheck()) {
    jthrowable t = env->ExceptionOccurred();
    env->ExceptionClear();
    jclass describerClass = env->FindClass("io/glutenproject/exception/JniExceptionDescriber");
    jmethodID describeMethod =
        env->GetStaticMethodID(describerClass, "describe", "(Ljava/lang/Throwable;)Ljava/lang/String;");
    std::string description =
        jStringToCString(env, (jstring)env->CallStaticObjectMethod(describerClass, describeMethod, t));
    throw gluten::GlutenException("Error during calling Java code from native code: " + description);
  }
}

class SparkAllocationListener final : public gluten::AllocationListener {
 public:
  SparkAllocationListener(
      JavaVM* vm,
      jobject javaListener,
      jmethodID javaReserveMethod,
      jmethodID javaUnreserveMethod,
      int64_t blockSize)
      : vm_(vm),
        javaReserveMethod_(javaReserveMethod),
        javaUnreserveMethod_(javaUnreserveMethod),
        blockSize_(blockSize) {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }
    javaListener_ = env->NewGlobalRef(javaListener);
  }

  ~SparkAllocationListener() override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      std::cerr << "SparkAllocationListener#~SparkAllocationListener(): "
                << "JNIEnv was not attached to current thread" << std::endl;
      return;
    }
    env->DeleteGlobalRef(javaListener_);
  }

  void allocationChanged(int64_t size) override {
    updateReservation(size);
  };

 private:
  int64_t reserve(int64_t diff) {
    std::lock_guard<std::mutex> lock(mutex_);
    bytesReserved_ += diff;
    int64_t newBlockCount;
    if (bytesReserved_ == 0) {
      newBlockCount = 0;
    } else {
      // ceil to get the required block number
      newBlockCount = (bytesReserved_ - 1) / blockSize_ + 1;
    }
    int64_t bytesGranted = (newBlockCount - blocksReserved_) * blockSize_;
    blocksReserved_ = newBlockCount;
    if (bytesReserved_ > maxBytesReserved_) {
      maxBytesReserved_ = bytesReserved_;
    }
    return bytesGranted;
  }

  void updateReservation(int64_t diff) {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }
    int64_t granted = reserve(diff);
    if (granted == 0) {
      return;
    }
    if (granted < 0) {
      env->CallObjectMethod(javaListener_, javaUnreserveMethod_, -granted);
      checkException(env);
      return;
    }
    env->CallObjectMethod(javaListener_, javaReserveMethod_, granted);
    checkException(env);
  }

  JavaVM* vm_;
  jobject javaListener_;
  jmethodID javaReserveMethod_;
  jmethodID javaUnreserveMethod_;
  int64_t blockSize_;
  int64_t blocksReserved_ = 0L;
  int64_t bytesReserved_ = 0L;
  int64_t maxBytesReserved_ = 0L;
  std::mutex mutex_;
};

class RssClient {
 public:
  virtual ~RssClient() = default;
};

class CelebornClient : public RssClient {
 public:
  CelebornClient(JavaVM* vm, jobject javaCelebornShuffleWriter, jmethodID javaCelebornPushPartitionDataMethod)
      : vm_(vm), javaCelebornPushPartitionData_(javaCelebornPushPartitionDataMethod) {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }

    javaCelebornShuffleWriter_ = env->NewGlobalRef(javaCelebornShuffleWriter);
  }

  ~CelebornClient() {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      std::cerr << "CelebornClient#~CelebornClient(): "
                << "JNIEnv was not attached to current thread" << std::endl;
      return;
    }
    env->DeleteGlobalRef(javaCelebornShuffleWriter_);
  }

  void pushPartitonData(int32_t partitionId, char* bytes, int64_t size) {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }
    jbyteArray array = env->NewByteArray(size);
    env->SetByteArrayRegion(array, 0, size, reinterpret_cast<jbyte*>(bytes));
    env->CallIntMethod(javaCelebornShuffleWriter_, javaCelebornPushPartitionData_, partitionId, array);
    checkException(env);
  }

  JavaVM* vm_;
  jobject javaCelebornShuffleWriter_;
  jmethodID javaCelebornPushPartitionData_;
};
