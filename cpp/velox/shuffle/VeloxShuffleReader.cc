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

#include "VeloxShuffleReader.h"

#include <arrow/array/array_binary.h>

#include "memory/VeloxColumnarBatch.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/compression.h"
#include "utils/macros.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <iostream>

// using namespace facebook;
using namespace facebook::velox;

namespace gluten {

namespace {

struct BufferViewReleaser {
  BufferViewReleaser() : BufferViewReleaser(nullptr) {}
  BufferViewReleaser(std::shared_ptr<arrow::Buffer> arrowBuffer) : bufferReleaser_(std::move(arrowBuffer)) {}

  void addRef() const {}
  void release() const {}

 private:
  const std::shared_ptr<arrow::Buffer> bufferReleaser_;
};

BufferPtr wrapInBufferViewAsOwner(const void* buffer, size_t length, std::shared_ptr<arrow::Buffer> bufferReleaser) {
  return BufferView<BufferViewReleaser>::create(
      static_cast<const uint8_t*>(buffer), length, {std::move(bufferReleaser)});
}

BufferPtr convertToVeloxBuffer(std::shared_ptr<arrow::Buffer> buffer) {
  if (buffer == nullptr) {
    return nullptr;
  }
  return wrapInBufferViewAsOwner(buffer->data(), buffer->size(), buffer);
}

template <TypeKind kind>
VectorPtr readFlatVector(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto values = buffers[bufferIdx++];
  std::vector<BufferPtr> stringBuffers;
  using T = typename TypeTraits<kind>::NativeType;
  if (nulls == nullptr || nulls->size() == 0) {
    return std::make_shared<FlatVector<T>>(
        pool, type, BufferPtr(nullptr), length, std::move(values), std::move(stringBuffers));
  }
  return std::make_shared<FlatVector<T>>(
      pool, type, std::move(nulls), length, std::move(values), std::move(stringBuffers));
}

template <>
VectorPtr readFlatVector<TypeKind::HUGEINT>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto valueBuffer = buffers[bufferIdx++];
  // Because if buffer does not compress, it will get from netty, the address maynot aligned 16B, which will cause
  // int128_t = xxx coredump by instruction movdqa
  auto data = valueBuffer->as<int128_t>();
  BufferPtr values;
  if ((reinterpret_cast<uintptr_t>(data) & 0xf) == 0) {
    values = valueBuffer;
  } else {
    values = AlignedBuffer::allocate<char>(valueBuffer->size(), pool);
    memcpy(values->asMutable<char>(), valueBuffer->as<char>(), valueBuffer->size());
  }
  std::vector<BufferPtr> stringBuffers;
  if (nulls == nullptr || nulls->size() == 0) {
    auto vp = std::make_shared<FlatVector<int128_t>>(
        pool, type, BufferPtr(nullptr), length, std::move(values), std::move(stringBuffers));
    return vp;
  }
  return std::make_shared<FlatVector<int128_t>>(
      pool, type, std::move(nulls), length, std::move(values), std::move(stringBuffers));
}

VectorPtr readFlatVectorStringView(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto offsetBuffers = buffers[bufferIdx++];
  auto valueBuffers = buffers[bufferIdx++];
  const int32_t* rawOffset = offsetBuffers->as<int32_t>();

  std::vector<BufferPtr> stringBuffers;
  auto values = AlignedBuffer::allocate<char>(sizeof(StringView) * length, pool);
  auto rawValues = values->asMutable<StringView>();
  auto rawChars = valueBuffers->as<char>();
  for (int32_t i = 0; i < length; ++i) {
    rawValues[i] = StringView(rawChars + rawOffset[i], rawOffset[i + 1] - rawOffset[i]);
  }
  stringBuffers.emplace_back(valueBuffers);
  if (nulls == nullptr || nulls->size() == 0) {
    return std::make_shared<FlatVector<StringView>>(
        pool, type, BufferPtr(nullptr), length, std::move(values), std::move(stringBuffers));
  }
  return std::make_shared<FlatVector<StringView>>(
      pool, type, std::move(nulls), length, std::move(values), std::move(stringBuffers));
}

template <>
VectorPtr readFlatVector<TypeKind::VARCHAR>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  return readFlatVectorStringView(buffers, bufferIdx, length, type, pool);
}

template <>
VectorPtr readFlatVector<TypeKind::VARBINARY>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  return readFlatVectorStringView(buffers, bufferIdx, length, type, pool);
}

std::unique_ptr<ByteStream> toByteStream(uint8_t* data, int32_t size) {
  auto byteStream = std::make_unique<ByteStream>();
  ByteRange byteRange{data, size, 0};
  byteStream->resetInput({byteRange});
  return byteStream;
}

RowVectorPtr readComplexType(BufferPtr buffer, RowTypePtr& rowType, memory::MemoryPool* pool) {
  RowVectorPtr result;
  auto byteStream = toByteStream(const_cast<uint8_t*>(buffer->as<uint8_t>()), buffer->size());
  auto serde = std::make_unique<serializer::presto::PrestoVectorSerde>();
  serde->deserialize(byteStream.get(), pool, rowType, &result, /* serdeOptions */ nullptr);
  return result;
}

RowTypePtr getComplexWriteType(const std::vector<TypePtr>& types) {
  std::vector<std::string> complexTypeColNames;
  std::vector<TypePtr> complexTypeChildrens;
  for (int32_t i = 0; i < types.size(); ++i) {
    auto kind = types[i]->kind();
    switch (kind) {
      case TypeKind::ROW:
      case TypeKind::MAP:
      case TypeKind::ARRAY: {
        complexTypeColNames.emplace_back(types[i]->name());
        complexTypeChildrens.emplace_back(types[i]);
      } break;
      default:
        break;
    }
  }
  return std::make_shared<const RowType>(std::move(complexTypeColNames), std::move(complexTypeChildrens));
}

void readColumns(
    std::vector<BufferPtr>& buffers,
    memory::MemoryPool* pool,
    uint32_t numRows,
    const std::vector<TypePtr>& types,
    std::vector<VectorPtr>& result) {
  int32_t bufferIdx = 0;
  std::vector<VectorPtr> complexChildren;
  auto complexRowType = getComplexWriteType(types);
  if (complexRowType->children().size() > 0) {
    complexChildren = readComplexType(buffers[buffers.size() - 1], complexRowType, pool)->children();
  }

  int32_t complexIdx = 0;
  for (int32_t i = 0; i < types.size(); ++i) {
    auto kind = types[i]->kind();
    switch (kind) {
      case TypeKind::ROW:
      case TypeKind::MAP:
      case TypeKind::ARRAY: {
        result.emplace_back(std::move(complexChildren[complexIdx]));
        complexIdx++;
      } break;
      default: {
        auto res = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            readFlatVector, types[i]->kind(), buffers, bufferIdx, numRows, types[i], pool);
        result.emplace_back(std::move(res));
      } break;
    }
  }
}

RowVectorPtr deserialize(RowTypePtr type, uint32_t numRows, std::vector<BufferPtr>& buffers, memory::MemoryPool* pool) {
  std::vector<VectorPtr> children;
  auto childTypes = type->as<TypeKind::ROW>().children();
  readColumns(buffers, pool, numRows, childTypes, children);
  return std::make_shared<RowVector>(pool, type, BufferPtr(nullptr), numRows, children);
}

std::shared_ptr<arrow::Buffer> readColumnBuffer(const arrow::RecordBatch& batch, int32_t fieldIdx) {
  return std::dynamic_pointer_cast<arrow::LargeStringArray>(batch.column(fieldIdx))->value_data();
}

void getUncompressedBuffersOneByOne(
    arrow::MemoryPool* arrowPool,
    arrow::util::Codec* codec,
    const int64_t* lengthPtr,
    std::shared_ptr<arrow::Buffer> valueBuffer,
    std::vector<BufferPtr>& buffers) {
  int64_t valueOffset = 0;
  auto valueBufferLength = lengthPtr[0];
  for (int64_t i = 0, j = 1; i < valueBufferLength; i++, j = j + 2) {
    int64_t uncompressLength = lengthPtr[j];
    int64_t compressLength = lengthPtr[j + 1];
    auto compressBuffer = arrow::SliceBuffer(valueBuffer, valueOffset, compressLength);
    valueOffset += compressLength;
    // Small buffer, not compressed
    if (uncompressLength == -1) {
      buffers.emplace_back(convertToVeloxBuffer(compressBuffer));
    } else {
      std::shared_ptr<arrow::Buffer> uncompressBuffer = std::make_shared<arrow::Buffer>(nullptr, 0);
      if (uncompressLength != 0) {
        GLUTEN_ASSIGN_OR_THROW(uncompressBuffer, arrow::AllocateBuffer(uncompressLength, arrowPool));
        GLUTEN_ASSIGN_OR_THROW(
            auto actualDecompressLength,
            codec->Decompress(
                compressLength, compressBuffer->data(), uncompressLength, uncompressBuffer->mutable_data()));
        VELOX_DCHECK_EQ(actualDecompressLength, uncompressLength);
      }
      buffers.emplace_back(convertToVeloxBuffer(uncompressBuffer));
    }
  }
}

void getUncompressedBuffersStream(
    arrow::MemoryPool* arrowPool,
    arrow::util::Codec* codec,
    const int64_t* lengthPtr,
    std::shared_ptr<arrow::Buffer> compressBuffer,
    std::vector<BufferPtr>& buffers) {
  int64_t uncompressLength = lengthPtr[0];
  int64_t compressLength = lengthPtr[1];
  auto valueBufferLength = lengthPtr[2];
  if (uncompressLength != -1) {
    std::shared_ptr<arrow::Buffer> uncompressBuffer;
    GLUTEN_ASSIGN_OR_THROW(uncompressBuffer, arrow::AllocateBuffer(uncompressLength, arrowPool));
    GLUTEN_ASSIGN_OR_THROW(
        auto actualDecompressLength,
        codec->Decompress(compressLength, compressBuffer->data(), uncompressLength, uncompressBuffer->mutable_data()));
    VELOX_DCHECK_EQ(actualDecompressLength, uncompressLength);
    const std::shared_ptr<arrow::Buffer> kNullBuffer = std::make_shared<arrow::Buffer>(nullptr, 0);
    int64_t bufferOffset = 0;
    for (int64_t i = 3; i < valueBufferLength + 3; i++) {
      if (lengthPtr[i] == 0) {
        buffers.emplace_back(convertToVeloxBuffer(kNullBuffer));
      } else {
        auto uncompressBufferSlice = arrow::SliceBuffer(uncompressBuffer, bufferOffset, lengthPtr[i]);
        buffers.emplace_back(convertToVeloxBuffer(uncompressBufferSlice));
        bufferOffset += lengthPtr[i];
      }
    }
  } else {
    const std::shared_ptr<arrow::Buffer> kNullBuffer = std::make_shared<arrow::Buffer>(nullptr, 0);
    int64_t bufferOffset = 0;
    for (int64_t i = 3; i < valueBufferLength + 3; i++) {
      if (lengthPtr[i] == 0) {
        buffers.emplace_back(convertToVeloxBuffer(kNullBuffer));
      } else {
        auto uncompressBufferSlice = arrow::SliceBuffer(compressBuffer, bufferOffset, lengthPtr[i]);
        buffers.emplace_back(convertToVeloxBuffer(uncompressBufferSlice));
        bufferOffset += lengthPtr[i];
      }
    }
  }
}

void getUncompressedBuffers(
    const arrow::RecordBatch& batch,
    arrow::MemoryPool* arrowPool,
    arrow::util::Codec* codec,
    CompressionMode compressionMode,
    std::vector<BufferPtr>& buffers) {
  auto lengthBuffer = readColumnBuffer(batch, 1);
  const int64_t* lengthPtr = reinterpret_cast<const int64_t*>(lengthBuffer->data());
  auto valueBuffer = readColumnBuffer(batch, 2);
  if (compressionMode == CompressionMode::BUFFER) {
    getUncompressedBuffersOneByOne(arrowPool, codec, lengthPtr, valueBuffer, buffers);
  } else {
    getUncompressedBuffersStream(arrowPool, codec, lengthPtr, valueBuffer, buffers);
  }
}

RowVectorPtr readRowVector(
    const arrow::RecordBatch& batch,
    RowTypePtr rowType,
    CodecBackend codecBackend,
    CompressionMode compressionMode,
    int64_t& decompressTime,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool) {
  auto header = readColumnBuffer(batch, 0);
  uint32_t length;
  memcpy(&length, header->data(), sizeof(uint32_t));
  int32_t compressTypeValue;
  memcpy(&compressTypeValue, header->data() + sizeof(uint32_t), sizeof(int32_t));
  arrow::Compression::type compressType = static_cast<arrow::Compression::type>(compressTypeValue);

  std::vector<BufferPtr> buffers;
  buffers.reserve(batch.num_columns() * 2);
  if (compressType == arrow::Compression::type::UNCOMPRESSED) {
    for (int32_t i = 0; i < batch.num_columns() - 1; i++) {
      auto buffer = readColumnBuffer(batch, i + 1);
      buffers.emplace_back(convertToVeloxBuffer(buffer));
    }
  } else {
    TIME_NANO_START(decompressTime);
    auto codec = createArrowIpcCodec(compressType, codecBackend);
    getUncompressedBuffers(batch, arrowPool, codec.get(), compressionMode, buffers);
    TIME_NANO_END(decompressTime);
  }
  return deserialize(rowType, length, buffers, pool);
}

class VeloxShuffleReaderOutStream : public ColumnarBatchIterator {
 public:
  VeloxShuffleReaderOutStream(
      const std::shared_ptr<arrow::MemoryPool>& pool,
      const std::shared_ptr<facebook::velox::memory::MemoryPool>& veloxPool,
      const ReaderOptions& options,
      const RowTypePtr& rowType,
      const std::function<void(int64_t&)> decompressionTimeAccumulator,
      ResultIterator& in)
      : pool_(pool),
        veloxPool_(veloxPool),
        options_(options),
        rowType_(rowType),
        decompressionTimeAccumulator_(decompressionTimeAccumulator),
        in_(std::move(in)) {}

  std::shared_ptr<ColumnarBatch> next() override {
    if (!in_.hasNext()) {
      return nullptr;
    }
    auto batch = in_.next();
    auto rb = std::dynamic_pointer_cast<ArrowColumnarBatch>(batch)->getRecordBatch();
    int64_t decompressTime = 0LL;
    auto vp = readRowVector(
        *rb,
        rowType_,
        options_.codec_backend,
        options_.compression_mode,
        decompressTime,
        pool_.get(),
        veloxPool_.get());
    decompressionTimeAccumulator_(decompressTime);
    return std::make_shared<VeloxColumnarBatch>(vp);
  }

 private:
  std::shared_ptr<arrow::MemoryPool> pool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
  ReaderOptions options_;
  facebook::velox::RowTypePtr rowType_;
  std::function<void(int64_t&)> decompressionTimeAccumulator_;
  ResultIterator in_;
};

} // namespace

VeloxShuffleReader::VeloxShuffleReader(
    std::shared_ptr<arrow::Schema> schema,
    ReaderOptions options,
    std::shared_ptr<arrow::MemoryPool> pool,
    std::shared_ptr<memory::MemoryPool> veloxPool)
    : ShuffleReader(schema, options, pool), veloxPool_(std::move(veloxPool)) {
  rowType_ = asRowType(gluten::fromArrowSchema(schema));
}

std::shared_ptr<ResultIterator> VeloxShuffleReader::readStream(std::shared_ptr<arrow::io::InputStream> in) {
  auto wrappedIn = ShuffleReader::readStream(in);
  return std::make_shared<ResultIterator>(std::make_unique<VeloxShuffleReaderOutStream>(
      pool_,
      veloxPool_,
      options_,
      rowType_,
      [this](int64_t decompressionTime) { this->decompressTime_ += decompressionTime; },
      *wrappedIn));
}

} // namespace gluten
