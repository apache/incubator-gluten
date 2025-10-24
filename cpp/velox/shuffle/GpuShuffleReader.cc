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

#include "shuffle/GpuShuffleReader.h"

#include <arrow/array/array_binary.h>
#include <arrow/io/buffered.h>

#include "memory/VeloxColumnarBatch.h"
#include "shuffle/Payload.h"
#include "shuffle/Utils.h"
#include "utils/Common.h"
#include "utils/Macros.h"
#include "utils/Timer.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <algorithm>

#include "cudf/GpuLock.h"

using namespace facebook::velox;

namespace gluten {
namespace {

arrow::Result<BlockType> readBlockType(arrow::io::InputStream* inputStream) {
  BlockType type;
  ARROW_ASSIGN_OR_RAISE(auto bytes, inputStream->Read(sizeof(BlockType), &type));
  if (bytes == 0) {
    // Reach EOS.
    return BlockType::kEndOfStream;
  }
  return type;
}

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

template <TypeKind Kind, typename T = typename TypeTraits<Kind>::NativeType>
VectorPtr readFlatVector(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto valuesOrIndices = buffers[bufferIdx++];

  nulls = nulls == nullptr || nulls->size() == 0 ? BufferPtr(nullptr) : nulls;

  if (dictionary != nullptr) {
    return BaseVector::wrapInDictionary(nulls, valuesOrIndices, length, dictionary);
  }

  return std::make_shared<FlatVector<T>>(
      pool, type, nulls, length, std::move(valuesOrIndices), std::vector<BufferPtr>{});
}

template <>
VectorPtr readFlatVector<TypeKind::UNKNOWN>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  return BaseVector::createNullConstant(type, length, pool);
}

template <>
VectorPtr readFlatVector<TypeKind::HUGEINT>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto valuesOrIndices = buffers[bufferIdx++];

  // Because if buffer does not compress, it will get from netty, the address maynot aligned 16B, which will cause
  // int128_t = xxx coredump by instruction movdqa
  const auto* addr = valuesOrIndices->as<facebook::velox::int128_t>();
  if ((reinterpret_cast<uintptr_t>(addr) & 0xf) != 0) {
    auto alignedBuffer = AlignedBuffer::allocate<char>(valuesOrIndices->size(), pool);
    fastCopy(alignedBuffer->asMutable<char>(), valuesOrIndices->as<char>(), valuesOrIndices->size());
    valuesOrIndices = alignedBuffer;
  }

  nulls = nulls == nullptr || nulls->size() == 0 ? BufferPtr(nullptr) : nulls;

  if (dictionary != nullptr) {
    return BaseVector::wrapInDictionary(nulls, valuesOrIndices, length, dictionary);
  }

  return std::make_shared<FlatVector<int128_t>>(
      pool, type, nulls, length, std::move(valuesOrIndices), std::vector<BufferPtr>{});
}

VectorPtr readFlatVectorStringView(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  auto nulls = buffers[bufferIdx++];
  auto lengthOrIndices = buffers[bufferIdx++];

  nulls = nulls == nullptr || nulls->size() == 0 ? BufferPtr(nullptr) : nulls;

  if (dictionary != nullptr) {
    return BaseVector::wrapInDictionary(nulls, lengthOrIndices, length, dictionary);
  }

  auto valueBuffer = buffers[bufferIdx++];

  const auto* rawLength = lengthOrIndices->as<StringLengthType>();
  const auto* valueBufferPtr = valueBuffer->as<char>();

  auto values = AlignedBuffer::allocate<char>(sizeof(StringView) * length, pool);
  auto* rawValues = values->asMutable<StringView>();

  uint64_t offset = 0;
  for (int32_t i = 0; i < length; ++i) {
    rawValues[i] = StringView(valueBufferPtr + offset, rawLength[i]);
    offset += rawLength[i];
  }

  std::vector<BufferPtr> stringBuffers;
  stringBuffers.emplace_back(valueBuffer);

  return std::make_shared<FlatVector<StringView>>(
      pool, type, nulls, length, std::move(values), std::move(stringBuffers));
}

template <>
VectorPtr readFlatVector<TypeKind::VARCHAR>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  return readFlatVectorStringView(buffers, bufferIdx, length, type, dictionary, pool);
}

template <>
VectorPtr readFlatVector<TypeKind::VARBINARY>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    const VectorPtr& dictionary,
    memory::MemoryPool* pool) {
  return readFlatVectorStringView(buffers, bufferIdx, length, type, dictionary, pool);
}

std::unique_ptr<ByteInputStream> toByteStream(uint8_t* data, int32_t size) {
  std::vector<ByteRange> byteRanges;
  byteRanges.push_back(ByteRange{data, size, 0});
  auto byteStream = std::make_unique<BufferInputStream>(byteRanges);
  return byteStream;
}

RowVectorPtr readComplexType(BufferPtr buffer, RowTypePtr& rowType, memory::MemoryPool* pool) {
  RowVectorPtr result;
  auto byteStream = toByteStream(const_cast<uint8_t*>(buffer->as<uint8_t>()), buffer->size());
  auto serde = std::make_unique<serializer::presto::PrestoVectorSerde>();
  serializer::presto::PrestoVectorSerde::PrestoOptions options;
  options.useLosslessTimestamp = true;
  serde->deserialize(byteStream.get(), pool, rowType, &result, &options);
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

RowVectorPtr deserialize(
    RowTypePtr type,
    uint32_t numRows,
    std::vector<BufferPtr>& buffers,
    const std::vector<int32_t>& dictionaryFields,
    const std::vector<VectorPtr>& dictionaries,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> children;
  auto types = type->as<TypeKind::ROW>().children();

  std::vector<VectorPtr> complexChildren;
  auto complexRowType = getComplexWriteType(types);
  if (complexRowType->children().size() > 0) {
    complexChildren = readComplexType(buffers[buffers.size() - 1], complexRowType, pool)->children();
  }

  int32_t bufferIdx = 0;
  int32_t complexIdx = 0;
  int32_t dictionaryIdx = 0;
  for (size_t i = 0; i < types.size(); ++i) {
    const auto kind = types[i]->kind();
    switch (kind) {
      case TypeKind::ROW:
      case TypeKind::MAP:
      case TypeKind::ARRAY: {
        children.emplace_back(std::move(complexChildren[complexIdx]));
        complexIdx++;
      } break;
      default: {
        VectorPtr dictionary{nullptr};
        if (!dictionaryFields.empty() && dictionaryIdx < dictionaryFields.size() &&
            dictionaryFields[dictionaryIdx] == i) {
          dictionary = dictionaries[dictionaryIdx++];
        }
        auto res = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            readFlatVector, kind, buffers, bufferIdx, numRows, types[i], dictionary, pool);
        children.emplace_back(std::move(res));
      } break;
    }
  }

  return std::make_shared<RowVector>(pool, type, BufferPtr(nullptr), numRows, children);
}

std::shared_ptr<VeloxColumnarBatch> makeColumnarBatch(
    RowTypePtr type,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> arrowBuffers,
    const std::vector<int32_t>& dictionaryFields,
    const std::vector<VectorPtr>& dictionaries,
    memory::MemoryPool* pool,
    int64_t& deserializeTime) {
  ScopedTimer timer(&deserializeTime);
  std::vector<BufferPtr> veloxBuffers;
  veloxBuffers.reserve(arrowBuffers.size());
  for (auto& buffer : arrowBuffers) {
    veloxBuffers.push_back(convertToVeloxBuffer(std::move(buffer)));
  }
  auto rowVector = deserialize(type, numRows, veloxBuffers, dictionaryFields, dictionaries, pool);
  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

} // namespace


GpuHashShuffleReaderDeserializer::GpuHashShuffleReaderDeserializer(
    const std::shared_ptr<StreamReader>& streamReader,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const facebook::velox::RowTypePtr& rowType,
    int32_t batchSize,
    int64_t readerBufferSize,
    VeloxMemoryManager* memoryManager,
    std::vector<bool>* isValidityBuffer,
    bool hasComplexType,
    int64_t& deserializeTime,
    int64_t& decompressTime)
    : streamReader_(streamReader),
      schema_(schema),
      codec_(codec),
      rowType_(rowType),
      batchSize_(batchSize),
      readerBufferSize_(readerBufferSize),
      memoryManager_(memoryManager),
      isValidityBuffer_(isValidityBuffer),
      hasComplexType_(hasComplexType),
      deserializeTime_(deserializeTime),
      decompressTime_(decompressTime) {}

bool GpuHashShuffleReaderDeserializer::resolveNextBlockType() {
  GLUTEN_ASSIGN_OR_THROW(auto blockType, readBlockType(in_.get()));
  switch (blockType) {
    case BlockType::kEndOfStream:
      return false;
    case BlockType::kPlainPayload:
      return true;
    default:
      throw GlutenException(fmt::format("Unsupported block type: {}", static_cast<int32_t>(blockType)));
  }
  return true;
}

void GpuHashShuffleReaderDeserializer::loadNextStream() {
  if (reachedEos_) {
    return;
  }

  auto in = streamReader_->readNextStream(memoryManager_->defaultArrowMemoryPool());
  if (in == nullptr) {
    reachedEos_ = true;
    return;
  }

  GLUTEN_ASSIGN_OR_THROW(
      in_,
      arrow::io::BufferedInputStream::Create(
          readerBufferSize_, memoryManager_->defaultArrowMemoryPool(), std::move(in)));
}

std::shared_ptr<ColumnarBatch> GpuHashShuffleReaderDeserializer::next() {
  if (in_ == nullptr) {
    loadNextStream();

    if (reachedEos_) {
      return nullptr;
    }
  }

  while (!resolveNextBlockType()) {
    loadNextStream();

    if (reachedEos_) {
      return nullptr;
    }
  }

  uint32_t numRows = 0;
  GLUTEN_ASSIGN_OR_THROW(
      auto arrowBuffers,
      BlockPayload::deserialize(
          in_.get(), codec_, memoryManager_->defaultArrowMemoryPool(), numRows, deserializeTime_, decompressTime_));

  auto batch  = makeColumnarBatch(
      rowType_,
      numRows,
      std::move(arrowBuffers),
      dictionaryFields_,
      dictionaries_,
      memoryManager_->getLeafMemoryPool().get(),
      deserializeTime_);

  lockGpu();

  return batch;
}

}
