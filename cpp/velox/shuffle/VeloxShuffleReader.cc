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
#include <arrow/io/buffered.h>

#include "memory/VeloxColumnarBatch.h"
#include "shuffle/Payload.h"
#include "shuffle/Utils.h"
#include "utils/Common.h"
#include "utils/Compression.h"
#include "utils/Timer.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/macros.h"
#include "velox/row/CompactRow.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

#include <algorithm>
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
VectorPtr readFlatVector<TypeKind::UNKNOWN>(
    std::vector<BufferPtr>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  return BaseVector::createNullConstant(type, length, pool);
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
    gluten::fastCopy(values->asMutable<char>(), valueBuffer->as<char>(), valueBuffer->size());
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
  auto lengthBuffer = buffers[bufferIdx++];
  auto valueBuffer = buffers[bufferIdx++];
  const auto* rawLength = lengthBuffer->as<BinaryArrayLengthBufferType>();

  std::vector<BufferPtr> stringBuffers;
  auto values = AlignedBuffer::allocate<char>(sizeof(StringView) * length, pool);
  auto rawValues = values->asMutable<StringView>();
  auto rawChars = valueBuffer->as<char>();
  uint64_t offset = 0;
  for (int32_t i = 0; i < length; ++i) {
    rawValues[i] = StringView(rawChars + offset, rawLength[i]);
    offset += rawLength[i];
  }
  stringBuffers.emplace_back(valueBuffer);
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

std::unique_ptr<ByteInputStream> toByteStream(uint8_t* data, int32_t size) {
  std::vector<ByteRange> byteRanges;
  byteRanges.push_back(ByteRange{data, size, 0});
  auto byteStream = std::make_unique<ByteInputStream>(byteRanges);
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

std::shared_ptr<VeloxColumnarBatch> makeColumnarBatch(
    RowTypePtr type,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> arrowBuffers,
    memory::MemoryPool* pool,
    int64_t& deserializeTime) {
  ScopedTimer timer(&deserializeTime);
  std::vector<BufferPtr> veloxBuffers;
  veloxBuffers.reserve(arrowBuffers.size());
  for (auto& buffer : arrowBuffers) {
    veloxBuffers.push_back(convertToVeloxBuffer(std::move(buffer)));
  }
  auto rowVector = deserialize(type, numRows, veloxBuffers, pool);
  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

std::shared_ptr<VeloxColumnarBatch> makeColumnarBatch(
    RowTypePtr type,
    std::unique_ptr<InMemoryPayload> payload,
    memory::MemoryPool* pool,
    int64_t& deserializeTime) {
  ScopedTimer timer(&deserializeTime);
  std::vector<BufferPtr> veloxBuffers;
  auto numBuffers = payload->numBuffers();
  veloxBuffers.reserve(numBuffers);
  for (size_t i = 0; i < numBuffers; ++i) {
    GLUTEN_ASSIGN_OR_THROW(auto buffer, payload->readBufferAt(i));
    veloxBuffers.push_back(convertToVeloxBuffer(std::move(buffer)));
  }
  auto rowVector = deserialize(type, payload->numRows(), veloxBuffers, pool);
  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

} // namespace

VeloxHashShuffleReaderDeserializer::VeloxHashShuffleReaderDeserializer(
    std::shared_ptr<arrow::io::InputStream> in,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const facebook::velox::RowTypePtr& rowType,
    int32_t batchSize,
    arrow::MemoryPool* memoryPool,
    facebook::velox::memory::MemoryPool* veloxPool,
    std::vector<bool>* isValidityBuffer,
    bool hasComplexType,
    int64_t& deserializeTime,
    int64_t& decompressTime)
    : in_(std::move(in)),
      schema_(schema),
      codec_(codec),
      rowType_(rowType),
      batchSize_(batchSize),
      memoryPool_(memoryPool),
      veloxPool_(veloxPool),
      isValidityBuffer_(isValidityBuffer),
      hasComplexType_(hasComplexType),
      deserializeTime_(deserializeTime),
      decompressTime_(decompressTime) {}

std::shared_ptr<ColumnarBatch> VeloxHashShuffleReaderDeserializer::next() {
  if (hasComplexType_) {
    uint32_t numRows;
    GLUTEN_ASSIGN_OR_THROW(
        auto arrowBuffers,
        BlockPayload::deserialize(in_.get(), schema_, codec_, memoryPool_, numRows, decompressTime_));
    if (numRows == 0) {
      // Reach EOS.
      return nullptr;
    }
    return makeColumnarBatch(rowType_, numRows, std::move(arrowBuffers), veloxPool_, deserializeTime_);
  }

  if (reachEos_) {
    if (merged_) {
      return makeColumnarBatch(rowType_, std::move(merged_), veloxPool_, deserializeTime_);
    }
    return nullptr;
  }

  std::vector<std::shared_ptr<arrow::Buffer>> arrowBuffers{};
  uint32_t numRows = 0;
  while (!merged_ || merged_->numRows() < batchSize_) {
    GLUTEN_ASSIGN_OR_THROW(
        arrowBuffers, BlockPayload::deserialize(in_.get(), schema_, codec_, memoryPool_, numRows, decompressTime_));
    if (numRows == 0) {
      reachEos_ = true;
      break;
    }
    if (!merged_) {
      merged_ = std::make_unique<InMemoryPayload>(numRows, isValidityBuffer_, std::move(arrowBuffers));
      arrowBuffers.clear();
      continue;
    }
    auto mergedRows = merged_->numRows() + numRows;
    if (mergedRows > batchSize_) {
      break;
    }

    auto append = std::make_unique<InMemoryPayload>(numRows, isValidityBuffer_, std::move(arrowBuffers));
    GLUTEN_ASSIGN_OR_THROW(merged_, InMemoryPayload::merge(std::move(merged_), std::move(append), memoryPool_));
    arrowBuffers.clear();
  }

  // Reach EOS.
  if (reachEos_ && !merged_) {
    return nullptr;
  }

  auto columnarBatch = makeColumnarBatch(rowType_, std::move(merged_), veloxPool_, deserializeTime_);

  // Save remaining rows.
  if (!arrowBuffers.empty()) {
    merged_ = std::make_unique<InMemoryPayload>(numRows, isValidityBuffer_, std::move(arrowBuffers));
  }
  return columnarBatch;
}

VeloxSortShuffleReaderDeserializer::VeloxSortShuffleReaderDeserializer(
    std::shared_ptr<arrow::io::InputStream> in,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const RowTypePtr& rowType,
    int32_t batchSize,
    arrow::MemoryPool* memoryPool,
    facebook::velox::memory::MemoryPool* veloxPool,
    int64_t& deserializeTime,
    int64_t& decompressTime)
    : in_(std::move(in)),
      schema_(schema),
      codec_(codec),
      rowType_(rowType),
      batchSize_(batchSize),
      arrowPool_(memoryPool),
      veloxPool_(veloxPool),
      deserializeTime_(deserializeTime),
      decompressTime_(decompressTime) {}

std::shared_ptr<ColumnarBatch> VeloxSortShuffleReaderDeserializer::next() {
  if (reachEos_) {
    if (cachedRows_ > 0) {
      return deserializeToBatch();
    }
    return nullptr;
  }

  if (cachedRows_ >= batchSize_) {
    return deserializeToBatch();
  }

  while (cachedRows_ < batchSize_) {
    uint32_t numRows;
    GLUTEN_ASSIGN_OR_THROW(
        auto arrowBuffers, BlockPayload::deserialize(in_.get(), schema_, codec_, arrowPool_, numRows, decompressTime_));

    if (numRows == 0) {
      reachEos_ = true;
      if (cachedRows_ > 0) {
        return deserializeToBatch();
      }
      return nullptr;
    }
    auto buffer = std::move(arrowBuffers[0]);
    cachedInputs_.emplace_back(numRows, wrapInBufferViewAsOwner(buffer->data(), buffer->size(), buffer));
    cachedRows_ += numRows;
  }
  return deserializeToBatch();
}

std::shared_ptr<ColumnarBatch> VeloxSortShuffleReaderDeserializer::deserializeToBatch() {
  ScopedTimer timer(&deserializeTime_);
  std::vector<std::string_view> data;
  data.reserve(std::min(cachedRows_, batchSize_));

  uint32_t readRows = 0;
  auto cur = cachedInputs_.begin();
  while (readRows < batchSize_ && cur != cachedInputs_.end()) {
    auto buffer = cur->second;
    const auto* rawBuffer = buffer->as<char>();
    while (rowOffset_ < cur->first && readRows < batchSize_) {
      auto rowSize = *(RowSizeType*)(rawBuffer + byteOffset_) - sizeof(RowSizeType);
      byteOffset_ += sizeof(RowSizeType);
      data.push_back(std::string_view(rawBuffer + byteOffset_, rowSize));
      byteOffset_ += rowSize;
      ++rowOffset_;
      ++readRows;
    }
    if (rowOffset_ == cur->first) {
      rowOffset_ = 0;
      byteOffset_ = 0;
      ++cur;
    }
  }
  cachedRows_ -= readRows;
  auto rowVector = facebook::velox::row::CompactRow::deserialize(data, rowType_, veloxPool_);
  // Free memory.
  auto iter = cachedInputs_.begin();
  while (iter++ != cur) {
    cachedInputs_.pop_front();
  }
  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

class VeloxRssSortShuffleReaderDeserializer::VeloxInputStream : public facebook::velox::ByteInputStream {
 public:
  VeloxInputStream(std::shared_ptr<arrow::io::InputStream> input, facebook::velox::BufferPtr buffer);

  bool hasNext();

  void next(bool throwIfPastEnd) override;

  std::shared_ptr<arrow::io::InputStream> in_;
  const facebook::velox::BufferPtr buffer_;
  uint64_t offset_ = -1;
};

VeloxRssSortShuffleReaderDeserializer::VeloxInputStream::VeloxInputStream(
    std::shared_ptr<arrow::io::InputStream> input,
    facebook::velox::BufferPtr buffer)
    : in_(std::move(input)), buffer_(std::move(buffer)) {
  next(true);
}

bool VeloxRssSortShuffleReaderDeserializer::VeloxInputStream::hasNext() {
  if (offset_ == 0) {
    return false;
  }
  if (ranges()[0].position >= ranges()[0].size) {
    next(true);
    return offset_ != 0;
  }
  return true;
}

void VeloxRssSortShuffleReaderDeserializer::VeloxInputStream::next(bool throwIfPastEnd) {
  const uint32_t readBytes = buffer_->capacity();
  offset_ = in_->Read(readBytes, buffer_->asMutable<char>()).ValueOr(0);
  if (offset_ > 0) {
    int32_t realBytes = offset_;
    VELOX_CHECK_LT(0, realBytes, "Reading past end of file.");
    setRange({buffer_->asMutable<uint8_t>(), realBytes, 0});
  }
}

VeloxRssSortShuffleReaderDeserializer::VeloxRssSortShuffleReaderDeserializer(
    const std::shared_ptr<facebook::velox::memory::MemoryPool>& veloxPool,
    const RowTypePtr& rowType,
    int32_t batchSize,
    facebook::velox::common::CompressionKind veloxCompressionType,
    int64_t& deserializeTime,
    std::shared_ptr<arrow::io::InputStream> in)
    : veloxPool_(veloxPool),
      rowType_(rowType),
      batchSize_(batchSize),
      veloxCompressionType_(veloxCompressionType),
      deserializeTime_(deserializeTime) {
  constexpr uint64_t kMaxReadBufferSize = (1 << 20) - AlignedBuffer::kPaddedSize;
  auto buffer = AlignedBuffer::allocate<char>(kMaxReadBufferSize, veloxPool_.get());
  in_ = std::make_unique<VeloxInputStream>(std::move(in), std::move(buffer));
  serdeOptions_ = {false, veloxCompressionType_};
}

std::shared_ptr<ColumnarBatch> VeloxRssSortShuffleReaderDeserializer::next() {
  if (!in_->hasNext()) {
    return nullptr;
  }

  ScopedTimer timer(&deserializeTime_);

  RowVectorPtr rowVector;
  VectorStreamGroup::read(in_.get(), veloxPool_.get(), rowType_, &rowVector, &serdeOptions_);

  if (rowVector->size() >= batchSize_) {
    return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
  }

  while (rowVector->size() < batchSize_ && in_->hasNext()) {
    RowVectorPtr rowVectorTemp;
    VectorStreamGroup::read(in_.get(), veloxPool_.get(), rowType_, &rowVectorTemp, &serdeOptions_);
    rowVector->append(rowVectorTemp.get());
  }

  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

VeloxColumnarBatchDeserializerFactory::VeloxColumnarBatchDeserializerFactory(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    const facebook::velox::common::CompressionKind veloxCompressionType,
    const RowTypePtr& rowType,
    int32_t batchSize,
    arrow::MemoryPool* memoryPool,
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
    ShuffleWriterType shuffleWriterType)
    : schema_(schema),
      codec_(codec),
      veloxCompressionType_(veloxCompressionType),
      rowType_(rowType),
      batchSize_(batchSize),
      memoryPool_(memoryPool),
      veloxPool_(veloxPool),
      shuffleWriterType_(shuffleWriterType) {
  initFromSchema();
}

std::unique_ptr<ColumnarBatchIterator> VeloxColumnarBatchDeserializerFactory::createDeserializer(
    std::shared_ptr<arrow::io::InputStream> in) {
  switch (shuffleWriterType_) {
    case ShuffleWriterType::kHashShuffle:
      return std::make_unique<VeloxHashShuffleReaderDeserializer>(
          std::move(in),
          schema_,
          codec_,
          rowType_,
          batchSize_,
          memoryPool_,
          veloxPool_.get(),
          &isValidityBuffer_,
          hasComplexType_,
          deserializeTime_,
          decompressTime_);
    case ShuffleWriterType::kSortShuffle:
      return std::make_unique<VeloxSortShuffleReaderDeserializer>(
          std::move(in),
          schema_,
          codec_,
          rowType_,
          batchSize_,
          memoryPool_,
          veloxPool_.get(),
          deserializeTime_,
          decompressTime_);
    case ShuffleWriterType::kRssSortShuffle:
      return std::make_unique<VeloxRssSortShuffleReaderDeserializer>(
          veloxPool_, rowType_, batchSize_, veloxCompressionType_, deserializeTime_, std::move(in));
    default:
      throw gluten::GlutenException("Unsupported shuffle writer type: " + std::to_string(shuffleWriterType_));
  }
}

arrow::MemoryPool* VeloxColumnarBatchDeserializerFactory::getPool() {
  return memoryPool_;
}

ShuffleWriterType VeloxColumnarBatchDeserializerFactory::getShuffleWriterType() {
  return shuffleWriterType_;
}

int64_t VeloxColumnarBatchDeserializerFactory::getDecompressTime() {
  return decompressTime_;
}

int64_t VeloxColumnarBatchDeserializerFactory::getDeserializeTime() {
  return deserializeTime_;
}

void VeloxColumnarBatchDeserializerFactory::initFromSchema() {
  GLUTEN_ASSIGN_OR_THROW(auto arrowColumnTypes, toShuffleTypeId(schema_->fields()));
  isValidityBuffer_.reserve(arrowColumnTypes.size());
  for (size_t i = 0; i < arrowColumnTypes.size(); ++i) {
    switch (arrowColumnTypes[i]->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
        isValidityBuffer_.push_back(false);
      } break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id: {
        hasComplexType_ = true;
      } break;
      case arrow::BooleanType::type_id: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(true);
      } break;
      case arrow::NullType::type_id:
        break;
      default: {
        isValidityBuffer_.push_back(true);
        isValidityBuffer_.push_back(false);
      } break;
    }
  }
}

VeloxShuffleReader::VeloxShuffleReader(std::unique_ptr<DeserializerFactory> factory)
    : ShuffleReader(std::move(factory)) {}

} // namespace gluten
