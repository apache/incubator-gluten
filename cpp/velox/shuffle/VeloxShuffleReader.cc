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
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  auto nulls = convertToVeloxBuffer(buffers[bufferIdx]);
  bufferIdx++;
  auto values = convertToVeloxBuffer(buffers[bufferIdx]);
  bufferIdx++;
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
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  auto nulls = convertToVeloxBuffer(buffers[bufferIdx]);
  bufferIdx++;
  auto arrowValueBuffer = buffers[bufferIdx];
  // Because if buffer does not compress, it will get from netty, the address maynot aligned 16B, which will cause
  // int128_t = xxx coredump by instruction movdqa
  auto data = static_cast<const int128_t*>(reinterpret_cast<const void*>(arrowValueBuffer->data()));
  BufferPtr values;
  if ((reinterpret_cast<uintptr_t>(data) & 0xf) == 0) {
    values = convertToVeloxBuffer(arrowValueBuffer);
  } else {
    values = AlignedBuffer::allocate<char>(arrowValueBuffer->size(), pool);
    memcpy(values->asMutable<char>(), arrowValueBuffer->data(), arrowValueBuffer->size());
  }
  bufferIdx++;
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
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  auto nulls = convertToVeloxBuffer(buffers[bufferIdx]);
  bufferIdx++;
  auto offsetBuffers = convertToVeloxBuffer(buffers[bufferIdx]);
  bufferIdx++;
  auto valueBuffers = convertToVeloxBuffer(buffers[bufferIdx]);
  bufferIdx++;
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
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    int32_t& bufferIdx,
    uint32_t length,
    std::shared_ptr<const Type> type,
    memory::MemoryPool* pool) {
  return readFlatVectorStringView(buffers, bufferIdx, length, type, pool);
}

template <>
VectorPtr readFlatVector<TypeKind::VARBINARY>(
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
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

RowVectorPtr readComplexType(std::shared_ptr<arrow::Buffer> buffer, RowTypePtr& rowType, memory::MemoryPool* pool) {
  RowVectorPtr result;
  auto byteStream = toByteStream(const_cast<uint8_t*>(buffer->data()), buffer->size());
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
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
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

RowVectorPtr deserialize(
    RowTypePtr type,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> children;
  auto childTypes = type->as<TypeKind::ROW>().children();
  readColumns(buffers, pool, numRows, childTypes, children);
  return std::make_shared<RowVector>(pool, type, BufferPtr(nullptr), numRows, children);
}

std::shared_ptr<arrow::Buffer> readColumnBuffer(const arrow::RecordBatch& batch, int32_t fieldIdx) {
  return std::dynamic_pointer_cast<arrow::LargeStringArray>(batch.column(fieldIdx))->value_data();
}

std::shared_ptr<arrow::Buffer> getUncompressedBuffers(
    const arrow::RecordBatch& batch,
    arrow::MemoryPool* arrowPool,
    const arrow::Compression::type compressType,
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  auto lengthBuffer = readColumnBuffer(batch, 1);
  const int64_t* lengthPtr = reinterpret_cast<const int64_t*>(lengthBuffer->data());
  int64_t uncompressLength = lengthPtr[0];
  int64_t compressLength = lengthPtr[1];
  auto valueBufferLength = lengthPtr[2];
  auto compressBuffer = readColumnBuffer(batch, 2);
  auto codec = createArrowIpcCodec(compressType);
  std::shared_ptr<arrow::ResizableBuffer> uncompressBuffer;
  GLUTEN_ASSIGN_OR_THROW(uncompressBuffer, arrow::AllocateResizableBuffer(uncompressLength, arrowPool));
  GLUTEN_THROW_NOT_OK(uncompressBuffer->Resize(0, false));
  zstdUncompress(compressBuffer.get(), uncompressBuffer.get());
  const std::shared_ptr<arrow::Buffer> kNullBuffer = std::make_shared<arrow::Buffer>(nullptr, 0);
  int64_t bufferOffset = 0;
  for (int64_t i = 3; i < valueBufferLength + 3; i++) {
    if (lengthPtr[i] == 0) {
      buffers.emplace_back(kNullBuffer);
    } else {
      auto uncompressBufferSlice = arrow::SliceBuffer(uncompressBuffer, bufferOffset, lengthPtr[i]);
      buffers.emplace_back(uncompressBufferSlice);
      bufferOffset += lengthPtr[i];
    }
  }
  // This buffer should not release until the deserialized RowVector released
  // TODO: will bind to each RowVector buffer in the final version
  // if it has performance gain
  return uncompressBuffer;
}

RowVectorPtr readRowVectorInternal(
    const arrow::RecordBatch& batch,
    RowTypePtr rowType,
    int64_t& decompressTime,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool,
    std::vector<std::shared_ptr<arrow::Buffer>> unCompressedBuffers_) {
  auto header = readColumnBuffer(batch, 0);
  uint32_t length;
  mempcpy(&length, header->data(), sizeof(uint32_t));
  int32_t compressTypeValue;
  mempcpy(&compressTypeValue, header->data() + sizeof(uint32_t), sizeof(int32_t));
  arrow::Compression::type compressType = static_cast<arrow::Compression::type>(compressTypeValue);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  buffers.reserve(batch.num_columns() * 2);
  if (compressType == arrow::Compression::type::UNCOMPRESSED) {
    for (int32_t i = 0; i < batch.num_columns() - 1; i++) {
      auto buffer = readColumnBuffer(batch, i + 1);
      buffers.emplace_back(buffer);
    }
  } else {
    TIME_NANO_START(decompressTime);
    auto uncompressBuffer = getUncompressedBuffers(batch, arrowPool, compressType, buffers);
    unCompressedBuffers_.emplace_back(uncompressBuffer);
    TIME_NANO_END(decompressTime);
  }
  return deserialize(rowType, length, buffers, pool);
}
} // namespace

VeloxShuffleReader::VeloxShuffleReader(
    std::shared_ptr<arrow::io::InputStream> in,
    std::shared_ptr<arrow::Schema> schema,
    ReaderOptions options,
    std::shared_ptr<arrow::MemoryPool> pool,
    std::shared_ptr<memory::MemoryPool> veloxPool)
    : Reader(in, schema, options, pool), veloxPool_(std::move(veloxPool)) {
  ArrowSchema cSchema;
  GLUTEN_THROW_NOT_OK(arrow::ExportSchema(*schema, &cSchema));
  rowType_ = asRowType(importFromArrow(cSchema));
}

arrow::Result<std::shared_ptr<ColumnarBatch>> VeloxShuffleReader::next() {
  ARROW_ASSIGN_OR_RAISE(auto batch, Reader::next());
  if (batch == nullptr) {
    return nullptr;
  }
  auto rb = std::dynamic_pointer_cast<ArrowColumnarBatch>(batch)->getRecordBatch();
  auto vp = readRowVectorInternal(*rb, rowType_, decompressTime_, pool_.get(), veloxPool_.get(), unCompressedBuffers_);
  return std::make_shared<VeloxColumnarBatch>(vp);
}

RowVectorPtr VeloxShuffleReader::readRowVector(
    const arrow::RecordBatch& rb,
    RowTypePtr rowType,
    arrow::MemoryPool* arrowPool,
    memory::MemoryPool* pool) {
  int64_t decompressTime = 0;
  std::vector<std::shared_ptr<arrow::Buffer>> unCompressedBuffers_;
  return readRowVectorInternal(rb, rowType, decompressTime, arrowPool, pool, unCompressedBuffers_);
}

} // namespace gluten
