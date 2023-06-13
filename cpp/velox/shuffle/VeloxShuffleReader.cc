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

void readColumns(
    std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    memory::MemoryPool* pool,
    uint32_t numRows,
    const std::vector<TypePtr>& types,
    std::vector<VectorPtr>& result) {
  int32_t bufferIdx = 0;
  for (int32_t i = 0; i < types.size(); ++i) {
    auto res = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
        readFlatVector, types[i]->kind(), buffers, bufferIdx, numRows, types[i], pool);
    result.emplace_back(std::move(res));
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
  auto des = std::make_shared<RowVector>(pool, type, BufferPtr(nullptr), numRows, children);
  return des;
}

std::shared_ptr<arrow::Buffer> readColumnBuffer(const arrow::RecordBatch& batch, int32_t fieldIdx) {
  if (batch.column(fieldIdx)->type()->id() == arrow::StringType::type_id) {
    return std::dynamic_pointer_cast<arrow::StringArray>(batch.column(fieldIdx))->value_data();
  } else {
    return std::dynamic_pointer_cast<arrow::LargeStringArray>(batch.column(fieldIdx))->value_data();
  }
}

RowVectorPtr readRowVectorInternal(const arrow::RecordBatch& batch, RowTypePtr rowType, memory::MemoryPool* pool) {
  auto header = readColumnBuffer(batch, 0);
  uint32_t length;
  mempcpy(&length, header->data(), sizeof(uint32_t));

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  buffers.reserve(batch.num_columns() - 1);
  for (int32_t i = 0; i < batch.num_columns() - 1; i++) {
    auto buffer = readColumnBuffer(batch, i + 1);
    buffers.emplace_back(buffer);
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
  auto vp = readRowVectorInternal(*rb, rowType_, getDefaultVeloxLeafMemoryPool().get());
  return std::make_shared<VeloxColumnarBatch>(vp);
}

RowVectorPtr
VeloxShuffleReader::readRowVector(const arrow::RecordBatch& rb, RowTypePtr rowType, memory::MemoryPool* pool) {
  return readRowVectorInternal(rb, rowType, pool);
}

} // namespace gluten
