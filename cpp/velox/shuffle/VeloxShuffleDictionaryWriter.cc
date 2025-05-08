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

#include "shuffle/VeloxShuffleDictionaryWriter.h"

#include <shuffle/Utils.h>
#include <utils/VeloxArrowUtils.h>
#include <velox/common/memory/MemoryPool.h>

#include <type_traits>

namespace gluten {

using DictionaryIndex = int32_t;

template <typename ValueType>
class VeloxDictionaryStorage : public DictionaryStorage {
 public:
  VeloxDictionaryStorage(facebook::velox::memory::MemoryPool* pool)
      : values_(0, facebook::velox::memory::StlAllocator<ValueType>(*pool)) {}

  DictionaryIndex getOrUpdate(const ValueType& value) {
    const auto& [it, inserted] = indexMap_.emplace(value, indexMap_.size());
    if (inserted) {
      values_.push_back(value);
    }
    return it->second;
  }

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    const size_t nBytes = values_.size() * sizeof(ValueType);

    RETURN_NOT_OK(out->Write(&nBytes, sizeof(nBytes)));
    RETURN_NOT_OK(out->Write(values_.data(), nBytes));

    return arrow::Status::OK();
  }

 private:
  std::vector<ValueType, facebook::velox::memory::StlAllocator<ValueType>> values_;
  std::unordered_map<ValueType, DictionaryIndex> indexMap_;
};

template <>
class VeloxDictionaryStorage<bool> : public DictionaryStorage {
 public:
  VeloxDictionaryStorage(facebook::velox::memory::MemoryPool*) {}

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    throw GlutenException("not implemented");
  }

  DictionaryIndex getOrUpdate(bool value) {
    throw GlutenException("not implemented");
  }
};

template <>
class VeloxDictionaryStorage<facebook::velox::StringView> : public DictionaryStorage {
 public:
  VeloxDictionaryStorage(facebook::velox::memory::MemoryPool* pool)
      : lengths_(0, facebook::velox::memory::StlAllocator<StringLengthType>(*pool)),
        values_(0, facebook::velox::memory::StlAllocator<char>(*pool)) {}

  DictionaryIndex getOrUpdate(const std::string_view& view) {
    auto it = indexMap_.find(view);
    if (it == indexMap_.end()) {
      const auto length = static_cast<StringLengthType>(view.size());
      lengths_.emplace_back(length);

      const auto offset = values_.size();
      values_.insert(values_.end(), view.data(), view.data() + length);

      auto value = std::string_view{values_.data() + offset, length};
      const auto& [newIt, _] = indexMap_.emplace(value, indexMap_.size());
      it = newIt;
    }
    return it->second;
  }

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    const size_t lengthBufferSize = lengths_.size() * sizeof(StringLengthType);
    RETURN_NOT_OK(out->Write(&lengthBufferSize, sizeof(lengthBufferSize)));
    RETURN_NOT_OK(out->Write(lengths_.data(), lengthBufferSize));

    const size_t valueBufferSize = values_.size() * sizeof(char);
    RETURN_NOT_OK(out->Write(&valueBufferSize, sizeof(valueBufferSize)));
    RETURN_NOT_OK(out->Write(values_.data(), valueBufferSize));

    return arrow::Status::OK();
  }

 private:
  std::vector<StringLengthType, facebook::velox::memory::StlAllocator<StringLengthType>> lengths_;
  std::vector<char, facebook::velox::memory::StlAllocator<char>> values_;
  std::unordered_map<std::string_view, DictionaryIndex> indexMap_;
};

namespace {
template <typename T>
arrow::Result<std::shared_ptr<arrow::Buffer>> updateDictionary(
    int32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    size_t& bufferIdx,
    const std::shared_ptr<VeloxDictionaryStorage<T>>& dictionary,
    arrow::MemoryPool* pool) {
  const auto& validityBuffer = buffers[bufferIdx++];
  const auto& valueBuffer = buffers[bufferIdx++];

  ARROW_ASSIGN_OR_RAISE(auto indices, arrow::AllocateBuffer(sizeof(DictionaryIndex) * numRows, pool));
  auto rawIndices = indices->mutable_data_as<DictionaryIndex>();

  const auto* values = valueBuffer->data_as<T>();

  if (validityBuffer != nullptr) {
    const auto* nulls = validityBuffer->data();
    for (auto i = 0; i < numRows; ++i) {
      if (arrow::bit_util::GetBit(nulls, i)) {
        *rawIndices = dictionary->getOrUpdate(values[i]);
      }
      ++rawIndices;
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      *rawIndices++ = dictionary->getOrUpdate(values[i]);
    }
  }

  return indices;
}

template <>
arrow::Result<std::shared_ptr<arrow::Buffer>> updateDictionary<facebook::velox::StringView>(
    int32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    size_t& bufferIdx,
    const std::shared_ptr<VeloxDictionaryStorage<facebook::velox::StringView>>& dictionary,
    arrow::MemoryPool* pool) {
  const auto& validityBuffer = buffers[bufferIdx++];
  const auto& lengthBuffer = buffers[bufferIdx++];
  const auto& valueBuffer = buffers[bufferIdx++];

  ARROW_ASSIGN_OR_RAISE(auto indices, arrow::AllocateBuffer(sizeof(DictionaryIndex) * numRows, pool));
  auto rawIndices = indices->mutable_data_as<DictionaryIndex>();

  const auto* lengths = lengthBuffer->data_as<StringLengthType>();
  const auto* values = valueBuffer->data_as<char>();

  size_t offset = 0;

  if (validityBuffer != nullptr) {
    const auto* nulls = validityBuffer->data();
    for (auto i = 0; i < numRows; ++i) {
      if (arrow::bit_util::GetBit(nulls, i)) {
        std::string_view view(values + offset, lengths[i]);
        offset += lengths[i];
        *rawIndices = dictionary->getOrUpdate(view);
      }
      ++rawIndices;
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      std::string_view view(values + offset, lengths[i]);
      offset += lengths[i];
      *rawIndices++ = dictionary->getOrUpdate(view);
    }
  }

  return indices;
}
} // namespace

class DictionaryUpdater {
 public:
  DictionaryUpdater(facebook::velox::memory::MemoryPool* veloxPool, arrow::MemoryPool* arrowPool)
      : veloxPool_(veloxPool), arrowPool_(arrowPool) {}

  template <typename ValueType>
  std::shared_ptr<VeloxDictionaryStorage<ValueType>> getOrCreateDict(int32_t fieldIdx) {
    const auto it = dictionaries_.find(fieldIdx);
    if (it == dictionaries_.end()) {
      auto dict = std::make_shared<VeloxDictionaryStorage<ValueType>>(veloxPool_);
      dictionaries_[fieldIdx] = dict;
      return dict;
    }
    return std::dynamic_pointer_cast<VeloxDictionaryStorage<ValueType>>(it->second);
  }

  template <facebook::velox::TypeKind Kind>
  void toDictionaryBuffers(
      int32_t fieldIdx,
      int32_t numRows,
      const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
      size_t& bufferIdx,
      std::vector<std::shared_ptr<arrow::Buffer>>& results) {
    using ValueType = typename facebook::velox::TypeTraits<Kind>::NativeType;

    const auto& nulls = buffers[bufferIdx];
    const auto& dict = getOrCreateDict<ValueType>(fieldIdx);

    GLUTEN_ASSIGN_OR_THROW(auto indices, updateDictionary<ValueType>(numRows, buffers, bufferIdx, dict, arrowPool_));

    results.push_back(nulls);
    results.push_back(indices);
  }

  const std::unordered_map<int32_t, std::shared_ptr<DictionaryStorage>>& snapshot() {
    return dictionaries_;
  }

 private:
  facebook::velox::memory::MemoryPool* veloxPool_;
  arrow::MemoryPool* arrowPool_;
  std::unordered_map<int32_t, std::shared_ptr<DictionaryStorage>> dictionaries_;
};

VeloxShuffleDictionaryWriter::VeloxShuffleDictionaryWriter(
    facebook::velox::memory::MemoryPool* veloxPool,
    arrow::MemoryPool* arrowPool)
    : dictionaryUpdater_(std::make_shared<DictionaryUpdater>(veloxPool, arrowPool)) {}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> VeloxShuffleDictionaryWriter::updateAndGet(
    const std::shared_ptr<arrow::Schema>& schema,
    int32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  RETURN_NOT_OK(initSchema(schema));

  std::vector<std::shared_ptr<arrow::Buffer>> results;

  size_t bufferIdx = 0;
  for (auto i = 0; i < rowType_->size(); ++i) {
    switch (fieldTypes_[i]) {
      case FieldType::kNull:
      case FieldType::kComplex:
        break;
      case FieldType::kFixedWidth:
        results.emplace_back(buffers[bufferIdx++]);
        results.emplace_back(buffers[bufferIdx++]);
        break;
      case FieldType::kSupportsDictionary: {
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            dictionaryUpdater_->toDictionaryBuffers,
            rowType_->childAt(i)->kind(),
            i,
            numRows,
            buffers,
            bufferIdx,
            results);
        break;
      }
    }
  }

  if (hasComplexType_) {
    results.emplace_back(buffers[bufferIdx++]);
  }

  GLUTEN_DCHECK(bufferIdx == buffers.size(), "Not all buffers are consumed.");

  return results;
}

arrow::Status VeloxShuffleDictionaryWriter::serialize(arrow::io::OutputStream* out) {
  const auto& dictionaries = dictionaryUpdater_->snapshot();

  auto bitMapSize = arrow::bit_util::RoundUpToMultipleOf8(rowType_->size());
  std::vector<uint8_t> bitMap(bitMapSize);

  for (auto fieldIdx : dictionaryFields_) {
    arrow::bit_util::SetBit(bitMap.data(), fieldIdx);
  }

  RETURN_NOT_OK(out->Write(bitMap.data(), bitMapSize));

  for (auto fieldIdx : dictionaryFields_) {
    GLUTEN_DCHECK(
        dictionaries.find(fieldIdx) != dictionaries.end(),
        "Invalid dictionary field index: " + std::to_string(fieldIdx));

    const auto& dictionary = dictionaries.at(fieldIdx);
    RETURN_NOT_OK(dictionary->serialize(out));
  }

  return arrow::Status::OK();
}

arrow::Status VeloxShuffleDictionaryWriter::initSchema(const std::shared_ptr<arrow::Schema>& schema) {
  if (rowType_ == nullptr) {
    rowType_ = facebook::velox::asRowType(fromArrowSchema(schema));
    fieldTypes_.resize(rowType_->size());
    for (auto i = 0; i < rowType_->size(); ++i) {
      switch (rowType_->childAt(i)->kind()) {
        case facebook::velox::TypeKind::UNKNOWN:
          fieldTypes_[i] = FieldType::kNull;
          break;
        case facebook::velox::TypeKind::BOOLEAN:
        case facebook::velox::TypeKind::HUGEINT:
        case facebook::velox::TypeKind::TIMESTAMP:
          // TODO: support HUGEINT and TIMESTAMP
          fieldTypes_[i] = FieldType::kFixedWidth;
          break;
        case facebook::velox::TypeKind::ARRAY:
        case facebook::velox::TypeKind::MAP:
        case facebook::velox::TypeKind::ROW:
          fieldTypes_[i] = FieldType::kComplex;
          hasComplexType_ = true;
          break;
        default:
          fieldTypes_[i] = FieldType::kSupportsDictionary;
          dictionaryFields_.push_back(i);
          break;
      }
    }
  }
  return arrow::Status::OK();
}

} // namespace gluten