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

namespace {

arrow::Status writeDictionaryBuffer(
    const uint8_t* data,
    size_t size,
    facebook::velox::memory::MemoryPool* pool,
    arrow::util::Codec* codec,
    arrow::io::OutputStream* out) {
  ARROW_RETURN_NOT_OK(out->Write(&size, sizeof(size_t)));

  if (size == 0) {
    return arrow::Status::OK();
  }

  GLUTEN_DCHECK(data != nullptr, "Cannot write null data");

  if (codec != nullptr) {
    size_t compressedLength = codec->MaxCompressedLen(size, data);
    auto buffer = facebook::velox::AlignedBuffer::allocate<uint8_t>(compressedLength, pool, std::nullopt, true);
    ARROW_ASSIGN_OR_RAISE(
        compressedLength, codec->Compress(size, data, compressedLength, buffer->asMutable<uint8_t>()));

    ARROW_RETURN_NOT_OK(out->Write(&compressedLength, sizeof(size_t)));
    ARROW_RETURN_NOT_OK(out->Write(buffer->as<void>(), compressedLength));
  } else {
    ARROW_RETURN_NOT_OK(out->Write(data, size));
  }

  return arrow::Status::OK();
}

} // namespace

template <typename ValueType>
class VeloxDictionaryStorage : public ShuffleDictionaryStorage {
 public:
  VeloxDictionaryStorage(facebook::velox::memory::MemoryPool* pool, arrow::util::Codec* codec)
      : pool_(pool), codec_(codec), values_(0, facebook::velox::memory::StlAllocator<ValueType>(*pool)) {}

  DictionaryIndex getOrUpdate(const ValueType& value) {
    const auto& [it, inserted] = indexMap_.emplace(value, indexMap_.size());
    if (inserted) {
      values_.push_back(value);
    }
    return it->second;
  }

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    ARROW_RETURN_NOT_OK(writeDictionaryBuffer(
        reinterpret_cast<const uint8_t*>(values_.data()), values_.size() * sizeof(ValueType), pool_, codec_, out));
    return arrow::Status::OK();
  }

 private:
  facebook::velox::memory::MemoryPool* pool_;
  arrow::util::Codec* codec_;
  std::vector<ValueType, facebook::velox::memory::StlAllocator<ValueType>> values_;

  std::unordered_map<ValueType, DictionaryIndex> indexMap_;
};

template <>
class VeloxDictionaryStorage<bool> : public ShuffleDictionaryStorage {
 public:
  VeloxDictionaryStorage(facebook::velox::memory::MemoryPool* pool, arrow::util::Codec* codec) {}

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    throw GlutenException("not implemented");
  }

  DictionaryIndex getOrUpdate(bool value) {
    throw GlutenException("not implemented");
  }
};

template <>
class VeloxDictionaryStorage<facebook::velox::StringView> : public ShuffleDictionaryStorage {
  struct Strings {
    std::variant<size_t, const char*> offsetOrData;
    size_t length;
  };

  struct StringsResolver {
    const char* data(const Strings& s) const {
      return std::visit(
          [&](auto&& arg) -> const char* {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, size_t>) {
              return buffer.data() + arg;
            } else {
              return arg;
            }
          },
          s.offsetOrData);
    }

    const std::vector<char, facebook::velox::memory::StlAllocator<char>>& buffer;
  };

  struct StringsHash {
    explicit StringsHash(const std::vector<char, facebook::velox::memory::StlAllocator<char>>& buf) : resolver_{buf} {}

    size_t operator()(const Strings& s) const {
      return std::hash<std::string_view>{}(std::string_view(resolver_.data(s), s.length));
    }

   private:
    StringsResolver resolver_;
  };

  struct StringsEqual {
    explicit StringsEqual(const std::vector<char, facebook::velox::memory::StlAllocator<char>>& buf) : resolver_{buf} {}

    bool operator()(const Strings& a, const Strings& b) const {
      return a.length == b.length && std::memcmp(resolver_.data(a), resolver_.data(b), a.length) == 0;
    }

   private:
    StringsResolver resolver_;
  };

 public:
  VeloxDictionaryStorage(facebook::velox::memory::MemoryPool* pool, arrow::util::Codec* codec)
      : pool_(pool),
        codec_(codec),
        lengths_(0, facebook::velox::memory::StlAllocator<StringLengthType>(*pool)),
        values_(0, facebook::velox::memory::StlAllocator<char>(*pool)),
        indexMap_(
            0,
            StringsHash{values_},
            StringsEqual{values_},
            facebook::velox::memory::StlAllocator<std::pair<const Strings, DictionaryIndex>>(*pool)) {}

  DictionaryIndex getOrUpdate(const std::string_view& view) {
    auto it = indexMap_.find(Strings{view.data(), view.size()});
    if (it == indexMap_.end()) {
      const auto length = static_cast<StringLengthType>(view.size());
      lengths_.emplace_back(length);

      const size_t offset = values_.size();
      if (values_.capacity() < values_.size() + length) {
        values_.reserve(std::max(values_.capacity() * 2, values_.size() + length));
      }
      values_.insert(values_.end(), view.data(), view.data() + length);

      const auto& [newIt, _] = indexMap_.emplace(Strings{offset, length}, indexMap_.size());
      it = newIt;
    }
    return it->second;
  }

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    ARROW_RETURN_NOT_OK(writeDictionaryBuffer(
        reinterpret_cast<const uint8_t*>(lengths_.data()),
        lengths_.size() * sizeof(StringLengthType),
        pool_,
        codec_,
        out));

    ARROW_RETURN_NOT_OK(
        writeDictionaryBuffer(reinterpret_cast<const uint8_t*>(values_.data()), values_.size(), pool_, codec_, out));

    return arrow::Status::OK();
  }

 private:
  facebook::velox::memory::MemoryPool* pool_;
  arrow::util::Codec* codec_;
  std::vector<StringLengthType, facebook::velox::memory::StlAllocator<StringLengthType>> lengths_;
  std::vector<char, facebook::velox::memory::StlAllocator<char>> values_;
  std::unordered_map<
      Strings,
      DictionaryIndex,
      StringsHash,
      StringsEqual,
      facebook::velox::memory::StlAllocator<std::pair<const Strings, DictionaryIndex>>>
      indexMap_;
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
  DictionaryUpdater(
      facebook::velox::memory::MemoryPool* veloxPool,
      arrow::MemoryPool* arrowPool,
      arrow::util::Codec* codec)
      : veloxPool_(veloxPool), arrowPool_(arrowPool), codec_(codec) {}

  template <typename ValueType>
  std::shared_ptr<VeloxDictionaryStorage<ValueType>> getOrCreateDict(int32_t fieldIdx) {
    const auto it = dictionaries_.find(fieldIdx);
    if (it == dictionaries_.end()) {
      auto dict = std::make_shared<VeloxDictionaryStorage<ValueType>>(veloxPool_, codec_);
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

  const std::unordered_map<int32_t, std::shared_ptr<ShuffleDictionaryStorage>>& snapshot() {
    return dictionaries_;
  }

 private:
  facebook::velox::memory::MemoryPool* veloxPool_;
  arrow::MemoryPool* arrowPool_;
  arrow::util::Codec* codec_;

  std::unordered_map<int32_t, std::shared_ptr<ShuffleDictionaryStorage>> dictionaries_;
};

VeloxShuffleDictionaryWriter::VeloxShuffleDictionaryWriter(
    facebook::velox::memory::MemoryPool* veloxPool,
    arrow::MemoryPool* arrowPool,
    arrow::util::Codec* codec)
    : dictionaryUpdater_(std::make_shared<DictionaryUpdater>(veloxPool, arrowPool, codec)) {}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> VeloxShuffleDictionaryWriter::updateAndGet(
    const std::shared_ptr<arrow::Schema>& schema,
    int32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  ARROW_RETURN_NOT_OK(initSchema(schema));

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

  ARROW_RETURN_NOT_OK(out->Write(bitMap.data(), bitMapSize));

  for (auto fieldIdx : dictionaryFields_) {
    GLUTEN_DCHECK(
        dictionaries.find(fieldIdx) != dictionaries.end(),
        "Invalid dictionary field index: " + std::to_string(fieldIdx));

    const auto& dictionary = dictionaries.at(fieldIdx);
    ARROW_RETURN_NOT_OK(dictionary->serialize(out));
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