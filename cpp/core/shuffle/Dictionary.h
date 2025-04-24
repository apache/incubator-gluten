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

#include <arrow/buffer.h>
#include <arrow/buffer_builder.h>
#include <arrow/stl_allocator.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/visit_type_inline.h>

#include "utils/Exception.h"

#include <arrow/array/array_decimal.h>

namespace {

using IndexType = int32_t;

template <typename T>
using is_dictionary_binary_type =
    std::integral_constant<bool, std::is_same_v<arrow::BinaryType, T> || std::is_same_v<arrow::StringType, T>>;

template <typename T, typename R = void>
using enable_if_dictionary_binary = std::enable_if_t<is_dictionary_binary_type<T>::value, R>;

template <typename T>
using is_dictionary_fixed_width_type = std::integral_constant<
    bool,
    (arrow::is_primitive_ctype<T>::value && !std::is_same_v<arrow::BooleanType, T>) || arrow::is_date_type<T>::value ||
        arrow::is_timestamp_type<T>::value || arrow::is_time_type<T>::value>;

template <typename T, typename R = void>
using enable_if_dictionary_fixed_width = std::enable_if_t<is_dictionary_fixed_width_type<T>::value, R>;

constexpr uint8_t kPlainPayload = 1;
constexpr uint8_t kDictionary = 2;
constexpr uint8_t kDictionaryPayload = 3;

class IDictionaryStorage {
 public:
  virtual ~IDictionaryStorage() = default;

  virtual arrow::Status serialize(arrow::io::OutputStream* out) = 0;
};

template <typename ValueType>
class DictionaryStorage : public IDictionaryStorage {
 public:
  DictionaryStorage(arrow::MemoryPool* pool) {
    values_ = arrow::BufferBuilder{pool};
  }

  arrow::Result<IndexType> getOrUpdate(const ValueType& value) {
    const auto& [it, inserted] = indexMap.emplace(value, indexMap.size());
    if (inserted) {
      RETURN_NOT_OK(values_.Append(&value, sizeof(ValueType)));
    }
    return it->second;
  }

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    ARROW_ASSIGN_OR_RAISE(auto valueBuffer, values_.Finish());

    const auto valueBufferSize = valueBuffer->size();
    RETURN_NOT_OK(out->Write(&valueBufferSize, sizeof(valueBufferSize)));
    RETURN_NOT_OK(out->Write(valueBuffer->data(), valueBufferSize));

    return arrow::Status::OK();
  }

 private:
  arrow::BufferBuilder values_;
  std::unordered_map<ValueType, IndexType> indexMap;
};

template <>
class DictionaryStorage<std::string_view> : public IDictionaryStorage {
 public:
  DictionaryStorage(arrow::MemoryPool* pool) {
    values_ = arrow::BufferBuilder{pool};
    lengths_ = arrow::BufferBuilder{pool};
  }

  arrow::Result<IndexType> getOrUpdate(const std::string_view& view) {
    auto it = indexMap_.find(view);
    if (it == indexMap_.end()) {
      const auto length = view.size();
      RETURN_NOT_OK(lengths_.Append(&length, sizeof(length)));

      const auto offset = values_.length();
      RETURN_NOT_OK(values_.Append(view));

      auto value = std::string_view{values_.data_as<char>() + offset, length};
      const auto& [newIt, _] = indexMap_.emplace(value, indexMap_.size());
      it = newIt;
    }
    return it->second;
  }

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    ARROW_ASSIGN_OR_RAISE(auto lengthBuffer, lengths_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto valueBuffer, values_.Finish());

    const auto lengthBufferSize = lengthBuffer->size();
    RETURN_NOT_OK(out->Write(&lengthBufferSize, sizeof(lengthBufferSize)));
    RETURN_NOT_OK(out->Write(lengthBuffer->data(), lengthBufferSize));

    const auto valueBufferSize = valueBuffer->size();
    RETURN_NOT_OK(out->Write(&valueBufferSize, sizeof(valueBufferSize)));
    RETURN_NOT_OK(out->Write(valueBuffer->data(), valueBufferSize));

    return arrow::Status::OK();
  }

 private:
  arrow::BufferBuilder lengths_;
  arrow::BufferBuilder values_;
  std::unordered_map<std::string_view, IndexType> indexMap_;
};

template <typename T>
arrow::Result<std::shared_ptr<arrow::Buffer>> updateDictionary(
    int32_t numRows,
    const std::shared_ptr<arrow::Buffer>& validityBuffer,
    const std::shared_ptr<arrow::Buffer>& valueBuffer,
    const std::shared_ptr<DictionaryStorage<T>>& dictionary,
    arrow::MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto indices, arrow::AllocateBuffer(sizeof(IndexType) * numRows, pool));
  auto rawIndices = indices->mutable_data_as<IndexType>();

  const auto* values = valueBuffer->data_as<T>();

  if (validityBuffer != nullptr) {
    const auto* nulls = validityBuffer->data();
    for (auto i = 0; i < numRows; ++i) {
      if (arrow::bit_util::GetBit(nulls, i)) {
        ARROW_ASSIGN_OR_RAISE(*rawIndices, dictionary->getOrUpdate(values[i]));
      }
      ++rawIndices;
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      ARROW_ASSIGN_OR_RAISE(*rawIndices++, dictionary->getOrUpdate(values[i]));
    }
  }

  return indices;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> updateDictionaryForBinary(
    int32_t numRows,
    const std::shared_ptr<arrow::Buffer>& validityBuffer,
    const std::shared_ptr<arrow::Buffer>& lengthBuffer,
    const std::shared_ptr<arrow::Buffer>& valueBuffer,
    const std::shared_ptr<DictionaryStorage<std::string_view>>& dictionary,
    arrow::MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto indices, arrow::AllocateBuffer(sizeof(IndexType) * numRows, pool));
  auto rawIndices = indices->mutable_data_as<IndexType>();

  const auto* lengths = lengthBuffer->data_as<uint32_t>();
  const auto* values = valueBuffer->data_as<char>();

  size_t offset = 0;

  if (validityBuffer != nullptr) {
    const auto* nulls = validityBuffer->data();
    for (auto i = 0; i < numRows; ++i) {
      if (arrow::bit_util::GetBit(nulls, i)) {
        std::string_view view(values + offset, lengths[i]);
        offset += lengths[i];
        ARROW_ASSIGN_OR_RAISE(*rawIndices, dictionary->getOrUpdate(view));
      }
      ++rawIndices;
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      std::string_view view(values + offset, lengths[i]);
      offset += lengths[i];
      ARROW_ASSIGN_OR_RAISE(*rawIndices++, dictionary->getOrUpdate(view));
    }
  }

  return indices;
}

} // namespace

class DictionaryWriter {
 public:
  DictionaryWriter(arrow::MemoryPool* pool) : pool_(pool) {}

  arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> updateAndGet(
      const std::shared_ptr<arrow::Schema>& schema,
      int32_t numRows,
      const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
    RETURN_NOT_OK(initSchema(schema));

    std::vector<std::shared_ptr<arrow::Buffer>> results;

    size_t bufferIdx = 0;
    for (auto i = 0; i < schema->num_fields(); ++i) {
      switch (fieldTypes_[i]) {
        case FieldType::kNull:
        case FieldType::kComplex:
          break;
        case FieldType::kFixedWidth:
          results.emplace_back(buffers[bufferIdx++]);
          results.emplace_back(buffers[bufferIdx++]);
          break;
        case FieldType::kSupportsDictionary: {
          const auto fieldType = schema_->field(i)->type();
          bool isBinaryType =
              fieldType->id() == arrow::BinaryType::type_id || fieldType->id() == arrow::StringType::type_id;

          ValueUpdater valueUpdater{
              this,
              i,
              numRows,
              buffers[bufferIdx++],
              buffers[bufferIdx++],
              isBinaryType ? buffers[bufferIdx++] : nullptr,
              results};

          RETURN_NOT_OK(arrow::VisitTypeInline(*fieldType, &valueUpdater));
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

  arrow::Status serialize(arrow::io::OutputStream* out) {
    auto bitMapSize = arrow::bit_util::RoundUpToMultipleOf8(schema_->num_fields());
    std::vector<uint8_t> bitMap(bitMapSize);

    for (auto fieldIdx : dictionaryFields_) {
      arrow::bit_util::SetBit(bitMap.data(), fieldIdx);
    }

    RETURN_NOT_OK(out->Write(bitMap.data(), bitMapSize));

    for (auto fieldIdx : dictionaryFields_) {
      GLUTEN_DCHECK(
          dictionaries_.find(fieldIdx) != dictionaries_.end(),
          "Invalid dictionary field index: " + std::to_string(fieldIdx));

      const auto& dictionary = dictionaries_[fieldIdx];
      RETURN_NOT_OK(dictionary->serialize(out));
    }

    return arrow::Status::OK();
  }

 private:
  enum class FieldType { kNull, kFixedWidth, kComplex, kSupportsDictionary };

  arrow::Status initSchema(const std::shared_ptr<arrow::Schema>& schema) {
    if (!schema_) {
      schema_ = schema;
      fieldTypes_.resize(schema_->num_fields());

      for (auto i = 0; i < schema_->num_fields(); ++i) {
        switch (schema_->field(i)->type()->id()) {
          case arrow::NullType::type_id:
            fieldTypes_[i] = FieldType::kNull;
            break;
          case arrow::BooleanType::type_id:
          case arrow::Decimal128Type::type_id:
            fieldTypes_[i] = FieldType::kFixedWidth;
            break;
          case arrow::ListType::type_id:
          case arrow::MapType::type_id:
          case arrow::StructType::type_id:
            fieldTypes_[i] = FieldType::kComplex;
            hasComplexType_ = true;
            break;
          default:
            fieldTypes_[i] = FieldType::kSupportsDictionary;
            dictionaryFields_.push_back(i);
            break;
        }
      }
    } else if (schema_ != schema) {
      return arrow::Status::Invalid("Schema mismatch");
    }
    return arrow::Status::OK();
  }

  arrow::MemoryPool* pool_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<FieldType> fieldTypes_;
  std::vector<int32_t> dictionaryFields_;
  bool hasComplexType_{false};
  std::unordered_map<int32_t, std::shared_ptr<IDictionaryStorage>> dictionaries_;

  struct ValueUpdater {
    DictionaryWriter* self;
    int32_t fieldIdx;
    int32_t numRows;
    std::shared_ptr<arrow::Buffer> nulls{nullptr};
    std::shared_ptr<arrow::Buffer> values{nullptr};
    std::shared_ptr<arrow::Buffer> binaryValues{nullptr};

    std::vector<std::shared_ptr<arrow::Buffer>>& results;

    template <typename ValueType>
    std::shared_ptr<DictionaryStorage<ValueType>> getOrCreateDict(int32_t fieldIdx) {
      auto it = self->dictionaries_.find(fieldIdx);
      if (it == self->dictionaries_.end()) {
        auto dict = std::make_shared<DictionaryStorage<ValueType>>(self->pool_);
        self->dictionaries_[fieldIdx] = dict;
        return dict;
      }
      return std::dynamic_pointer_cast<DictionaryStorage<ValueType>>(it->second);
    }

    template <typename ArrowType>
    enable_if_dictionary_fixed_width<ArrowType, arrow::Status> Visit(const ArrowType&) {
      using ValueType = typename ArrowType::c_type;

      const auto& dict = getOrCreateDict<ValueType>(fieldIdx);

      ARROW_ASSIGN_OR_RAISE(auto indices, updateDictionary<ValueType>(numRows, nulls, values, dict, self->pool_));

      results.push_back(nulls);
      results.push_back(indices);

      return arrow::Status::OK();
    }

    template <typename ArrowType>
    enable_if_dictionary_binary<ArrowType, arrow::Status> Visit(const ArrowType&) {
      using OffsetCType = typename arrow::TypeTraits<ArrowType>::OffsetType::c_type;
      static_assert(std::is_same_v<OffsetCType, int32_t>);

      const auto& dict = getOrCreateDict<std::string_view>(fieldIdx);

      ARROW_ASSIGN_OR_RAISE(
          auto indices, updateDictionaryForBinary(numRows, nulls, values, binaryValues, dict, self->pool_));

      results.push_back(nulls);
      results.push_back(indices);

      return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::DataType& type) {
      return arrow::Status::TypeError("Not implemented for type: ", type.ToString());
    }
  };
};
