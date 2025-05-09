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

#include "shuffle/ArrowShuffleDictionaryWriter.h"
#include "shuffle/Utils.h"

#include <arrow/array/builder_dict.h>

namespace gluten {

using ArrowDictionaryIndexType = int32_t;

template <typename T>
using is_dictionary_binary_type =
    std::integral_constant<bool, std::is_same_v<arrow::BinaryType, T> || std::is_same_v<arrow::StringType, T>>;

template <typename T, typename R = void>
using enable_if_dictionary_binary = std::enable_if_t<is_dictionary_binary_type<T>::value, R>;

template <typename T>
using is_dictionary_primitive_type = std::integral_constant<
    bool,
    (arrow::is_primitive_ctype<T>::value && !std::is_same_v<arrow::BooleanType, T>) || arrow::is_date_type<T>::value>;

template <typename T, typename R = void>
using enable_if_dictionary_primitive = std::enable_if_t<is_dictionary_primitive_type<T>::value, R>;

namespace {

arrow::Status writeDictionaryBuffer(
    const uint8_t* data,
    size_t size,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    arrow::io::OutputStream* out) {
  ARROW_RETURN_NOT_OK(out->Write(&size, sizeof(size_t)));

  if (size == 0) {
    return arrow::Status::OK();
  }

  GLUTEN_DCHECK(data != nullptr, "Cannot write null data");

  if (codec != nullptr) {
    size_t compressedLength = codec->MaxCompressedLen(size, data);
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(compressedLength, pool));
    ARROW_ASSIGN_OR_RAISE(
        compressedLength, codec->Compress(size, data, compressedLength, buffer->mutable_data_as<uint8_t>()));

    ARROW_RETURN_NOT_OK(out->Write(&compressedLength, sizeof(size_t)));
    ARROW_RETURN_NOT_OK(out->Write(buffer->data_as<void>(), compressedLength));
  } else {
    ARROW_RETURN_NOT_OK(out->Write(data, size));
  }

  return arrow::Status::OK();
}

} // namespace

template <typename ValueType>
class DictionaryStorageImpl : public ShuffleDictionaryStorage {
 public:
  DictionaryStorageImpl(arrow::MemoryPool* pool, arrow::util::Codec* codec) : pool_(pool), codec_(codec) {
    values_ = arrow::BufferBuilder{pool};
  }

  arrow::Result<ArrowDictionaryIndexType> getOrUpdate(const ValueType& value) {
    const auto& [it, inserted] = indexMap.emplace(value, indexMap.size());
    if (inserted) {
      RETURN_NOT_OK(values_.Append(&value, sizeof(ValueType)));
    }
    return it->second;
  }

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    ARROW_ASSIGN_OR_RAISE(auto valueBuffer, values_.Finish());
    ARROW_RETURN_NOT_OK(writeDictionaryBuffer(valueBuffer->data(), valueBuffer->size(), pool_, codec_, out));

    return arrow::Status::OK();
  }

 private:
  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;

  arrow::BufferBuilder values_;
  std::unordered_map<ValueType, ArrowDictionaryIndexType> indexMap;
};

template <>
class DictionaryStorageImpl<std::string_view> : public ShuffleDictionaryStorage {
 public:
  DictionaryStorageImpl(arrow::MemoryPool* pool, arrow::util::Codec* codec) : pool_(pool), codec_(codec) {
    table_ = std::make_shared<arrow::internal::DictionaryMemoTable>(pool, std::make_shared<arrow::LargeBinaryType>());
  }

  arrow::Result<ArrowDictionaryIndexType> getOrUpdate(const std::string_view& view) const {
    ArrowDictionaryIndexType memoIndex;
    ARROW_RETURN_NOT_OK(table_->GetOrInsert<arrow::LargeBinaryType>(view, &memoIndex));
    return memoIndex;
  }

  arrow::Status serialize(arrow::io::OutputStream* out) override {
    std::shared_ptr<arrow::ArrayData> data;
    ARROW_RETURN_NOT_OK(table_->GetArrayData(0, &data));

    ARROW_RETURN_IF(data->buffers.size() != 3, arrow::Status::Invalid("Invalid dictionary"));

    ARROW_ASSIGN_OR_RAISE(auto lengths, offsetToLength(data->length, data->buffers[1]));
    ARROW_RETURN_NOT_OK(writeDictionaryBuffer(lengths->data(), lengths->size(), pool_, codec_, out));

    const auto& values = data->buffers[2];
    ARROW_RETURN_NOT_OK(writeDictionaryBuffer(values->data(), values->size(), pool_, codec_, out));

    return arrow::Status::OK();
  }

 private:
  arrow::Result<std::shared_ptr<arrow::Buffer>> offsetToLength(
      size_t numElements,
      const std::shared_ptr<arrow::Buffer>& offsets) const {
    ARROW_ASSIGN_OR_RAISE(auto lengths, arrow::AllocateBuffer(sizeof(StringLengthType) * numElements, pool_));
    auto* rawLengths = lengths->mutable_data_as<StringLengthType>();
    const auto* rawOffsets = offsets->data_as<int64_t>();

    for (auto i = 0; i < numElements; ++i) {
      rawLengths[i] = static_cast<StringLengthType>(rawOffsets[i + 1] - rawOffsets[i]);
    }

    return lengths;
  }

  arrow::MemoryPool* pool_;
  arrow::util::Codec* codec_;

  std::shared_ptr<arrow::internal::DictionaryMemoTable> table_;
};

namespace {
template <typename T>
arrow::Result<std::shared_ptr<arrow::Buffer>> updateDictionary(
    int32_t numRows,
    const std::shared_ptr<arrow::Buffer>& validityBuffer,
    const std::shared_ptr<arrow::Buffer>& valueBuffer,
    const std::shared_ptr<DictionaryStorageImpl<T>>& dictionary,
    arrow::MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto indices, arrow::AllocateBuffer(sizeof(ArrowDictionaryIndexType) * numRows, pool));
  auto rawIndices = indices->mutable_data_as<ArrowDictionaryIndexType>();

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
    const std::shared_ptr<DictionaryStorageImpl<std::string_view>>& dictionary,
    arrow::MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto indices, arrow::AllocateBuffer(sizeof(ArrowDictionaryIndexType) * numRows, pool));
  auto rawIndices = indices->mutable_data_as<ArrowDictionaryIndexType>();

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

class ValueUpdater {
 public:
  template <typename ValueType>
  std::shared_ptr<DictionaryStorageImpl<ValueType>> getOrCreateDict(int32_t fieldIdx) {
    auto it = writer->dictionaries_.find(fieldIdx);
    if (it == writer->dictionaries_.end()) {
      auto dict = std::make_shared<DictionaryStorageImpl<ValueType>>(writer->pool_, writer->codec_);
      writer->dictionaries_[fieldIdx] = dict;
      return dict;
    }
    return std::dynamic_pointer_cast<DictionaryStorageImpl<ValueType>>(it->second);
  }

  template <typename ArrowType>
  enable_if_dictionary_primitive<ArrowType, arrow::Status> Visit(const ArrowType&) {
    using ValueType = typename ArrowType::c_type;

    const auto& dict = getOrCreateDict<ValueType>(fieldIdx);

    ARROW_ASSIGN_OR_RAISE(auto indices, updateDictionary<ValueType>(numRows, nulls, values, dict, writer->pool_));

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
        auto indices, updateDictionaryForBinary(numRows, nulls, values, binaryValues, dict, writer->pool_));

    results.push_back(nulls);
    results.push_back(indices);

    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampType&) {
    const auto& dict = getOrCreateDict<__int128_t>(fieldIdx);

    ARROW_ASSIGN_OR_RAISE(auto indices, updateDictionary(numRows, nulls, values, dict, writer->pool_));

    results.push_back(nulls);
    results.push_back(indices);

    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DataType& type) {
    return arrow::Status::TypeError("Not implemented for type: ", type.ToString());
  }

  ArrowShuffleDictionaryWriter* writer;
  int32_t fieldIdx;
  int32_t numRows;
  std::shared_ptr<arrow::Buffer> nulls{nullptr};
  std::shared_ptr<arrow::Buffer> values{nullptr};
  std::shared_ptr<arrow::Buffer> binaryValues{nullptr};

  std::vector<std::shared_ptr<arrow::Buffer>>& results;
};

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> ArrowShuffleDictionaryWriter::updateAndGet(
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

arrow::Status ArrowShuffleDictionaryWriter::serialize(arrow::io::OutputStream* out) {
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

arrow::Status ArrowShuffleDictionaryWriter::initSchema(const std::shared_ptr<arrow::Schema>& schema) {
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

} // namespace gluten
