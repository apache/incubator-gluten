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

#include "shuffle/BlockPayload.h"

namespace {
static const gluten::BlockPayload::Type kCompressedType = gluten::BlockPayload::kCompressed;
}
namespace gluten {

arrow::Result<std::unique_ptr<BlockPayload>> BlockPayload::fromBuffers(
    BlockPayload::Type payloadType,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    bool reuseBuffers) {
  if (payloadType == BlockPayload::Type::kCompressed) {
    // Compress.
    // Compressed buffer layout: | buffer1 compressedLength | buffer1 uncompressedLength | buffer1 | ...
    auto metadataLength = sizeof(int64_t) * 2 * buffers.size();
    int64_t totalCompressedLength =
        std::accumulate(buffers.begin(), buffers.end(), 0LL, [&](auto sum, const auto& buffer) {
          if (!buffer) {
            return sum;
          }
          return sum + codec->MaxCompressedLen(buffer->size(), buffer->data());
        });
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::ResizableBuffer> compressed,
        arrow::AllocateResizableBuffer(metadataLength + totalCompressedLength, pool));
    auto output = compressed->mutable_data();

    // Compress buffers one by one.
    for (auto& buffer : buffers) {
      auto availableLength = compressed->size() - (output - compressed->data());
      RETURN_NOT_OK(compressBuffer(buffer, output, availableLength, codec));
    }

    int64_t actualLength = output - compressed->data();
    ARROW_RETURN_IF(actualLength < 0, arrow::Status::Invalid("Writing compressed buffer out of bound."));
    RETURN_NOT_OK(compressed->Resize(actualLength));
    return std::make_unique<BlockPayload>(
        Type::kCompressed, numRows, std::vector<std::shared_ptr<arrow::Buffer>>{compressed});
  }
  if (reuseBuffers) {
    // Copy.
    std::vector<std::shared_ptr<arrow::Buffer>> copies;
    for (auto& buffer : buffers) {
      if (!buffer) {
        copies.push_back(nullptr);
        continue;
      }
      ARROW_ASSIGN_OR_RAISE(auto copy, arrow::AllocateResizableBuffer(buffer->size(), pool));
      memcpy(copy->mutable_data(), buffer->data(), buffer->size());
      copies.push_back(std::move(copy));
    }
    return std::make_unique<BlockPayload>(Type::kUncompressed, numRows, std::move(copies));
  }
  if (payloadType == Type::kToBeCompressed) {
    return std::make_unique<CompressibleBlockPayload>(Type::kUncompressed, numRows, std::move(buffers), pool, codec);
  }
  return std::make_unique<BlockPayload>(Type::kUncompressed, numRows, std::move(buffers));
}

arrow::Status BlockPayload::serialize(arrow::io::OutputStream* outputStream) {
  RETURN_NOT_OK(outputStream->Write(&type_, sizeof(Type)));
  RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
  if (type_ == Type::kUncompressed) {
    for (auto& buffer : buffers_) {
      if (!buffer) {
        RETURN_NOT_OK(outputStream->Write(&kNullBuffer, sizeof(int64_t)));
        continue;
      }
      int64_t bufferSize = buffer->size();
      RETURN_NOT_OK(outputStream->Write(&bufferSize, sizeof(int64_t)));
      RETURN_NOT_OK(outputStream->Write(std::move(buffer)));
    }
  } else {
    RETURN_NOT_OK(outputStream->Write(std::move(buffers_[0])));
  }
  buffers_.clear();
  return arrow::Status::OK();
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> BlockPayload::deserialize(
    arrow::io::InputStream* inputStream,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::MemoryPool* pool,
    uint32_t& numRows) {
  static const std::vector<std::shared_ptr<arrow::Buffer>> kEmptyBuffers{};
  ARROW_ASSIGN_OR_RAISE(auto typeAndRows, readTypeAndRows(inputStream));
  if (typeAndRows.first == kIpcContinuationToken && typeAndRows.second == kZeroLength) {
    numRows = 0;
    return kEmptyBuffers;
  }
  numRows = typeAndRows.second;
  auto fields = schema->fields();

  auto isCompressionEnabled = typeAndRows.first == Type::kUncompressed || codec == nullptr;
  auto readBuffer = [&]() {
    if (isCompressionEnabled) {
      return readUncompressedBuffer(inputStream);
    } else {
      return readCompressedBuffer(inputStream, codec, pool);
    }
  };

  bool hasComplexDataType = false;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (const auto& field : fields) {
    auto fieldType = field->type()->id();
    switch (fieldType) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        buffers.emplace_back();
        ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
        buffers.emplace_back();
        ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
        buffers.emplace_back();
        ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
        break;
      }
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id: {
        hasComplexDataType = true;
      } break;
      default: {
        buffers.emplace_back();
        ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
        buffers.emplace_back();
        ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
        break;
      }
    }
  }
  if (hasComplexDataType) {
    buffers.emplace_back();
    ARROW_ASSIGN_OR_RAISE(buffers.back(), readBuffer());
  }
  return buffers;
}

arrow::Result<std::pair<int32_t, uint32_t>> BlockPayload::readTypeAndRows(arrow::io::InputStream* inputStream) {
  int32_t type;
  uint32_t numRows;
  RETURN_NOT_OK(inputStream->Read(sizeof(Type), &type));
  RETURN_NOT_OK(inputStream->Read(sizeof(uint32_t), &numRows));
  return std::make_pair(type, numRows);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> BlockPayload::readUncompressedBuffer(
    arrow::io::InputStream* inputStream) {
  int64_t bufferLength;
  RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &bufferLength));
  if (bufferLength == kNullBuffer) {
    return nullptr;
  }
  ARROW_ASSIGN_OR_RAISE(auto buffer, inputStream->Read(bufferLength));
  return buffer;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> BlockPayload::readCompressedBuffer(
    arrow::io::InputStream* inputStream,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::MemoryPool* pool) {
  int64_t compressedLength;
  int64_t uncompressedLength;
  RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &compressedLength));
  RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &uncompressedLength));
  if (compressedLength == kNullBuffer) {
    return nullptr;
  }
  if (compressedLength == kUncompressedBuffer) {
    ARROW_ASSIGN_OR_RAISE(auto uncompressed, arrow::AllocateBuffer(uncompressedLength, pool));
    RETURN_NOT_OK(inputStream->Read(uncompressedLength, const_cast<uint8_t*>(uncompressed->data())));
    return uncompressed;
  }
  ARROW_ASSIGN_OR_RAISE(auto compressed, arrow::AllocateBuffer(compressedLength, pool));
  RETURN_NOT_OK(inputStream->Read(compressedLength, const_cast<uint8_t*>(compressed->data())));
  ARROW_ASSIGN_OR_RAISE(auto output, arrow::AllocateBuffer(uncompressedLength, pool));
  RETURN_NOT_OK(codec->Decompress(
      compressedLength, compressed->data(), uncompressedLength, const_cast<uint8_t*>(output->data())));
  return output;
}

arrow::Status CompressibleBlockPayload::serialize(arrow::io::OutputStream* outputStream) {
  RETURN_NOT_OK(outputStream->Write(&kCompressedType, sizeof(Type)));
  RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
  auto metadataLength = sizeof(int64_t) * 2 * buffers_.size();
  int64_t totalCompressedLength =
      std::accumulate(buffers_.begin(), buffers_.end(), 0LL, [&](auto sum, const auto& buffer) {
        if (!buffer) {
          return sum;
        }
        return sum + codec_->MaxCompressedLen(buffer->size(), buffer->data());
      });
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<arrow::ResizableBuffer> compressed,
      arrow::AllocateResizableBuffer(metadataLength + totalCompressedLength, pool_));
  auto output = compressed->mutable_data();

  // Compress buffers one by one.
  for (auto& buffer : buffers_) {
    auto availableLength = compressed->size() - (output - compressed->data());
    RETURN_NOT_OK(compressBuffer(buffer, output, availableLength, codec_));
  }

  int64_t actualLength = output - compressed->data();
  ARROW_RETURN_IF(actualLength < 0, arrow::Status::Invalid("Writing compressed buffer out of bound."));
  RETURN_NOT_OK(compressed->Resize(actualLength));

  RETURN_NOT_OK(outputStream->Write(std::move(compressed)));
  return arrow::Status::OK();
}
} // namespace gluten
