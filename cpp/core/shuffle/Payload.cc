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
#include "shuffle/Payload.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/util/bitmap.h>
#include <iostream>
#include <numeric>

#include "shuffle/Options.h"
#include "shuffle/Utils.h"
#include "utils/Timer.h"
#include "utils/exception.h"

namespace gluten {
namespace {

static const Payload::Type kCompressedType = gluten::BlockPayload::kCompressed;
static const Payload::Type kUncompressedType = gluten::BlockPayload::kUncompressed;

static constexpr int64_t kZeroLengthBuffer = 0;
static constexpr int64_t kNullBuffer = -1;
static constexpr int64_t kUncompressedBuffer = -2;

template <typename T>
void write(uint8_t** dst, T data) {
  memcpy(*dst, &data, sizeof(T));
  *dst += sizeof(T);
}

template <typename T>
T* advance(uint8_t** dst) {
  auto ptr = reinterpret_cast<T*>(*dst);
  *dst += sizeof(T);
  return ptr;
}

arrow::Result<std::pair<uint8_t, uint32_t>> readTypeAndRows(arrow::io::InputStream* inputStream) {
  uint8_t type;
  uint32_t numRows;
  ARROW_ASSIGN_OR_RAISE(auto bytes, inputStream->Read(sizeof(Payload::Type), &type));
  if (bytes == 0) {
    // Reach EOS.
    return std::make_pair(0, 0);
  }
  RETURN_NOT_OK(inputStream->Read(sizeof(uint32_t), &numRows));
  return std::make_pair(type, numRows);
}

arrow::Result<int64_t> compressBuffer(
    const std::shared_ptr<arrow::Buffer>& buffer,
    uint8_t* output,
    int64_t outputLength,
    arrow::util::Codec* codec) {
  auto outputPtr = &output;
  if (!buffer) {
    write<int64_t>(outputPtr, kNullBuffer);
    return sizeof(int64_t);
  }
  if (buffer->size() == 0) {
    write<int64_t>(outputPtr, kZeroLengthBuffer);
    return sizeof(int64_t);
  }
  static const int64_t kCompressedBufferHeaderLength = 2 * sizeof(int64_t);
  auto* compressedLengthPtr = advance<int64_t>(outputPtr);
  write(outputPtr, static_cast<int64_t>(buffer->size()));
  ARROW_ASSIGN_OR_RAISE(
      auto compressedLength, codec->Compress(buffer->size(), buffer->data(), outputLength, *outputPtr));
  if (compressedLength >= buffer->size()) {
    // Write uncompressed buffer.
    memcpy(*outputPtr, buffer->data(), buffer->size());
    *compressedLengthPtr = kUncompressedBuffer;
    return kCompressedBufferHeaderLength + buffer->size();
  }
  *compressedLengthPtr = static_cast<int64_t>(compressedLength);
  return kCompressedBufferHeaderLength + compressedLength;
}

arrow::Status compressAndFlush(
    const std::shared_ptr<arrow::Buffer>& buffer,
    arrow::io::OutputStream* outputStream,
    arrow::util::Codec* codec,
    arrow::MemoryPool* pool,
    int64_t& compressTime,
    int64_t& writeTime) {
  if (!buffer) {
    ScopedTimer timer(&writeTime);
    RETURN_NOT_OK(outputStream->Write(&kNullBuffer, sizeof(int64_t)));
    return arrow::Status::OK();
  }
  if (buffer->size() == 0) {
    ScopedTimer timer(&writeTime);
    RETURN_NOT_OK(outputStream->Write(&kZeroLengthBuffer, sizeof(int64_t)));
    return arrow::Status::OK();
  }
  ScopedTimer timer(&compressTime);
  auto maxCompressedLength = codec->MaxCompressedLen(buffer->size(), buffer->data());
  ARROW_ASSIGN_OR_RAISE(
      auto compressed, arrow::AllocateResizableBuffer(sizeof(int64_t) * 2 + maxCompressedLength, pool));
  auto output = compressed->mutable_data();
  ARROW_ASSIGN_OR_RAISE(auto compressedSize, compressBuffer(buffer, output, maxCompressedLength, codec));

  timer.switchTo(&writeTime);
  RETURN_NOT_OK(outputStream->Write(compressed->data(), compressedSize));
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> readUncompressedBuffer(arrow::io::InputStream* inputStream) {
  int64_t bufferLength;
  RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &bufferLength));
  if (bufferLength == kNullBuffer) {
    return nullptr;
  }
  ARROW_ASSIGN_OR_RAISE(auto buffer, inputStream->Read(bufferLength));
  return buffer;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> readCompressedBuffer(
    arrow::io::InputStream* inputStream,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::MemoryPool* pool,
    int64_t& decompressTime) {
  int64_t compressedLength;
  RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &compressedLength));
  if (compressedLength == kNullBuffer) {
    return nullptr;
  }
  if (compressedLength == kZeroLengthBuffer) {
    return zeroLengthNullBuffer();
  }

  int64_t uncompressedLength;
  RETURN_NOT_OK(inputStream->Read(sizeof(int64_t), &uncompressedLength));
  if (compressedLength == kUncompressedBuffer) {
    ARROW_ASSIGN_OR_RAISE(auto uncompressed, arrow::AllocateResizableBuffer(uncompressedLength, pool));
    RETURN_NOT_OK(inputStream->Read(uncompressedLength, const_cast<uint8_t*>(uncompressed->data())));
    return uncompressed;
  }
  ARROW_ASSIGN_OR_RAISE(auto compressed, arrow::AllocateBuffer(compressedLength, pool));
  RETURN_NOT_OK(inputStream->Read(compressedLength, const_cast<uint8_t*>(compressed->data())));

  ScopedTimer timer(&decompressTime);
  ARROW_ASSIGN_OR_RAISE(auto output, arrow::AllocateResizableBuffer(uncompressedLength, pool));
  RETURN_NOT_OK(codec->Decompress(
      compressedLength, compressed->data(), uncompressedLength, const_cast<uint8_t*>(output->data())));
  return output;
}

} // namespace

Payload::Payload(Payload::Type type, uint32_t numRows, const std::vector<bool>* isValidityBuffer)
    : type_(type), numRows_(numRows), isValidityBuffer_(isValidityBuffer) {}

std::string Payload::toString() const {
  static std::string kUncompressedString = "Payload::kUncompressed";
  static std::string kCompressedString = "Payload::kCompressed";
  static std::string kToBeCompressedString = "Payload::kToBeCompressed";

  if (type_ == kUncompressed) {
    return kUncompressedString;
  }
  if (type_ == kCompressed) {
    return kCompressedString;
  }
  return kToBeCompressedString;
}

arrow::Result<std::unique_ptr<BlockPayload>> BlockPayload::fromBuffers(
    Payload::Type payloadType,
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers,
    const std::vector<bool>* isValidityBuffer,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec) {
  if (payloadType == Payload::Type::kCompressed) {
    Timer compressionTime;
    compressionTime.start();
    // Compress.
    // Compressed buffer layout: | buffer1 compressedLength | buffer1 uncompressedLength | buffer1 | ...
    const auto metadataLength = sizeof(int64_t) * 2 * buffers.size();
    int64_t totalCompressedLength =
        std::accumulate(buffers.begin(), buffers.end(), 0LL, [&](auto sum, const auto& buffer) {
          if (!buffer) {
            return sum;
          }
          return sum + codec->MaxCompressedLen(buffer->size(), buffer->data());
        });
    const auto maxCompressedLength = metadataLength + totalCompressedLength;
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::ResizableBuffer> compressed, arrow::AllocateResizableBuffer(maxCompressedLength, pool));

    auto output = compressed->mutable_data();
    int64_t actualLength = 0;
    // Compress buffers one by one.
    for (auto& buffer : buffers) {
      auto availableLength = maxCompressedLength - actualLength;
      // Release buffer after compression.
      ARROW_ASSIGN_OR_RAISE(auto compressedSize, compressBuffer(std::move(buffer), output, availableLength, codec));
      output += compressedSize;
      actualLength += compressedSize;
    }

    ARROW_RETURN_IF(actualLength < 0, arrow::Status::Invalid("Writing compressed buffer out of bound."));
    RETURN_NOT_OK(compressed->Resize(actualLength));
    compressionTime.stop();
    auto payload = std::unique_ptr<BlockPayload>(new BlockPayload(
        Type::kCompressed,
        numRows,
        std::vector<std::shared_ptr<arrow::Buffer>>{compressed},
        isValidityBuffer,
        pool,
        codec));
    payload->setCompressionTime(compressionTime.realTimeUsed());
    return payload;
  }
  return std::unique_ptr<BlockPayload>(
      new BlockPayload(payloadType, numRows, std::move(buffers), isValidityBuffer, pool, codec));
}

arrow::Status BlockPayload::serialize(arrow::io::OutputStream* outputStream) {
  switch (type_) {
    case Type::kUncompressed: {
      ScopedTimer timer(&writeTime_);
      RETURN_NOT_OK(outputStream->Write(&kUncompressedType, sizeof(Type)));
      RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
      for (auto& buffer : buffers_) {
        if (!buffer) {
          RETURN_NOT_OK(outputStream->Write(&kNullBuffer, sizeof(int64_t)));
          continue;
        }
        int64_t bufferSize = buffer->size();
        RETURN_NOT_OK(outputStream->Write(&bufferSize, sizeof(int64_t)));
        if (bufferSize > 0) {
          RETURN_NOT_OK(outputStream->Write(std::move(buffer)));
        }
      }
    } break;
    case Type::kToBeCompressed: {
      {
        ScopedTimer timer(&writeTime_);
        RETURN_NOT_OK(outputStream->Write(&kCompressedType, sizeof(Type)));
        RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
      }
      for (auto& buffer : buffers_) {
        RETURN_NOT_OK(compressAndFlush(std::move(buffer), outputStream, codec_, pool_, compressTime_, writeTime_));
      }
    } break;
    case Type::kCompressed: {
      ScopedTimer timer(&writeTime_);
      RETURN_NOT_OK(outputStream->Write(&kCompressedType, sizeof(Type)));
      RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
      RETURN_NOT_OK(outputStream->Write(std::move(buffers_[0])));
    } break;
  }
  buffers_.clear();
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> BlockPayload::readBufferAt(uint32_t pos) {
  if (type_ == Type::kCompressed) {
    return arrow::Status::Invalid("Cannot read buffer from compressed BlockPayload.");
  }
  return std::move(buffers_[pos]);
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> BlockPayload::deserialize(
    arrow::io::InputStream* inputStream,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::util::Codec>& codec,
    arrow::MemoryPool* pool,
    uint32_t& numRows,
    int64_t& decompressTime) {
  static const std::vector<std::shared_ptr<arrow::Buffer>> kEmptyBuffers{};
  ARROW_ASSIGN_OR_RAISE(auto typeAndRows, readTypeAndRows(inputStream));
  if (typeAndRows.first == 0) {
    numRows = 0;
    return kEmptyBuffers;
  }
  numRows = typeAndRows.second;
  auto fields = schema->fields();

  auto isCompressionEnabled = typeAndRows.first == Type::kCompressed;
  auto readBuffer = [&]() {
    if (isCompressionEnabled) {
      return readCompressedBuffer(inputStream, codec, pool, decompressTime);
    } else {
      return readUncompressedBuffer(inputStream);
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

void BlockPayload::setCompressionTime(int64_t compressionTime) {
  compressTime_ = compressionTime;
}

arrow::Result<std::unique_ptr<InMemoryPayload>> InMemoryPayload::merge(
    std::unique_ptr<InMemoryPayload> source,
    std::unique_ptr<InMemoryPayload> append,
    arrow::MemoryPool* pool) {
  auto mergedRows = source->numRows() + append->numRows();
  auto isValidityBuffer = source->isValidityBuffer();

  auto numBuffers = append->numBuffers();
  ARROW_RETURN_IF(
      numBuffers != source->numBuffers(), arrow::Status::Invalid("Number of merging buffers doesn't match."));
  std::vector<std::shared_ptr<arrow::Buffer>> merged;
  merged.resize(numBuffers);
  for (size_t i = 0; i < numBuffers; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto sourceBuffer, source->readBufferAt(i));
    ARROW_ASSIGN_OR_RAISE(auto appendBuffer, append->readBufferAt(i));
    if (isValidityBuffer->at(i)) {
      if (!sourceBuffer) {
        if (!appendBuffer) {
          merged[i] = nullptr;
        } else {
          ARROW_ASSIGN_OR_RAISE(
              auto buffer, arrow::AllocateResizableBuffer(arrow::bit_util::BytesForBits(mergedRows), pool));
          // Source is null, fill all true.
          arrow::bit_util::SetBitsTo(buffer->mutable_data(), 0, source->numRows(), true);
          // Write append bits.
          arrow::internal::CopyBitmap(
              appendBuffer->data(), 0, append->numRows(), buffer->mutable_data(), source->numRows());
          merged[i] = std::move(buffer);
        }
      } else {
        // Because sourceBuffer can be resized, need to save buffer size in advance.
        auto sourceBufferSize = sourceBuffer->size();
        auto resizable = std::dynamic_pointer_cast<arrow::ResizableBuffer>(sourceBuffer);
        auto mergedBytes = arrow::bit_util::BytesForBits(mergedRows);
        if (resizable) {
          // If source is resizable, resize and reuse source.
          RETURN_NOT_OK(resizable->Resize(mergedBytes));
        } else {
          // Otherwise copy source.
          ARROW_ASSIGN_OR_RAISE(resizable, arrow::AllocateResizableBuffer(mergedBytes, pool));
          memcpy(resizable->mutable_data(), sourceBuffer->data(), sourceBufferSize);
        }
        if (!appendBuffer) {
          arrow::bit_util::SetBitsTo(resizable->mutable_data(), source->numRows(), append->numRows(), true);
        } else {
          arrow::internal::CopyBitmap(
              appendBuffer->data(), 0, append->numRows(), resizable->mutable_data(), source->numRows());
        }
        merged[i] = std::move(resizable);
      }
    } else {
      if (appendBuffer->size() == 0) {
        merged[i] = std::move(sourceBuffer);
      } else {
        // Because sourceBuffer can be resized, need to save buffer size in advance.
        auto sourceBufferSize = sourceBuffer->size();
        auto mergedSize = sourceBufferSize + appendBuffer->size();
        auto resizable = std::dynamic_pointer_cast<arrow::ResizableBuffer>(sourceBuffer);
        if (resizable) {
          // If source is resizable, resize and reuse source.
          RETURN_NOT_OK(resizable->Resize(mergedSize));
        } else {
          // Otherwise copy source.
          ARROW_ASSIGN_OR_RAISE(resizable, arrow::AllocateResizableBuffer(mergedSize, pool));
          memcpy(resizable->mutable_data(), sourceBuffer->data(), sourceBufferSize);
        }
        // Copy append.
        memcpy(resizable->mutable_data() + sourceBufferSize, appendBuffer->data(), appendBuffer->size());
        merged[i] = std::move(resizable);
      }
    }
  }
  return std::make_unique<InMemoryPayload>(mergedRows, isValidityBuffer, std::move(merged));
}

arrow::Result<std::unique_ptr<BlockPayload>>
InMemoryPayload::toBlockPayload(Payload::Type payloadType, arrow::MemoryPool* pool, arrow::util::Codec* codec) {
  return BlockPayload::fromBuffers(payloadType, numRows_, std::move(buffers_), isValidityBuffer_, pool, codec);
}

arrow::Status InMemoryPayload::serialize(arrow::io::OutputStream* outputStream) {
  return arrow::Status::Invalid("Cannot serialize InMemoryPayload.");
}

arrow::Result<std::shared_ptr<arrow::Buffer>> InMemoryPayload::readBufferAt(uint32_t index) {
  return std::move(buffers_[index]);
}

int64_t InMemoryPayload::getBufferSize() const {
  return gluten::getBufferSize(buffers_);
}

arrow::Status InMemoryPayload::copyBuffers(arrow::MemoryPool* pool) {
  for (auto& buffer : buffers_) {
    if (!buffer) {
      continue;
    }
    if (buffer->size() == 0) {
      buffer = zeroLengthNullBuffer();
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(auto copy, arrow::AllocateResizableBuffer(buffer->size(), pool));
    memcpy(copy->mutable_data(), buffer->data(), buffer->size());
    buffer = std::move(copy);
  }
  return arrow::Status::OK();
}

UncompressedDiskBlockPayload::UncompressedDiskBlockPayload(
    Type type,
    uint32_t numRows,
    const std::vector<bool>* isValidityBuffer,
    arrow::io::InputStream*& inputStream,
    uint64_t rawSize,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec)
    : Payload(type, numRows, isValidityBuffer),
      inputStream_(inputStream),
      rawSize_(rawSize),
      pool_(pool),
      codec_(codec) {}

arrow::Result<std::shared_ptr<arrow::Buffer>> UncompressedDiskBlockPayload::readBufferAt(uint32_t index) {
  return arrow::Status::Invalid("Cannot read buffer from UncompressedDiskBlockPayload.");
}

arrow::Status UncompressedDiskBlockPayload::serialize(arrow::io::OutputStream* outputStream) {
  if (codec_ == nullptr || type_ == Payload::kUncompressed) {
    ARROW_ASSIGN_OR_RAISE(auto block, inputStream_->Read(rawSize_));
    RETURN_NOT_OK(outputStream->Write(block));
    return arrow::Status::OK();
  }

  ARROW_RETURN_IF(
      type_ != Payload::kToBeCompressed,
      arrow::Status::Invalid(
          "Invalid payload type: " + std::to_string(type_) +
          ", should be either Payload::kUncompressed or Payload::kToBeCompressed"));
  ARROW_ASSIGN_OR_RAISE(auto startPos, inputStream_->Tell());
  auto typeAndRows = readTypeAndRows(inputStream_);
  // Discard type and rows.
  RETURN_NOT_OK(typeAndRows.status());
  RETURN_NOT_OK(outputStream->Write(&kCompressedType, sizeof(kCompressedType)));
  RETURN_NOT_OK(outputStream->Write(&numRows_, sizeof(uint32_t)));
  auto readPos = startPos + sizeof(kUncompressedType) + sizeof(uint32_t);
  while (readPos - startPos < rawSize_) {
    ARROW_ASSIGN_OR_RAISE(auto uncompressed, readUncompressedBuffer());
    ARROW_ASSIGN_OR_RAISE(readPos, inputStream_->Tell());
    RETURN_NOT_OK(compressAndFlush(std::move(uncompressed), outputStream, codec_, pool_, compressTime_, writeTime_));
  }
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> UncompressedDiskBlockPayload::readUncompressedBuffer() {
  readPos_++;
  int64_t bufferLength;
  RETURN_NOT_OK(inputStream_->Read(sizeof(int64_t), &bufferLength));
  if (bufferLength == kNullBuffer) {
    return nullptr;
  }
  if (bufferLength == 0) {
    return zeroLengthNullBuffer();
  }
  ARROW_ASSIGN_OR_RAISE(auto buffer, inputStream_->Read(bufferLength));
  return buffer;
}

CompressedDiskBlockPayload::CompressedDiskBlockPayload(
    uint32_t numRows,
    const std::vector<bool>* isValidityBuffer,
    arrow::io::InputStream*& inputStream,
    uint64_t rawSize,
    arrow::MemoryPool* /* pool */)
    : Payload(Type::kCompressed, numRows, isValidityBuffer), inputStream_(inputStream), rawSize_(rawSize) {}

arrow::Status CompressedDiskBlockPayload::serialize(arrow::io::OutputStream* outputStream) {
  ARROW_ASSIGN_OR_RAISE(auto block, inputStream_->Read(rawSize_));
  RETURN_NOT_OK(outputStream->Write(block));
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> CompressedDiskBlockPayload::readBufferAt(uint32_t index) {
  return arrow::Status::Invalid("Cannot read buffer from CompressedDiskBlockPayload.");
}
} // namespace gluten
