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

#include "shuffle/Utils.h"
#include <arrow/record_batch.h>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <thread>
#include "shuffle/Options.h"
#include "utils/Timer.h"

namespace gluten {
namespace {
arrow::Result<std::shared_ptr<arrow::Array>> makeNullBinaryArray(
    std::shared_ptr<arrow::DataType> type,
    arrow::MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto offsetBuffer, arrow::AllocateResizableBuffer(kSizeOfIpcOffsetBuffer << 1, pool));
  // set the first offset to 0, and set the value offset
  uint8_t* offsetaddr = offsetBuffer->mutable_data();
  memset(offsetaddr, 0, kSizeOfIpcOffsetBuffer);
  // second value offset 0
  memset(offsetaddr + kSizeOfIpcOffsetBuffer, 0, kSizeOfIpcOffsetBuffer);
  // If it is not compressed array, null valueBuffer
  // worked, but if compress, will core dump at buffer::size(), so replace by kNullBuffer
  static std::shared_ptr<arrow::Buffer> kNullBuffer = std::make_shared<arrow::Buffer>(nullptr, 0);
  return arrow::MakeArray(arrow::ArrayData::Make(type, 1, {nullptr, std::move(offsetBuffer), kNullBuffer}));
}

arrow::Result<std::shared_ptr<arrow::Array>> makeBinaryArray(
    std::shared_ptr<arrow::DataType> type,
    std::shared_ptr<arrow::Buffer> valueBuffer,
    arrow::MemoryPool* pool) {
  if (valueBuffer == nullptr) {
    return makeNullBinaryArray(type, pool);
  }

  ARROW_ASSIGN_OR_RAISE(auto offsetBuffer, arrow::AllocateResizableBuffer(kSizeOfIpcOffsetBuffer << 1, pool));
  // set the first offset to 0, and set the value offset
  uint8_t* offsetaddr = offsetBuffer->mutable_data();
  memset(offsetaddr, 0, kSizeOfIpcOffsetBuffer);
  int64_t length = valueBuffer->size();
  memcpy(offsetaddr + kSizeOfIpcOffsetBuffer, reinterpret_cast<uint8_t*>(&length), kSizeOfIpcOffsetBuffer);
  return arrow::MakeArray(arrow::ArrayData::Make(type, 1, {nullptr, std::move(offsetBuffer), valueBuffer}));
}

// Length buffer layout |compressionMode|buffers.size()|buffer1 unCompressedLength|buffer1 compressedLength| buffer2...
arrow::Status getLengthBufferAndValueBufferOneByOne(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    std::shared_ptr<arrow::ResizableBuffer>& lengthBuffer,
    std::shared_ptr<arrow::ResizableBuffer>& valueBuffer) {
  ARROW_ASSIGN_OR_RAISE(
      lengthBuffer, arrow::AllocateResizableBuffer((1 + 1 + buffers.size() * 2) * sizeof(int64_t), pool));
  auto lengthBufferPtr = (int64_t*)(lengthBuffer->mutable_data());
  // Write compression mode.
  *lengthBufferPtr++ = CompressionMode::BUFFER;
  // Write number of buffers.
  *lengthBufferPtr++ = buffers.size();

  int64_t compressedBufferMaxSize = getMaxCompressedBufferSize(buffers, codec);
  ARROW_ASSIGN_OR_RAISE(valueBuffer, arrow::AllocateResizableBuffer(compressedBufferMaxSize, pool));
  int64_t compressValueOffset = 0;
  for (auto& buffer : buffers) {
    if (buffer != nullptr && buffer->size() != 0) {
      int64_t actualLength;
      int64_t maxLength = codec->MaxCompressedLen(buffer->size(), nullptr);
      ARROW_ASSIGN_OR_RAISE(
          actualLength,
          codec->Compress(
              buffer->size(), buffer->data(), maxLength, valueBuffer->mutable_data() + compressValueOffset));
      compressValueOffset += actualLength;
      *lengthBufferPtr++ = buffer->size();
      *lengthBufferPtr++ = actualLength;
    } else {
      *lengthBufferPtr++ = 0;
      *lengthBufferPtr++ = 0;
    }
  }
  RETURN_NOT_OK(valueBuffer->Resize(compressValueOffset, /*shrink*/ true));
  return arrow::Status::OK();
}

// Length buffer layout |compressionMode|buffer unCompressedLength|buffer compressedLength|buffers.size()| buffer1 size
// | buffer2 size
arrow::Status getLengthBufferAndValueBufferStream(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    std::shared_ptr<arrow::ResizableBuffer>& lengthBuffer,
    std::shared_ptr<arrow::ResizableBuffer>& compressedBuffer) {
  ARROW_ASSIGN_OR_RAISE(lengthBuffer, arrow::AllocateResizableBuffer((1 + 3 + buffers.size()) * sizeof(int64_t), pool));
  auto originalBufferSize = getBufferSize(buffers);

  // because 64B align, uncompressedBuffer size maybe bigger than unCompressedBufferSize which is
  // getBuffersSize(buffers), then cannot use this size
  ARROW_ASSIGN_OR_RAISE(auto uncompressedBuffer, arrow::AllocateResizableBuffer(originalBufferSize, pool));
  int64_t uncompressedSize = uncompressedBuffer->size();

  auto lengthBufferPtr = (int64_t*)(lengthBuffer->mutable_data());
  // First write metadata.
  // Write compression mode.
  *lengthBufferPtr++ = CompressionMode::ROWVECTOR;
  // Store uncompressed size.
  *lengthBufferPtr++ = uncompressedSize; // uncompressedLength
  // Skip compressed size and update later.
  auto compressedLengthPtr = lengthBufferPtr++;
  // Store number of buffers.
  *lengthBufferPtr++ = buffers.size();

  int64_t compressValueOffset = 0;
  for (auto& buffer : buffers) {
    // Copy all buffers into one big buffer.
    if (buffer != nullptr && buffer->size() != 0) {
      *lengthBufferPtr++ = buffer->size();
      memcpy(uncompressedBuffer->mutable_data() + compressValueOffset, buffer->data(), buffer->size());
      compressValueOffset += buffer->size();
    } else {
      *lengthBufferPtr++ = 0;
    }
  }

  // Compress the big buffer.
  int64_t maxLength = codec->MaxCompressedLen(uncompressedSize, nullptr);
  ARROW_ASSIGN_OR_RAISE(compressedBuffer, arrow::AllocateResizableBuffer(maxLength, pool));
  ARROW_ASSIGN_OR_RAISE(
      int64_t actualLength,
      codec->Compress(uncompressedSize, uncompressedBuffer->data(), maxLength, compressedBuffer->mutable_data()));
  RETURN_NOT_OK(compressedBuffer->Resize(actualLength, /*shrink*/ true));

  // Update compressed size.
  *compressedLengthPtr = actualLength;
  return arrow::Status::OK();
}
} // namespace

arrow::Result<std::shared_ptr<arrow::RecordBatch>> makeCompressedRecordBatch(
    uint32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    const std::shared_ptr<arrow::Schema> compressWriteSchema,
    arrow::MemoryPool* pool,
    arrow::util::Codec* codec,
    int32_t bufferCompressThreshold,
    CompressionMode compressionMode,
    int64_t& compressionTime) {
  ScopedTimer timer{&compressionTime};
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  // header col, numRows, compressionType
  {
    ARROW_ASSIGN_OR_RAISE(auto headerBuffer, arrow::AllocateResizableBuffer(sizeof(uint32_t) + sizeof(int32_t), pool));
    memcpy(headerBuffer->mutable_data(), &numRows, sizeof(uint32_t));
    int32_t compressType = static_cast<int32_t>(codec->compression_type());
    memcpy(headerBuffer->mutable_data() + sizeof(uint32_t), &compressType, sizeof(int32_t));
    arrays.emplace_back();
    ARROW_ASSIGN_OR_RAISE(
        arrays.back(), makeBinaryArray(compressWriteSchema->field(0)->type(), std::move(headerBuffer), pool));
  }
  std::shared_ptr<arrow::ResizableBuffer> lengthBuffer;
  std::shared_ptr<arrow::ResizableBuffer> valueBuffer;
  if (compressionMode == CompressionMode::BUFFER && numRows > bufferCompressThreshold) {
    RETURN_NOT_OK(getLengthBufferAndValueBufferOneByOne(buffers, pool, codec, lengthBuffer, valueBuffer));
  } else {
    RETURN_NOT_OK(getLengthBufferAndValueBufferStream(buffers, pool, codec, lengthBuffer, valueBuffer));
  }

  arrays.emplace_back();
  ARROW_ASSIGN_OR_RAISE(arrays.back(), makeBinaryArray(compressWriteSchema->field(1)->type(), lengthBuffer, pool));
  arrays.emplace_back();
  ARROW_ASSIGN_OR_RAISE(arrays.back(), makeBinaryArray(compressWriteSchema->field(2)->type(), valueBuffer, pool));
  return arrow::RecordBatch::Make(compressWriteSchema, 1, {arrays});
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> makeUncompressedRecordBatch(
    uint32_t numRows,
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    const std::shared_ptr<arrow::Schema> writeSchema,
    arrow::MemoryPool* pool) {
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  // header col, numRows, compressionType
  {
    ARROW_ASSIGN_OR_RAISE(auto headerBuffer, arrow::AllocateResizableBuffer(sizeof(uint32_t) + sizeof(int32_t), pool));
    memcpy(headerBuffer->mutable_data(), &numRows, sizeof(uint32_t));
    int32_t compressType = static_cast<int32_t>(arrow::Compression::type::UNCOMPRESSED);
    memcpy(headerBuffer->mutable_data() + sizeof(uint32_t), &compressType, sizeof(int32_t));
    arrays.emplace_back();
    ARROW_ASSIGN_OR_RAISE(arrays.back(), makeBinaryArray(writeSchema->field(0)->type(), std::move(headerBuffer), pool));
  }

  int32_t bufferNum = writeSchema->num_fields() - 1;
  for (int32_t i = 0; i < bufferNum; i++) {
    arrays.emplace_back();
    ARROW_ASSIGN_OR_RAISE(arrays.back(), makeBinaryArray(writeSchema->field(i + 1)->type(), buffers[i], pool));
  }
  return arrow::RecordBatch::Make(writeSchema, 1, {arrays});
}
} // namespace gluten

std::string gluten::generateUuid() {
  boost::uuids::random_generator generator;
  return boost::uuids::to_string(generator());
}

std::string gluten::getSpilledShuffleFileDir(const std::string& configuredDir, int32_t subDirId) {
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  std::stringstream ss;
  ss << std::setfill('0') << std::setw(2) << std::hex << subDirId;
  auto dir = arrow::fs::internal::ConcatAbstractPath(configuredDir, ss.str());
  return dir;
}

arrow::Result<std::string> gluten::createTempShuffleFile(const std::string& dir) {
  if (dir.length() == 0) {
    return arrow::Status::Invalid("Failed to create spilled file, got empty path.");
  }

  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ARROW_ASSIGN_OR_RAISE(auto path_info, fs->GetFileInfo(dir));
  if (path_info.type() == arrow::fs::FileType::NotFound) {
    RETURN_NOT_OK(fs->CreateDir(dir, true));
  }

  bool exist = true;
  std::string filePath;
  while (exist) {
    filePath = arrow::fs::internal::ConcatAbstractPath(dir, "temp_shuffle_" + generateUuid());
    ARROW_ASSIGN_OR_RAISE(auto file_info, fs->GetFileInfo(filePath));
    if (file_info.type() == arrow::fs::FileType::NotFound) {
      int fd = open(filePath.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);
      if (fd < 0) {
        if (errno != EEXIST) {
          return arrow::Status::IOError("Failed to open local file " + filePath + ", Reason: " + strerror(errno));
        }
      } else {
        exist = false;
        close(fd);
      }
    }
  }
  return filePath;
}

arrow::Result<std::vector<std::shared_ptr<arrow::DataType>>> gluten::toShuffleTypeId(
    const std::vector<std::shared_ptr<arrow::Field>>& fields) {
  std::vector<std::shared_ptr<arrow::DataType>> shuffleTypeId;
  for (auto field : fields) {
    switch (field->type()->id()) {
      case arrow::BooleanType::type_id:
      case arrow::Int8Type::type_id:
      case arrow::UInt8Type::type_id:
      case arrow::Int16Type::type_id:
      case arrow::UInt16Type::type_id:
      case arrow::HalfFloatType::type_id:
      case arrow::Int32Type::type_id:
      case arrow::UInt32Type::type_id:
      case arrow::FloatType::type_id:
      case arrow::Date32Type::type_id:
      case arrow::Time32Type::type_id:
      case arrow::Int64Type::type_id:
      case arrow::UInt64Type::type_id:
      case arrow::DoubleType::type_id:
      case arrow::Date64Type::type_id:
      case arrow::Time64Type::type_id:
      case arrow::TimestampType::type_id:
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id:
      case arrow::LargeBinaryType::type_id:
      case arrow::LargeStringType::type_id:
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::ListType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::Decimal128Type::type_id:
      case arrow::NullType::type_id:
        shuffleTypeId.push_back(field->type());
        break;
      default:
        RETURN_NOT_OK(arrow::Status::NotImplemented(
            "Field type not implemented in ColumnarShuffle, type is ", field->type()->ToString()));
    }
  }
  return shuffleTypeId;
}

int64_t gluten::getBufferSize(const std::shared_ptr<arrow::Array>& array) {
  return gluten::getBufferSize(array->data()->buffers);
}

int64_t gluten::getBufferSize(const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) {
  return std::accumulate(
      std::cbegin(buffers), std::cend(buffers), 0LL, [](int64_t sum, const std::shared_ptr<arrow::Buffer>& buf) {
        return buf == nullptr ? sum : sum + buf->size();
      });
}

int64_t gluten::getMaxCompressedBufferSize(
    const std::vector<std::shared_ptr<arrow::Buffer>>& buffers,
    arrow::util::Codec* codec) {
  int64_t totalSize = 0;
  for (auto& buffer : buffers) {
    if (buffer != nullptr && buffer->size() != 0) {
      totalSize += codec->MaxCompressedLen(buffer->size(), nullptr);
    }
  }
  return totalSize;
}

std::shared_ptr<arrow::Buffer> gluten::zeroLengthNullBuffer() {
  static std::shared_ptr<arrow::Buffer> kNullBuffer = std::make_shared<arrow::Buffer>(nullptr, 0);
  return kNullBuffer;
}
