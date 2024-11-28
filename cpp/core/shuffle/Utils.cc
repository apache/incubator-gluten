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
#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/mman.h>
#include <unistd.h>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <thread>
#include "shuffle/Options.h"
#include "utils/StringUtil.h"
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

uint64_t roundUpToPageSize(uint64_t value) {
  static auto pageSize = static_cast<size_t>(arrow::internal::GetPageSize());
  static auto pageMask = ~(pageSize - 1);
  DCHECK_GT(pageSize, 0);
  DCHECK_EQ(pageMask & pageSize, pageSize);
  return (value + pageSize - 1) & pageMask;
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

MmapFileStream::MmapFileStream(arrow::internal::FileDescriptor fd, uint8_t* data, int64_t size, uint64_t prefetchSize)
    : prefetchSize_(roundUpToPageSize(prefetchSize)), fd_(std::move(fd)), data_(data), size_(size){};

arrow::Result<std::shared_ptr<MmapFileStream>> MmapFileStream::open(const std::string& path, uint64_t prefetchSize) {
  ARROW_ASSIGN_OR_RAISE(auto fileName, arrow::internal::PlatformFilename::FromString(path));

  ARROW_ASSIGN_OR_RAISE(auto fd, arrow::internal::FileOpenReadable(fileName));
  ARROW_ASSIGN_OR_RAISE(auto size, arrow::internal::FileGetSize(fd.fd()));

  ARROW_RETURN_IF(size == 0, arrow::Status::Invalid("Cannot mmap an empty file: ", path));

  void* result = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd.fd(), 0);
  if (result == MAP_FAILED) {
    return arrow::Status::IOError("Memory mapping file failed: ", ::arrow::internal::ErrnoMessage(errno));
  }

  return std::make_shared<MmapFileStream>(std::move(fd), static_cast<uint8_t*>(result), size, prefetchSize);
}

arrow::Result<int64_t> MmapFileStream::actualReadSize(int64_t nbytes) {
  if (nbytes < 0 || pos_ > size_) {
    return arrow::Status::IOError("Read out of range. Offset: ", pos_, " Size: ", nbytes, " File Size: ", size_);
  }
  return std::min(size_ - pos_, nbytes);
}

bool MmapFileStream::closed() const {
  return data_ == nullptr;
};

void MmapFileStream::advance(int64_t length) {
  // Dont need data before pos
  auto purgeLength = (pos_ - posRetain_) / prefetchSize_ * prefetchSize_;
  if (purgeLength > 0) {
    int ret = madvise(data_ + posRetain_, purgeLength, MADV_DONTNEED);
    if (ret != 0) {
      LOG(WARNING) << "fadvise failed " << ::arrow::internal::ErrnoMessage(errno);
    }
    posRetain_ += purgeLength;
  }

  pos_ += length;
}

void MmapFileStream::willNeed(int64_t length) {
  // Skip if already fetched
  if (pos_ + length <= posFetch_) {
    return;
  }

  // Round up to multiple of prefetchSize
  auto fetchLen = ((length + prefetchSize_ - 1) / prefetchSize_) * prefetchSize_;
  fetchLen = std::min(size_ - pos_, fetchLen);
  int ret = madvise(data_ + posFetch_, fetchLen, MADV_WILLNEED);
  if (ret != 0) {
    LOG(WARNING) << "madvise willneed failed: " << ::arrow::internal::ErrnoMessage(errno);
  }

  posFetch_ += fetchLen;
}

arrow::Status MmapFileStream::Close() {
  if (data_ != nullptr) {
    int result = munmap(data_, size_);
    if (result != 0) {
      LOG(WARNING) << "munmap failed";
    }
    data_ = nullptr;
  }

  return fd_.Close();
}

arrow::Result<int64_t> MmapFileStream::Tell() const {
  return pos_;
}

arrow::Result<int64_t> MmapFileStream::Read(int64_t nbytes, void* out) {
  ARROW_ASSIGN_OR_RAISE(nbytes, actualReadSize(nbytes));

  if (nbytes > 0) {
    memcpy(out, data_ + pos_, nbytes);
    advance(nbytes);
  }

  return nbytes;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> MmapFileStream::Read(int64_t nbytes) {
  ARROW_ASSIGN_OR_RAISE(nbytes, actualReadSize(nbytes));

  if (nbytes > 0) {
    auto buffer = std::make_shared<arrow::Buffer>(data_ + pos_, nbytes);
    willNeed(nbytes);
    advance(nbytes);
    return buffer;
  } else {
    return std::make_shared<arrow::Buffer>(nullptr, 0);
  }
}
} // namespace gluten

std::string gluten::getShuffleSpillDir(const std::string& configuredDir, int32_t subDirId) {
  std::stringstream ss;
  ss << std::setfill('0') << std::setw(2) << std::hex << subDirId;
  return std::filesystem::path(configuredDir) / ss.str();
}

arrow::Result<std::string> gluten::createTempShuffleFile(const std::string& dir) {
  if (dir.length() == 0) {
    return arrow::Status::Invalid("Failed to create spilled file, got empty path.");
  }

  if (std::filesystem::exists(dir)) {
    if (!std::filesystem::is_directory(dir)) {
      return arrow::Status::Invalid("Invalid directory. File path exists but is not a directory: ", dir);
    }
  } else {
    std::filesystem::create_directories(dir);
  }

  const auto parentPath = std::filesystem::path(dir);
  bool exist = true;
  std::filesystem::path filePath;
  while (exist) {
    filePath = parentPath / ("temp-shuffle-" + generateUuid());
    if (!std::filesystem::exists(filePath)) {
      auto fd = open(filePath.c_str(), O_CREAT | O_EXCL | O_RDWR, 0666);
      if (fd < 0) {
        if (errno != EEXIST) {
          return arrow::Status::IOError(
              "Failed to open local file " + filePath.string() + ", Reason: " + strerror(errno));
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
      case arrow::MonthIntervalType::type_id:
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
