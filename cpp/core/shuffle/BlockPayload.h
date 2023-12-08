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

#include <numeric>

#include <arrow/buffer.h>
#include "shuffle/Options.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/Utils.h"

namespace gluten {
// A block represents data to be cached in-memory or spilled.
// Can be compressed or uncompressed.

namespace {

static constexpr int64_t kNullBuffer = -1;
static constexpr int64_t kUncompressedBuffer = -2;

template <typename T>
void write(uint8_t** dst, T data) {
  auto ptr = reinterpret_cast<T*>(*dst);
  *ptr = data;
  *dst += sizeof(T);
}

template <typename T>
T* advance(uint8_t** dst) {
  auto ptr = reinterpret_cast<T*>(*dst);
  *dst += sizeof(T);
  return ptr;
}

arrow::Status compressBuffer(
    std::shared_ptr<arrow::Buffer>& buffer,
    uint8_t*& output,
    int64_t outputLength,
    arrow::util::Codec* codec) {
  if (!buffer) {
    write<int64_t>(&output, kNullBuffer);
    write<int64_t>(&output, kNullBuffer);
    return arrow::Status::OK();
  }
  auto* compressedLengthPtr = advance<int64_t>(&output);
  write(&output, static_cast<int64_t>(buffer->size()));
  ARROW_ASSIGN_OR_RAISE(auto compressedLength, codec->Compress(buffer->size(), buffer->data(), outputLength, output));
  if (compressedLength > buffer->size()) {
    // Write uncompressed buffer.
    memcpy(output, buffer->data(), buffer->size());
    output += buffer->size();
    *compressedLengthPtr = kUncompressedBuffer;
  } else {
    output += compressedLength;
    *compressedLengthPtr = static_cast<int64_t>(compressedLength);
  }
  // Release buffer after compression.
  buffer = nullptr;
  return arrow::Status::OK();
}

} // namespace

class BlockPayload : public Payload {
 public:
  enum Type : int32_t { kCompressed, kUncompressed };

  BlockPayload(BlockPayload::Type type, uint32_t numRows, std::vector<std::shared_ptr<arrow::Buffer>> buffers)
      : type_(type), numRows_(numRows), buffers_(std::move(buffers)) {}

  static arrow::Result<std::unique_ptr<BlockPayload>> fromBuffers(
      uint32_t numRows,
      std::vector<std::shared_ptr<arrow::Buffer>> buffers,
      ShuffleWriterOptions* options,
      arrow::MemoryPool* pool,
      arrow::util::Codec* codec,
      bool reuseBuffers);

  arrow::Status serialize(arrow::io::OutputStream* outputStream) override;

  static arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> deserialize(
      arrow::io::InputStream* inputStream,
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      arrow::MemoryPool* pool,
      uint32_t& numRows);

  static arrow::Result<std::pair<int32_t, uint32_t>> readTypeAndRows(arrow::io::InputStream* inputStream);

  static arrow::Result<std::shared_ptr<arrow::Buffer>> readUncompressedBuffer(arrow::io::InputStream* inputStream);

  static arrow::Result<std::shared_ptr<arrow::Buffer>> readCompressedBuffer(
      arrow::io::InputStream* inputStream,
      const std::shared_ptr<arrow::util::Codec>& codec,
      arrow::MemoryPool* pool);

  static arrow::Status mergeCompressed(
      arrow::io::InputStream* inputStream,
      arrow::io::OutputStream* outputStream,
      uint32_t numRows,
      int64_t totalLength) {
    static const Type kType = Type::kUncompressed;
    RETURN_NOT_OK(outputStream->Write(&kType, sizeof(Type)));
    RETURN_NOT_OK(outputStream->Write(&numRows, sizeof(uint32_t)));
    ARROW_ASSIGN_OR_RAISE(auto buffer, inputStream->Read(totalLength));
    RETURN_NOT_OK(outputStream->Write(buffer));
    return arrow::Status::OK();
  }

  static arrow::Status mergeUncompressed(arrow::io::InputStream* inputStream, arrow::ResizableBuffer* output) {
    ARROW_ASSIGN_OR_RAISE(auto input, readUncompressedBuffer(inputStream));
    auto data = output->mutable_data() + output->size();
    auto newSize = output->size() + input->size();
    RETURN_NOT_OK(output->Resize(newSize));
    memcpy(data, input->data(), input->size());
    return arrow::Status::OK();
  }

  static arrow::Status compressAndWrite(
      std::shared_ptr<arrow::Buffer> buffer,
      arrow::io::OutputStream* outputStream,
      arrow::util::Codec* codec,
      ShuffleMemoryPool* pool) {
    auto maxCompressedLength = codec->MaxCompressedLen(buffer->size(), buffer->data());
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::ResizableBuffer> compressed,
        arrow::AllocateResizableBuffer(sizeof(int64_t) * 2 + maxCompressedLength, pool));
    auto output = compressed->mutable_data();
    RETURN_NOT_OK(compressBuffer(buffer, output, maxCompressedLength, codec));
    RETURN_NOT_OK(outputStream->Write(compressed->data(), output - compressed->data()));
    return arrow::Status::OK();
  }

 private:
  Type type_;
  uint32_t numRows_;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers_;
};

} // namespace gluten