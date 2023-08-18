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

#include "reader.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "utils/macros.h"

#include <utility>

#include "ShuffleSchema.h"

namespace {
using namespace gluten;

class ShuffleReaderOutStream : public ColumnarBatchIterator {
 public:
  ShuffleReaderOutStream(
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::io::InputStream>& in,
      const ReaderOptions& options)
      : options_(options), in_(in) {
    GLUTEN_ASSIGN_OR_THROW(firstMessage_, arrow::ipc::ReadMessage(in_.get()))
    if (firstMessage_ == nullptr) {
      throw GlutenException("Failed to read message from shuffle.");
    }
    if (firstMessage_->type() == arrow::ipc::MessageType::SCHEMA) {
      GLUTEN_ASSIGN_OR_THROW(writeSchema_, arrow::ipc::ReadSchema(*firstMessage_, nullptr))
      firstMessageConsumed_ = true;
    } else {
      if (options.compression_type != arrow::Compression::UNCOMPRESSED) {
        writeSchema_ = toCompressWriteSchema(*schema);
      } else {
        writeSchema_ = toWriteSchema(*schema);
      }
    }
  }

  std::shared_ptr<ColumnarBatch> next() override {
    std::shared_ptr<arrow::RecordBatch> arrowBatch;
    std::unique_ptr<arrow::ipc::Message> messageToRead;
    if (!firstMessageConsumed_) {
      messageToRead = std::move(firstMessage_);
      firstMessageConsumed_ = true;
    } else {
      GLUTEN_ASSIGN_OR_THROW(messageToRead, arrow::ipc::ReadMessage(in_.get()))
    }
    if (messageToRead == nullptr) {
      return nullptr;
    }

    GLUTEN_ASSIGN_OR_THROW(
        arrowBatch, arrow::ipc::ReadRecordBatch(*messageToRead, writeSchema_, nullptr, options_.ipc_read_options))
    std::shared_ptr<ColumnarBatch> glutenBatch = std::make_shared<ArrowColumnarBatch>(arrowBatch);
    return glutenBatch;
  }

 private:
  ReaderOptions options_;
  std::shared_ptr<arrow::io::InputStream> in_;
  std::shared_ptr<arrow::Schema> writeSchema_;
  // TODO it's not reliable to infer schema from the first message, should finally drop this code
  std::unique_ptr<arrow::ipc::Message> firstMessage_;
  bool firstMessageConsumed_ = false;
};
} // namespace

namespace gluten {

ReaderOptions ReaderOptions::defaults() {
  return {};
}

Reader::Reader(std::shared_ptr<arrow::Schema> schema, ReaderOptions options, std::shared_ptr<arrow::MemoryPool> pool)
    : pool_(pool), options_(std::move(options)), schema_(schema) {}

std::shared_ptr<ResultIterator> Reader::readStream(std::shared_ptr<arrow::io::InputStream> in) {
  return std::make_shared<ResultIterator>(std::make_unique<ShuffleReaderOutStream>(schema_, in, options_));
}

arrow::Status Reader::close() {
  return arrow::Status::OK();
}

int64_t Reader::getDecompressTime() {
  return decompressTime_;
}
const std::shared_ptr<arrow::MemoryPool>& Reader::getPool() const {
  return pool_;
}

} // namespace gluten
