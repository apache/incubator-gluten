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

#include "ShuffleReader.h"
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
    if (options.compression_type != arrow::Compression::UNCOMPRESSED) {
      writeSchema_ = toCompressWriteSchema(*schema);
    } else {
      writeSchema_ = toWriteSchema(*schema);
    }
  }

  std::shared_ptr<ColumnarBatch> next() override {
    std::shared_ptr<arrow::RecordBatch> arrowBatch;
    std::unique_ptr<arrow::ipc::Message> messageToRead;
    GLUTEN_ASSIGN_OR_THROW(messageToRead, arrow::ipc::ReadMessage(in_.get()))
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
};
} // namespace

namespace gluten {

ReaderOptions ReaderOptions::defaults() {
  return {};
}

ShuffleReader::ShuffleReader(
    std::shared_ptr<arrow::Schema> schema,
    ReaderOptions options,
    std::shared_ptr<arrow::MemoryPool> pool)
    : pool_(pool), options_(std::move(options)), schema_(schema) {}

std::shared_ptr<ResultIterator> ShuffleReader::readStream(std::shared_ptr<arrow::io::InputStream> in) {
  return std::make_shared<ResultIterator>(std::make_unique<ShuffleReaderOutStream>(schema_, in, options_));
}

arrow::Status ShuffleReader::close() {
  return arrow::Status::OK();
}

int64_t ShuffleReader::getDecompressTime() {
  return decompressTime_;
}
const std::shared_ptr<arrow::MemoryPool>& ShuffleReader::getPool() const {
  return pool_;
}

} // namespace gluten
