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

#include "operators/writer/VeloxColumnarBatchWriter.h"

#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/writer/Writer.h"

namespace gluten {

VeloxColumnarBatchWriter::VeloxColumnarBatchWriter(
    const std::string& path,
    int64_t batchSize,
    facebook::velox::memory::MemoryPool* pool)
    : path_(path), batchSize_(batchSize), pool_(pool) {}

arrow::Status VeloxColumnarBatchWriter::initWriter(const facebook::velox::RowTypePtr& rowType) {
  auto localWriteFile = std::make_unique<facebook::velox::LocalWriteFile>(path_, false, true);
  auto sink = std::make_unique<facebook::velox::dwio::common::WriteFileSink>(std::move(localWriteFile), path_);

  facebook::velox::parquet::WriterOptions writerOptions;
  writerOptions.memoryPool = pool_;
  writerOptions.compressionKind = facebook::velox::common::CompressionKind::CompressionKind_SNAPPY;
  writerOptions.batchSize = batchSize_;

  writer_ = std::make_unique<facebook::velox::parquet::Writer>(std::move(sink), writerOptions, rowType);
  return arrow::Status::OK();
}

arrow::Status VeloxColumnarBatchWriter::write(const std::shared_ptr<ColumnarBatch>& batch) {
  auto rowVector = VeloxColumnarBatch::from(pool_, batch)->getRowVector();
  if (!writer_) {
    RETURN_NOT_OK(initWriter(facebook::velox::asRowType(rowVector->type())));
  }

  writer_->write(rowVector);
  return arrow::Status::OK();
}

arrow::Status VeloxColumnarBatchWriter::close() {
  writer_->close();
  closed_ = true;
  return arrow::Status::OK();
}

std::shared_ptr<ColumnarBatchIterator> VeloxColumnarBatchWriter::retrieveIterator() {
  GLUTEN_CHECK(writer_ != nullptr, "Writer is not initialized");
  GLUTEN_CHECK(closed_, "Writer is not closed");

  return std::make_shared<ParquetStreamReaderIterator>(path_, batchSize_, pool_);
}

} // namespace gluten
