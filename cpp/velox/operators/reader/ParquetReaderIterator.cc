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

#include "operators/reader/ParquetReaderIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "utils/Timer.h"

namespace gluten {
namespace {
std::unique_ptr<facebook::velox::parquet::ParquetReader> createReader(
    const std::string& path,
    const facebook::velox::dwio::common::ReaderOptions& opts) {
  auto input = std::make_unique<facebook::velox::dwio::common::BufferedInput>(
      std::make_shared<facebook::velox::LocalReadFile>(path), opts.memoryPool());
  return std::make_unique<facebook::velox::parquet::ParquetReader>(std::move(input), opts);
}

std::shared_ptr<facebook::velox::common::ScanSpec> makeScanSpec(const facebook::velox::RowTypePtr& rowType) {
  auto scanSpec = std::make_shared<facebook::velox::common::ScanSpec>("");
  scanSpec->addAllChildFields(*rowType);
  return scanSpec;
}
} // namespace

ParquetReaderIterator::ParquetReaderIterator(
    const std::string& path,
    int64_t batchSize,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool)
    : FileReaderIterator(path), pool_(std::move(pool)), batchSize_(batchSize) {}

void ParquetReaderIterator::createRowReader() {
  facebook::velox::dwio::common::ReaderOptions readerOptions{pool_.get()};
  auto reader = createReader(path_, readerOptions);

  rowType_ = reader->rowType();

  facebook::velox::dwio::common::RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(std::make_shared<facebook::velox::dwio::common::ColumnSelector>(rowType_, rowType_->names()));
  rowReaderOpts.setScanSpec(makeScanSpec(rowType_));

  rowReader_ = reader->createRowReader(rowReaderOpts);

  DLOG(INFO) << "Opened file for read: " << path_;
}

ParquetStreamReaderIterator::ParquetStreamReaderIterator(
    const std::string& path,
    int64_t batchSize,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool)
    : ParquetReaderIterator(path, batchSize, std::move(pool)) {
  createRowReader();
}

std::shared_ptr<gluten::ColumnarBatch> ParquetStreamReaderIterator::next() {
  ScopedTimer timer(&collectBatchTime_);

  static constexpr int32_t kBatchSize = 4096;

  auto result = facebook::velox::BaseVector::create(rowType_, kBatchSize, pool_.get());
  auto numRows = rowReader_->next(kBatchSize, result);

  if (numRows == 0) {
    return nullptr;
  }

  // Load lazy vector.
  result = facebook::velox::BaseVector::loadedVectorShared(result);

  auto rowVector = std::dynamic_pointer_cast<facebook::velox::RowVector>(result);
  GLUTEN_DCHECK(rowVector != nullptr, "Error casting to RowVector");

  DLOG(INFO) << "ParquetStreamReaderIterator read rows: " << numRows;

  return std::make_shared<VeloxColumnarBatch>(std::move(rowVector));
}

ParquetBufferedReaderIterator::ParquetBufferedReaderIterator(
    const std::string& path,
    int64_t batchSize,
    std::shared_ptr<facebook::velox::memory::MemoryPool> pool)
    : ParquetReaderIterator(path, batchSize, std::move(pool)) {
  createRowReader();
  collectBatches();
}

std::shared_ptr<gluten::ColumnarBatch> ParquetBufferedReaderIterator::next() {
  if (iter_ == batches_.cend()) {
    return nullptr;
  }
  return *iter_++;
}

void ParquetBufferedReaderIterator::collectBatches() {
  ScopedTimer timer(&collectBatchTime_);

  static constexpr int32_t kBatchSize = 4096;

  uint64_t numRows = 0;
  while (true) {
    auto result = facebook::velox::BaseVector::create(rowType_, kBatchSize, pool_.get());
    numRows = rowReader_->next(kBatchSize, result);
    if (numRows == 0) {
      break;
    }

    // Load lazy vector.
    result = facebook::velox::BaseVector::loadedVectorShared(result);

    auto rowVector = std::dynamic_pointer_cast<facebook::velox::RowVector>(result);
    GLUTEN_DCHECK(rowVector != nullptr, "Error casting to RowVector");

    DLOG(INFO) << "ParquetStreamReaderIterator read rows: " << numRows;

    batches_.push_back(std::make_shared<VeloxColumnarBatch>(std::move(rowVector)));
  }

  iter_ = batches_.begin();

  DLOG(INFO) << "Number of input batches: " << std::to_string(batches_.size());
}

} // namespace gluten
