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

#include "operators/reader/FileReaderIterator.h"

#include <parquet/arrow/reader.h>
#include <memory>

namespace gluten {

class ParquetReaderIterator : public FileReaderIterator {
 public:
  explicit ParquetReaderIterator(const std::string& path, int64_t batchSize, facebook::velox::memory::MemoryPool* pool);

  void createReader();

  std::shared_ptr<arrow::Schema> getSchema() override;

 protected:
  std::unique_ptr<::parquet::arrow::FileReader> fileReader_;
  std::shared_ptr<arrow::RecordBatchReader> recordBatchReader_;
  int64_t batchSize_;
  facebook::velox::memory::MemoryPool* pool_;
};

class ParquetStreamReaderIterator final : public ParquetReaderIterator {
 public:
  ParquetStreamReaderIterator(const std::string& path, int64_t batchSize, facebook::velox::memory::MemoryPool* pool);

  std::shared_ptr<gluten::ColumnarBatch> next() override;
};

class ParquetBufferedReaderIterator final : public ParquetReaderIterator {
 public:
  explicit ParquetBufferedReaderIterator(
      const std::string& path,
      int64_t batchSize,
      facebook::velox::memory::MemoryPool* pool);

  std::shared_ptr<gluten::ColumnarBatch> next() override;

 private:
  void collectBatches();

  arrow::RecordBatchVector batches_;
  std::vector<std::shared_ptr<arrow::RecordBatch>>::const_iterator iter_;
};

} // namespace gluten
