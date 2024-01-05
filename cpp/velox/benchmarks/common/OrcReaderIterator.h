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

#include <arrow/adapters/orc/adapter.h>
#include "benchmarks/common/FileReaderIterator.h"

namespace gluten {

class OrcReaderIterator : public FileReaderIterator {
 public:
  explicit OrcReaderIterator(const std::string& path) : FileReaderIterator(path) {}

  void createReader() override {
    // Open File
    auto input = arrow::io::ReadableFile::Open(path_);
    GLUTEN_THROW_NOT_OK(input);

    // Open ORC File Reader
    auto maybeReader = arrow::adapters::orc::ORCFileReader::Open(*input, arrow::default_memory_pool());
    GLUTEN_THROW_NOT_OK(maybeReader);
    fileReader_.reset((*maybeReader).release());

    // get record batch Reader
    auto recordBatchReader = fileReader_->GetRecordBatchReader(4096, std::vector<std::string>());
    GLUTEN_THROW_NOT_OK(recordBatchReader);
    recordBatchReader_ = *recordBatchReader;
  }

  std::shared_ptr<arrow::Schema> getSchema() override {
    auto schema = fileReader_->ReadSchema();
    GLUTEN_THROW_NOT_OK(schema);
    return *schema;
  }

 protected:
  std::unique_ptr<arrow::adapters::orc::ORCFileReader> fileReader_;
  std::shared_ptr<arrow::RecordBatchReader> recordBatchReader_;
};

class OrcStreamReaderIterator final : public OrcReaderIterator {
 public:
  explicit OrcStreamReaderIterator(const std::string& path) : OrcReaderIterator(path) {
    createReader();
  }

  std::shared_ptr<gluten::ColumnarBatch> next() override {
    auto startTime = std::chrono::steady_clock::now();
    GLUTEN_ASSIGN_OR_THROW(auto batch, recordBatchReader_->Next());
    DLOG(INFO) << "OrcStreamReaderIterator get a batch, num rows: " << (batch ? batch->num_rows() : 0);
    collectBatchTime_ +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - startTime).count();
    if (batch == nullptr) {
      return nullptr;
    }
    return convertBatch(std::make_shared<gluten::ArrowColumnarBatch>(batch));
  }
};

class OrcBufferedReaderIterator final : public OrcReaderIterator {
 public:
  explicit OrcBufferedReaderIterator(const std::string& path) : OrcReaderIterator(path) {
    createReader();
    collectBatches();
    iter_ = batches_.begin();
    DLOG(INFO) << "OrcBufferedReaderIterator open file: " << path;
    DLOG(INFO) << "Number of input batches: " << std::to_string(batches_.size());
    if (iter_ != batches_.cend()) {
      DLOG(INFO) << "columns: " << (*iter_)->num_columns();
      DLOG(INFO) << "rows: " << (*iter_)->num_rows();
    }
  }

  std::shared_ptr<gluten::ColumnarBatch> next() override {
    if (iter_ == batches_.cend()) {
      return nullptr;
    }
    return convertBatch(std::make_shared<gluten::ArrowColumnarBatch>(*iter_++));
  }

 private:
  void collectBatches() {
    auto startTime = std::chrono::steady_clock::now();
    GLUTEN_ASSIGN_OR_THROW(batches_, recordBatchReader_->ToRecordBatches());
    auto endTime = std::chrono::steady_clock::now();
    collectBatchTime_ += std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
  }

  arrow::RecordBatchVector batches_;
  std::vector<std::shared_ptr<arrow::RecordBatch>>::const_iterator iter_;
};

} // namespace gluten