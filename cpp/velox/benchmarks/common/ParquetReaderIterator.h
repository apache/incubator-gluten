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

#include "benchmarks/common/BenchmarkUtils.h"
#include "benchmarks/common/FileReaderIterator.h"
#include "utils/macros.h"

#include <parquet/arrow/reader.h>

namespace gluten {

class ParquetReaderIterator : public FileReaderIterator {
 public:
  explicit ParquetReaderIterator(const std::string& path) : FileReaderIterator(path) {}

  void createReader() override {
    parquet::ArrowReaderProperties properties = parquet::default_arrow_reader_properties();
    properties.set_batch_size(FLAGS_batch_size);
    GLUTEN_THROW_NOT_OK(parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), parquet::ParquetFileReader::OpenFile(path_), properties, &fileReader_));
    GLUTEN_THROW_NOT_OK(
        fileReader_->GetRecordBatchReader(arrow::internal::Iota(fileReader_->num_row_groups()), &recordBatchReader_));

    auto schema = recordBatchReader_->schema();
    LOG(INFO) << "schema:\n" << schema->ToString();
  }

  std::shared_ptr<arrow::Schema> getSchema() override {
    return recordBatchReader_->schema();
  }

 protected:
  std::unique_ptr<parquet::arrow::FileReader> fileReader_;
  std::shared_ptr<arrow::RecordBatchReader> recordBatchReader_;
};

class ParquetStreamReaderIterator final : public ParquetReaderIterator {
 public:
  explicit ParquetStreamReaderIterator(const std::string& path) : ParquetReaderIterator(path) {
    createReader();
    DLOG(INFO) << "ParquetStreamReaderIterator open file: " << path;
  }

  std::shared_ptr<gluten::ColumnarBatch> next() override {
    auto startTime = std::chrono::steady_clock::now();
    GLUTEN_ASSIGN_OR_THROW(auto batch, recordBatchReader_->Next());
    DLOG(INFO) << "ParquetStreamReaderIterator get a batch, num rows: " << (batch ? batch->num_rows() : 0);
    collectBatchTime_ +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - startTime).count();
    if (batch == nullptr) {
      return nullptr;
    }
    return convertBatch(std::make_shared<gluten::ArrowColumnarBatch>(batch));
  }
};

class ParquetBufferedReaderIterator final : public ParquetReaderIterator {
 public:
  explicit ParquetBufferedReaderIterator(const std::string& path) : ParquetReaderIterator(path) {
    createReader();
    collectBatches();
    iter_ = batches_.begin();
    DLOG(INFO) << "ParquetBufferedReaderIterator open file: " << path;
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
