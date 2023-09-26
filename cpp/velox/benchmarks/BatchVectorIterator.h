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

#include "BatchIteratorWrapper.h"

namespace gluten {

class ParquetBatchVectorIterator final : public ParquetBatchIterator {
 public:
  explicit ParquetBatchVectorIterator(const std::string& path) : ParquetBatchIterator(path) {
    createReader();
    collectBatches();

    iter_ = batches_.begin();
    DEBUG_OUT << "ParquetBatchVectorIterator open file: " << path << std::endl;
    DEBUG_OUT << "Number of input batches: " << std::to_string(batches_.size()) << std::endl;
    if (iter_ != batches_.cend()) {
      DEBUG_OUT << "columns: " << (*iter_)->num_columns() << std::endl;
      DEBUG_OUT << "rows: " << (*iter_)->num_rows() << std::endl;
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

inline std::shared_ptr<gluten::ResultIterator> getParquetInputFromBatchVector(const std::string& path) {
  return std::make_shared<gluten::ResultIterator>(std::make_unique<ParquetBatchVectorIterator>(path));
}

class OrcBatchVectorIterator final : public OrcBatchIterator {
 public:
  explicit OrcBatchVectorIterator(const std::string& path) : OrcBatchIterator(path) {
    createReader();
    collectBatches();

    iter_ = batches_.begin();
#ifdef GLUTEN_PRINT_DEBUG
    DEBUG_OUT << "OrcBatchVectorIterator open file: " << path << std::endl;
    DEBUG_OUT << "Number of input batches: " << std::to_string(batches_.size()) << std::endl;
    if (iter_ != batches_.cend()) {
      DEBUG_OUT << "columns: " << (*iter_)->num_columns() << std::endl;
      DEBUG_OUT << "rows: " << (*iter_)->num_rows() << std::endl;
    }
#endif
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

inline std::shared_ptr<gluten::ResultIterator> getOrcInputFromBatchVector(const std::string& path) {
  return std::make_shared<gluten::ResultIterator>(std::make_unique<OrcBatchVectorIterator>(path));
}

} // namespace gluten
