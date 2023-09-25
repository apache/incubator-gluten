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

class ParquetBatchStreamIterator final : public ParquetBatchIterator {
 public:
  explicit ParquetBatchStreamIterator(const std::string& path) : ParquetBatchIterator(path) {
    createReader();
    DEBUG_OUT << "ParquetBatchStreamIterator open file: " << path << std::endl;
  }

  std::shared_ptr<gluten::ColumnarBatch> next() override {
    auto startTime = std::chrono::steady_clock::now();
    GLUTEN_ASSIGN_OR_THROW(auto batch, recordBatchReader_->Next());
    DEBUG_OUT << "ParquetBatchStreamIterator get a batch, num rows: " << (batch ? batch->num_rows() : 0) << std::endl;
    collectBatchTime_ +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - startTime).count();
    if (batch == nullptr) {
      return nullptr;
    }
    return convertBatch(std::make_shared<gluten::ArrowColumnarBatch>(batch));
  }
};

inline std::shared_ptr<gluten::ResultIterator> getParquetInputFromBatchStream(const std::string& path) {
  return std::make_shared<gluten::ResultIterator>(std::make_unique<ParquetBatchStreamIterator>(path));
}

class OrcBatchStreamIterator final : public OrcBatchIterator {
 public:
  explicit OrcBatchStreamIterator(const std::string& path) : OrcBatchIterator(path) {
    createReader();
  }

  std::shared_ptr<gluten::ColumnarBatch> next() override {
    auto startTime = std::chrono::steady_clock::now();
    GLUTEN_ASSIGN_OR_THROW(auto batch, recordBatchReader_->Next());
    DEBUG_OUT << "OrcBatchStreamIterator get a batch, num rows: " << (batch ? batch->num_rows() : 0) << std::endl;
    collectBatchTime_ +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - startTime).count();
    if (batch == nullptr) {
      return nullptr;
    }
    return convertBatch(std::make_shared<gluten::ArrowColumnarBatch>(batch));
  }
};

inline std::shared_ptr<gluten::ResultIterator> getOrcInputFromBatchStream(const std::string& path) {
  return std::make_shared<gluten::ResultIterator>(std::make_unique<OrcBatchStreamIterator>(path));
}

} // namespace gluten
