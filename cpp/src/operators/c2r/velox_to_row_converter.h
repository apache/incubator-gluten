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

#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "velox/vector/ComplexVector.h"

using namespace facebook::velox;

namespace gazellejni {
namespace columnartorow {

class VeloxToRowConverter {
 public:
  VeloxToRowConverter(const std::shared_ptr<arrow::RecordBatch>& rb,
                      arrow::MemoryPool* memory_pool)
      : rb_(rb), memory_pool_(memory_pool) {}

  arrow::Status Init();
  void Write();

  char* GetBufferAddress() { return buffer_address_; }
  const std::vector<int64_t>& GetOffsets() { return offsets_; }
  const std::vector<int64_t>& GetLengths() { return lengths_; }

 private:
  // RowVectorPtr rv_;
  std::vector<VectorPtr> vecs_;
  std::shared_ptr<arrow::RecordBatch> rb_;
  std::shared_ptr<arrow::Buffer> buffer_;
  char* buffer_address_;
  arrow::MemoryPool* memory_pool_ = arrow::default_memory_pool();
  std::unique_ptr<memory::MemoryPool> velox_pool_{memory::getDefaultScopedMemoryPool()};
  std::vector<int64_t> offsets_;
  std::vector<int64_t> lengths_;
  int64_t nullBitsetWidthInBytes_;
  int64_t num_cols_;
  int64_t num_rows_;
};

}  // namespace columnartorow
}  // namespace gazellejni
