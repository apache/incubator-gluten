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

// This File includes common helper functions with Arrow dependency.

#pragma once

#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>
#include <sys/mman.h>
#include <numeric>
#include "memory/ArrowMemoryPool.h"
#include "memory/ColumnarBatch.h"
#include "utils/macros.h"
#include "velox/type/Type.h"

#include <velox/common/memory/MemoryPool.h>
#include <iostream>

namespace gluten {

void toArrowSchema(
    const facebook::velox::TypePtr& rowType,
    facebook::velox::memory::MemoryPool* pool,
    struct ArrowSchema* out);

std::shared_ptr<arrow::Schema> toArrowSchema(
    const facebook::velox::TypePtr& rowType,
    facebook::velox::memory::MemoryPool* pool);

facebook::velox::TypePtr fromArrowSchema(const std::shared_ptr<arrow::Schema>& schema);

/**
 * For testing.
 */
arrow::Result<std::shared_ptr<ColumnarBatch>> recordBatch2VeloxColumnarBatch(const arrow::RecordBatch& rb);

/**
 * arrow::MemoryPool instance used by tests and benchmarks
 */
class MyMemoryPool final : public arrow::MemoryPool {
 public:
  explicit MyMemoryPool() : capacity_(std::numeric_limits<int64_t>::max()) {}
  explicit MyMemoryPool(int64_t capacity) : capacity_(capacity) {}

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;

  arrow::Status Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) override;

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;

  int64_t bytes_allocated() const override;

  int64_t max_memory() const override;

  std::string backend_name() const override;

  int64_t total_bytes_allocated() const override;

  int64_t num_allocations() const override;

 private:
  arrow::MemoryPool* pool_ = arrow::default_memory_pool();
  int64_t capacity_;
  arrow::internal::MemoryPoolStats stats_;
};

} // namespace gluten
