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
#include <arrow/type.h>

#include "operators/c2r/ArrowColumnarToRowConverter.h"
#include "operators/c2r/ColumnarToRow.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

class VeloxColumnarToRowConverter final : public ColumnarToRowConverter {
 public:
  VeloxColumnarToRowConverter(
      std::shared_ptr<arrow::MemoryPool> arrow_pool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> velox_pool)
      : ColumnarToRowConverter(arrow_pool), velox_pool_(velox_pool) {
    }

  VeloxColumnarToRowConverter(
      const facebook::velox::RowVectorPtr& rv,
      std::shared_ptr<arrow::MemoryPool> arrow_pool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> velox_pool)
      : rv_(rv), ColumnarToRowConverter(arrow_pool), velox_pool_(velox_pool) {}

  ~VeloxColumnarToRowConverter() = default;

  arrow::Status Init() override;

  arrow::Status Init(const facebook::velox::RowVectorPtr& rv)
  {
    rv_ = rv;
    return Init();
  }

  arrow::Status FillBuffer(
      int32_t& row_start,
      int32_t batch_rows,
      std::vector<const uint8_t*>& dataptrs,
      std::vector<uint8_t> nullvec,
      std::vector<arrow::Type::type>& typevec,
      std::vector<uint8_t>& typewidth);

  arrow::Status Write() override;

 private:
  void ResumeVeloxVector();

  facebook::velox::RowVectorPtr rv_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> velox_pool_;
  std::vector<facebook::velox::VectorPtr> vecs_;
  std::shared_ptr<arrow::Schema> schema_;
};

} // namespace gluten
