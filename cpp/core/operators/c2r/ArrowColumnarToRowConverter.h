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

#include "ColumnarToRow.h"

namespace gluten {

class ArrowColumnarToRowConverter final : public ColumnarToRowConverter {
 public:
  ArrowColumnarToRowConverter(std::shared_ptr<arrow::RecordBatch> rb, std::shared_ptr<arrow::MemoryPool> memory_pool)
      : ColumnarToRowConverter(memory_pool), rb_(rb) {}

  arrow::Status Init() override;

  arrow::Status Write() override;

 private:
  arrow::Status FillBuffer(
      int32_t& row_start,
      int32_t batch_rows,
      std::vector<std::vector<const uint8_t*>>& dataptrs,
      std::vector<uint8_t> nullvec,
      uint8_t* buffer_address,
      std::vector<int32_t>& offsets,
      std::vector<int32_t>& buffer_cursor,
      int32_t& num_cols,
      int32_t& num_rows,
      int32_t& nullBitsetWidthInBytes,
      std::vector<arrow::Type::type>& typevec,
      std::vector<uint8_t>& typewidth,
      std::vector<std::shared_ptr<arrow::Array>>& arrays,
      bool support_avx512);

  std::shared_ptr<arrow::RecordBatch> rb_;
};

} // namespace gluten
