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
  ArrowColumnarToRowConverter(std::shared_ptr<arrow::RecordBatch> rb, std::shared_ptr<arrow::MemoryPool> memoryPool)
      : ColumnarToRowConverter(memoryPool), rb_(rb) {}

  arrow::Status init() override;

  arrow::Status write() override;

 private:
  arrow::Status fillBuffer(
      int32_t& rowStart,
      int32_t batchRows,
      std::vector<std::vector<const uint8_t*>>& dataptrs,
      std::vector<uint8_t> nullvec,
      uint8_t* bufferAddress,
      std::vector<int32_t>& offsets,
      std::vector<int32_t>& bufferCursor,
      int32_t& numCols,
      int32_t& numRows,
      int32_t& nullBitsetWidthInBytes,
      std::vector<arrow::Type::type>& typevec,
      std::vector<uint8_t>& typewidth,
      std::vector<std::shared_ptr<arrow::Array>>& arrays,
      bool supportAvx512);

  std::shared_ptr<arrow::RecordBatch> rb_;
};

} // namespace gluten
