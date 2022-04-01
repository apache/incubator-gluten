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

#include "columnar_to_row_base.h"

namespace gazellejni {
namespace columnartorow {

class ArrowColumnarToRowConverter : public ColumnarToRowConverterBase {
 public:
  ArrowColumnarToRowConverter(std::shared_ptr<arrow::RecordBatch> rb,
                              arrow::MemoryPool* memory_pool)
      : ColumnarToRowConverterBase(rb, memory_pool) {}

  arrow::Status Init() override;

  arrow::Status Write() override;

 private:
  arrow::Status WriteValue(uint8_t* buffer_address, int64_t field_offset,
                           std::shared_ptr<arrow::Array> array, int32_t col_index,
                           int64_t num_rows, std::vector<int64_t>& offsets,
                           std::vector<int64_t>& buffer_cursor);
};

}  // namespace columnartorow
}  // namespace gazellejni
