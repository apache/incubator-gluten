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

#include "velox_to_row_converter.h"

#include "conversion_utils.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/row/UnsafeRowSerializer.h"

namespace gazellejni {
namespace columnartorow {

VeloxToRowConverter::VeloxToRowConverter(const std::shared_ptr<arrow::Schema>& schema)
    : schema_(schema) {}

void VeloxToRowConverter::Init() {
  num_rows_ = rb_->num_rows();
  num_cols_ = rb_->num_columns();
  // Calculate the initial size
  nullBitsetWidthInBytes_ = CalculateBitSetWidthInBytes(num_cols_);
  int64_t fixed_size_per_row = CalculatedFixeSizePerRow(schema_, num_cols_);
  // Initialize the offsets_ , lengths_, buffer_cursor_
  for (auto i = 0; i < num_rows_; i++) {
    lengths_.push_back(fixed_size_per_row);
    offsets_.push_back(0);
  }
  // Calculated the lengths_
  // for (auto i = 0; i < num_cols_; i++) {
  //   auto array = rb_->column(i);
  //   if (arrow::is_binary_like(array->type_id())) {
  //     auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
  //     using offset_type = typename arrow::BinaryType::offset_type;
  //     offset_type length;
  //     for (auto j = 0; j < num_rows_; j++) {
  //       auto value = binary_array->GetValue(j, &length);
  //       lengths_[j] += RoundNumberOfBytesToNearestWord(length);
  //     }
  //   }
  // }
  // Calculated the offsets_  and total memory size based on lengths_
  int64_t total_memory_size = lengths_[0];
  for (auto i = 1; i < num_rows_; i++) {
    offsets_[i] = offsets_[i - 1] + lengths_[i - 1];
    total_memory_size += lengths_[i];
  }
  ARROW_ASSIGN_OR_RAISE(buffer_, AllocateBuffer(total_memory_size, memory_pool_));
  memset(buffer_->mutable_data(), 0, sizeof(int8_t) * total_memory_size);
  buffer_address_ = (char*)buffer_->mutable_data();
  // The input is fake Arrow batch. We need to resume Velox Vector here.
  for (int col_idx; col_idx < num_cols_; col_idx++) {
    auto array = rb_->column(col_idx);
    uint8_t* data_addr = array->data()->buffers[1]->data();
    BufferPtr nulls = nullptr;
    BufferPtr values = BufferPtr(data_addr, 8 * num_rows_);
    auto flatVector = std::make_shared<FlatVector<double>>(
        velox_pool_, nulls, num_rows_, values, std::vector<BufferPtr>());
    vecs_.push_back(flatVector);
  }
}

void VeloxToRowConverter::Write() {
  for (int col_idx; col_idx < num_cols_; col_idx++) {
    auto vec = vecs_(col_idx);
    auto num_rows = vec->size();
    for (int row_idx; row_idx < num_rows; row_idx++) {
      int64_t field_offset = GetFieldOffset(nullBitsetWidthInBytes_, row_idx);
      auto value_address = buffer_address_ + offsets[row_idx] + field_offset;
      auto serialized =
          UnsafeRowSerializer::serialize<DoubleType>(vec, value_address, row_idx);
    }
  }
}

}  // namespace columnartorow
}  // namespace gazellejni
