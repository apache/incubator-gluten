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

#include "VeloxToRowConverter.h"

#include <arrow/array/array_base.h>
#include <arrow/buffer.h>
#include <arrow/type_traits.h>

#include "ArrowTypeUtils.h"
#include "arrow/c/Bridge.h"
#include "arrow/c/bridge.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/row/UnsafeRowSerializer.h"

namespace velox {
namespace compute {

arrow::Status VeloxToRowConverter::Init() {
  num_rows_ = rb_->num_rows();
  num_cols_ = rb_->num_columns();
  schema_ = rb_->schema();
  // The input is Arrow batch. We need to resume Velox Vector here.
  ResumeVeloxVector();
  // Calculate the initial size
  nullBitsetWidthInBytes_ = CalculateBitSetWidthInBytes(num_cols_);
  int64_t fixed_size_per_row = CalculatedFixeSizePerRow(schema_, num_cols_);
  // Initialize the offsets_ , lengths_, buffer_cursor_
  for (auto i = 0; i < num_rows_; i++) {
    lengths_.push_back(fixed_size_per_row);
    offsets_.push_back(0);
    buffer_cursor_.push_back(nullBitsetWidthInBytes_ + 8 * num_cols_);
  }
  // Calculated the lengths_
  for (int64_t col_idx = 0; col_idx < num_cols_; col_idx++) {
    auto array = rb_->column(col_idx);
    if (arrow::is_binary_like(array->type_id())) {
      auto str_views = vecs_[col_idx]->asFlatVector<StringView>()->rawValues();
      for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
        auto length = str_views[row_idx].size();
        int64_t bytes = RoundNumberOfBytesToNearestWord(length);
        lengths_[row_idx] += bytes;
      }
    }
  }
  // Calculated the offsets_  and total memory size based on lengths_
  int64_t total_memory_size = lengths_[0];
  for (int64_t rowIdx = 1; rowIdx < num_rows_; rowIdx++) {
    offsets_[rowIdx] = offsets_[rowIdx - 1] + lengths_[rowIdx - 1];
    total_memory_size += lengths_[rowIdx];
  }
  ARROW_ASSIGN_OR_RAISE(buffer_, arrow::AllocateBuffer(total_memory_size, memory_pool_));
  memset(buffer_->mutable_data(), 0, sizeof(int8_t) * total_memory_size);
  buffer_address_ = buffer_->mutable_data();
  return arrow::Status::OK();
}

void VeloxToRowConverter::ResumeVeloxVector() {
  for (int col_idx = 0; col_idx < num_cols_; col_idx++) {
    auto array = rb_->column(col_idx);
    ArrowArray c_array{};
    ArrowSchema c_schema{};
    arrow::Status status = arrow::ExportArray(*array, &c_array, &c_schema);
    if (!status.ok()) {
      throw std::runtime_error("Failed to export from Arrow record batch");
    }
    VectorPtr vec = importFromArrowAsViewer(c_schema, c_array);
    // auto& pool = *velox_pool_;
    vecs_.push_back(vec);
  }
}

// TODO: handle Null value
arrow::Status VeloxToRowConverter::Write() {
  for (int col_idx = 0; col_idx < num_cols_; col_idx++) {
    int64_t field_offset = GetFieldOffset(nullBitsetWidthInBytes_, col_idx);
    auto col_type_id = schema_->field(col_idx)->type()->id();
    switch (col_type_id) {
      case arrow::Int32Type::type_id: {
        // Will use Velox's conversion.
        auto vec = vecs_[col_idx];
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          auto write_address =
              (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
          auto serialized = row::UnsafeRowSerializer::serialize<IntegerType>(
              vec, write_address, row_idx);
        }
        break;
      }
      case arrow::Int64Type::type_id: {
        // Will use Velox's conversion.
        auto vec = vecs_[col_idx];
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          auto write_address =
              (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
          auto serialized = row::UnsafeRowSerializer::serialize<BigintType>(
              vec, write_address, row_idx);
        }
        break;
      }
      case arrow::DoubleType::type_id: {
        // Will use Velox's conversion.
        auto vec = vecs_[col_idx];
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          auto write_address =
              (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
          auto serialized = row::UnsafeRowSerializer::serialize<DoubleType>(
              vec, write_address, row_idx);
        }
        break;
      }
      case arrow::StringType::type_id: {
        // Will convert the faked String column into Row through memcopy.
        auto str_views = vecs_[col_idx]->asFlatVector<StringView>()->rawValues();
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          int32_t length = (int32_t)str_views[row_idx].size();
          auto value = str_views[row_idx].data();
          // Write the variable value.
          memcpy(buffer_address_ + offsets_[row_idx] + buffer_cursor_[row_idx], value,
                 length);
          int64_t offset_and_size = (buffer_cursor_[row_idx] << 32) | length;
          // Write the offset and size.
          memcpy(buffer_address_ + offsets_[row_idx] + field_offset, &offset_and_size,
                 sizeof(int64_t));
          buffer_cursor_[row_idx] += length;
        }
        break;
      }
      default:
        return arrow::Status::Invalid("Type is not supported in VeloxToRow conversion.");
    }
  }
  return arrow::Status::OK();
}

}  // namespace compute
}  // namespace velox
