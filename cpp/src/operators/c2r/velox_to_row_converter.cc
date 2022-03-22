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

#include <arrow/array/array_base.h>
#include <arrow/buffer.h>
#include <arrow/type_traits.h>

#include "arrow/c/Bridge.h"
#include "compute/type_utils.h"
#include "conversion_utils.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/row/UnsafeRowSerializer.h"

namespace gazellejni {
namespace columnartorow {

void mockSchemaRelease(ArrowSchema*) {}
void mockArrayRelease(ArrowArray*) {}

arrow::Status VeloxToRowConverter::Init() {
  num_rows_ = rb_->num_rows();
  num_cols_ = rb_->num_columns();
  schema_ = rb_->schema();
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
  for (auto col_idx = 0; col_idx < num_cols_; col_idx++) {
    auto array = rb_->column(col_idx);
    if (arrow::is_binary_like(array->type_id())) {
      auto array_data = array->data();
      const uint8_t* val = array_data->buffers[2]->data();
      auto str_view = reinterpret_cast<const StringView*>(val);
      for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
        auto length = str_view[row_idx].size();
        lengths_[row_idx] += RoundNumberOfBytesToNearestWord(length);
      }
    }
  }
  // Calculated the offsets_  and total memory size based on lengths_
  int64_t total_memory_size = lengths_[0];
  for (auto i = 1; i < num_rows_; i++) {
    offsets_[i] = offsets_[i - 1] + lengths_[i - 1];
    total_memory_size += lengths_[i];
  }
  ARROW_ASSIGN_OR_RAISE(buffer_, arrow::AllocateBuffer(total_memory_size, memory_pool_));
  memset(buffer_->mutable_data(), 0, sizeof(int8_t) * total_memory_size);
  buffer_address_ = (char*)buffer_->mutable_data();
  // The input is Arrow batch. We need to resume Velox Vector here.
  ResumeVeloxVector();
  return arrow::Status::OK();
}

void VeloxToRowConverter::ResumeVeloxVector() {
  for (int col_idx = 0; col_idx < num_cols_; col_idx++) {
    auto array = rb_->column(col_idx);
    if (arrow::is_binary_like(array->type_id())) {
      VectorPtr vec;
      // push an invalid vec for position occupation
      vecs_.push_back(vec);
      continue;
    }
    // Construct Velox's Arrow Schema and Array
    auto array_data = array->data();
    const void* data_addr = array_data->buffers[1]->data();
    const void* buffers[] = {nullptr, data_addr};
    const char* format = arrowTypeIdToFormatStr(array->type_id());
    auto arrow_schema = ArrowSchema{
        .format = format,
        .name = nullptr,
        .metadata = nullptr,
        .flags = 0,
        .n_children = 0,
        .children = nullptr,
        .dictionary = nullptr,
        .release = mockSchemaRelease,
        .private_data = nullptr,
    };
    // auto null_count = array->null_count();
    auto arrow_array = ArrowArray{
        .length = num_rows_,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 0,
        .buffers = buffers,
        .children = nullptr,
        .dictionary = nullptr,
        .release = mockArrayRelease,
        .private_data = nullptr,
    };
    VectorPtr vec = importFromArrowAsViewer(arrow_schema, arrow_array);
    // auto& pool = *velox_pool_;
    vecs_.push_back(vec);
  }
}

// TODO: handle Null value
void VeloxToRowConverter::Write() {
  for (int col_idx = 0; col_idx < num_cols_; col_idx++) {
    int64_t field_offset = GetFieldOffset(nullBitsetWidthInBytes_, col_idx);
    auto col_type_id = schema_->field(col_idx)->type()->id();
    switch (col_type_id) {
      case arrow::Int32Type::type_id: {
        // Will use Velox's conversion.
        auto vec = vecs_[col_idx];
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          auto write_address = buffer_address_ + offsets_[row_idx] + field_offset;
          auto serialized = row::UnsafeRowSerializer::serialize<IntegerType>(
              vec, write_address, row_idx);
        }
        break;
      }
      case arrow::Int64Type::type_id: {
        // Will use Velox's conversion.
        auto vec = vecs_[col_idx];
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          auto write_address = buffer_address_ + offsets_[row_idx] + field_offset;
          auto serialized = row::UnsafeRowSerializer::serialize<BigintType>(
              vec, write_address, row_idx);
        }
        break;
      }
      case arrow::DoubleType::type_id: {
        // Will use Velox's conversion.
        auto vec = vecs_[col_idx];
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          auto write_address = buffer_address_ + offsets_[row_idx] + field_offset;
          auto serialized = row::UnsafeRowSerializer::serialize<DoubleType>(
              vec, write_address, row_idx);
        }
        break;
      }
      case arrow::StringType::type_id: {
        // Will convert the faked String column into Row through memcopy.
        auto array_data = rb_->column(col_idx)->data();
        const uint8_t* val = array_data->buffers[2]->data();
        auto str_view = reinterpret_cast<const StringView*>(val);
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          int32_t length = (int32_t)str_view[row_idx].size();
          auto value = str_view[row_idx].data();
          // write the variable value
          memcpy(buffer_address_ + offsets_[row_idx] + buffer_cursor_[row_idx], value,
                 length);
          int64_t offset_and_size = (buffer_cursor_[row_idx] << 32) | length;
          // write the offset and size
          memcpy(buffer_address_ + offsets_[row_idx] + field_offset, &offset_and_size,
                 sizeof(int64_t));
          buffer_cursor_[row_idx] += length;
        }
        break;
      }
      default:
        throw std::runtime_error("Type is not supported in VeloxToRow conversion.");
    }
  }
}

}  // namespace columnartorow
}  // namespace gazellejni
