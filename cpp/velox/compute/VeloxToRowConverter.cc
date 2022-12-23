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
#include "arrow/c/helpers.h"
#include "include/arrow/c/bridge.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/row/UnsafeRowSerializer.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox;

namespace gluten {

arrow::Status VeloxToRowConverter::Init() {
  num_rows_ = rv_->size();
  num_cols_ = rv_->childrenSize();
  ArrowSchema c_schema{};
  facebook::velox::exportToArrow(rv_, c_schema);
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, arrow::ImportSchema(&c_schema));
  if (num_cols_ != schema->num_fields()) {
    return arrow::Status::Invalid("Mismatch: num_cols_ != schema->num_fields()");
  }
  schema_ = schema;
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
    std::shared_ptr<arrow::Field> field = schema_->field(col_idx);
    if (arrow::is_binary_like(field->type()->id())) {
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
  ARROW_ASSIGN_OR_RAISE(buffer_, arrow::AllocateBuffer(total_memory_size, arrow_pool_.get()));
  memset(buffer_->mutable_data(), 0, sizeof(int8_t) * total_memory_size);
  buffer_address_ = buffer_->mutable_data();
  return arrow::Status::OK();
}

void VeloxToRowConverter::ResumeVeloxVector() {
  for (int col_idx = 0; col_idx < num_cols_; col_idx++) {
    vecs_.push_back(rv_->childAt(col_idx));
  }
}

arrow::Status VeloxToRowConverter::Write() {
  for (int col_idx = 0; col_idx < num_cols_; col_idx++) {
    int64_t field_offset = GetFieldOffset(nullBitsetWidthInBytes_, col_idx);
    auto col_type_id = schema_->field(col_idx)->type()->id();
    switch (col_type_id) {
      // We should keep supported types consistent with that in #buildCheck of GlutenColumnarToRowExec.scala.
      case arrow::Int8Type::type_id: {
        auto vec = vecs_[col_idx];
        bool mayHaveNulls = vec->mayHaveNulls();
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          if (mayHaveNulls && vec->isNullAt(row_idx)) {
            SetNullAt(buffer_address_, offsets_[row_idx], field_offset, col_idx);
          } else {
            // Will use Velox's conversion.
            auto write_address = (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
            auto serialized = row::UnsafeRowSerializer::serialize<TinyintType>(vec, write_address, row_idx);
          }
        }
        break;
      }
      case arrow::Int16Type::type_id: {
        auto vec = vecs_[col_idx];
        bool mayHaveNulls = vec->mayHaveNulls();
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          if (mayHaveNulls && vec->isNullAt(row_idx)) {
            SetNullAt(buffer_address_, offsets_[row_idx], field_offset, col_idx);
          } else {
            // Will use Velox's conversion.
            auto write_address = (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
            auto serialized = row::UnsafeRowSerializer::serialize<SmallintType>(vec, write_address, row_idx);
          }
        }
        break;
      }
      case arrow::Int32Type::type_id: {
        auto vec = vecs_[col_idx];
        bool mayHaveNulls = vec->mayHaveNulls();
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          if (mayHaveNulls && vec->isNullAt(row_idx)) {
            SetNullAt(buffer_address_, offsets_[row_idx], field_offset, col_idx);
          } else {
            // Will use Velox's conversion.
            auto write_address = (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
            auto serialized = row::UnsafeRowSerializer::serialize<IntegerType>(vec, write_address, row_idx);
          }
        }
        break;
      }
      case arrow::Int64Type::type_id: {
        auto vec = vecs_[col_idx];
        bool mayHaveNulls = vec->mayHaveNulls();
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          if (mayHaveNulls && vec->isNullAt(row_idx)) {
            SetNullAt(buffer_address_, offsets_[row_idx], field_offset, col_idx);
          } else {
            // Will use Velox's conversion.
            auto write_address = (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
            auto serialized = row::UnsafeRowSerializer::serialize<BigintType>(vec, write_address, row_idx);
          }
        }
        break;
      }
      case arrow::Date32Type::type_id: {
        auto vec = vecs_[col_idx];
        bool mayHaveNulls = vec->mayHaveNulls();
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          if (mayHaveNulls && vec->isNullAt(row_idx)) {
            SetNullAt(buffer_address_, offsets_[row_idx], field_offset, col_idx);
          } else {
            // Will use Velox's conversion.
            auto write_address = (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
            auto serialized = row::UnsafeRowSerializer::serialize<DateType>(vec, write_address, row_idx);
          }
        }
        break;
      }
      case arrow::FloatType::type_id: {
        auto vec = vecs_[col_idx];
        bool mayHaveNulls = vec->mayHaveNulls();
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          if (mayHaveNulls && vec->isNullAt(row_idx)) {
            SetNullAt(buffer_address_, offsets_[row_idx], field_offset, col_idx);
          } else {
            // Will use Velox's conversion.
            auto write_address = (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
            auto serialized = row::UnsafeRowSerializer::serialize<RealType>(vec, write_address, row_idx);
          }
        }
        break;
      }
      case arrow::DoubleType::type_id: {
        auto vec = vecs_[col_idx];
        bool mayHaveNulls = vec->mayHaveNulls();
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          if (mayHaveNulls && vec->isNullAt(row_idx)) {
            SetNullAt(buffer_address_, offsets_[row_idx], field_offset, col_idx);
          } else {
            // Will use Velox's conversion.
            auto write_address = (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
            auto serialized = row::UnsafeRowSerializer::serialize<DoubleType>(vec, write_address, row_idx);
          }
        }
        break;
      }
      case arrow::BooleanType::type_id: {
        auto vec = vecs_[col_idx];
        bool mayHaveNulls = vec->mayHaveNulls();
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          if (mayHaveNulls && vec->isNullAt(row_idx)) {
            SetNullAt(buffer_address_, offsets_[row_idx], field_offset, col_idx);
          } else {
            // Will use Velox's conversion.
            auto write_address = (char*)(buffer_address_ + offsets_[row_idx] + field_offset);
            auto serialized = row::UnsafeRowSerializer::serialize<BooleanType>(vec, write_address, row_idx);
          }
        }
        break;
      }
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        auto vec = vecs_[col_idx];
        auto str_views = vec->asFlatVector<StringView>()->rawValues();
        bool mayHaveNulls = vec->mayHaveNulls();
        for (int row_idx = 0; row_idx < num_rows_; row_idx++) {
          if (mayHaveNulls && vec->isNullAt(row_idx)) {
            SetNullAt(buffer_address_, offsets_[row_idx], field_offset, col_idx);
          } else {
            int32_t length = (int32_t)str_views[row_idx].size();
            auto value = str_views[row_idx].data();
            // Write the variable value.
            memcpy(buffer_address_ + offsets_[row_idx] + buffer_cursor_[row_idx], value, length);
            int64_t offset_and_size = ((int64_t)buffer_cursor_[row_idx] << 32) | length;
            // Write the offset and size.
            memcpy(buffer_address_ + offsets_[row_idx] + field_offset, &offset_and_size, sizeof(int64_t));
            buffer_cursor_[row_idx] += length;
          }
        }
        break;
      }
      default:
        return arrow::Status::Invalid(
            "Type " + schema_->field(col_idx)->type()->name() + " is not supported in VeloxToRow conversion.");
    }
  }
  return arrow::Status::OK();
}

} // namespace gluten
