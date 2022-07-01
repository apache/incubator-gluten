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

#include "arrow_columnar_to_row_converter.h"

#include <iostream>

namespace gluten {
namespace columnartorow {

arrow::Status ArrowColumnarToRowConverter::Init() {
  num_rows_ = rb_->num_rows();
  num_cols_ = rb_->num_columns();
  // Calculate the initial size
  nullBitsetWidthInBytes_ = CalculateBitSetWidthInBytes(num_cols_);

  int64_t fixed_size_per_row = CalculatedFixeSizePerRow(rb_->schema(), num_cols_);

  // Initialize the offsets_ , lengths_, buffer_cursor_
  for (auto i = 0; i < num_rows_; i++) {
    lengths_.push_back(fixed_size_per_row);
    offsets_.push_back(0);
    buffer_cursor_.push_back(nullBitsetWidthInBytes_ + 8 * num_cols_);
  }
  // Calculated the lengths_
  for (auto i = 0; i < num_cols_; i++) {
    auto array = rb_->column(i);
    if (arrow::is_binary_like(array->type_id())) {
      auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
      using offset_type = typename arrow::BinaryType::offset_type;
      offset_type length;
      for (auto j = 0; j < num_rows_; j++) {
        auto value = binary_array->GetValue(j, &length);
        lengths_[j] += RoundNumberOfBytesToNearestWord(length);
      }
    }
  }
  // Calculated the offsets_  and total memory size based on lengths_
  int64_t total_memory_size = lengths_[0];
  for (auto i = 1; i < num_rows_; i++) {
    offsets_[i] = offsets_[i - 1] + lengths_[i - 1];
    total_memory_size += lengths_[i];
  }

  ARROW_ASSIGN_OR_RAISE(buffer_, AllocateBuffer(total_memory_size, memory_pool_.get()));

  memset(buffer_->mutable_data(), 0, sizeof(int8_t) * total_memory_size);

  buffer_address_ = buffer_->mutable_data();
  return arrow::Status::OK();
}

arrow::Status ArrowColumnarToRowConverter::WriteValue(
    uint8_t* buffer_address, int64_t field_offset, std::shared_ptr<arrow::Array> array,
    int32_t col_index, int64_t num_rows, std::vector<int64_t>& offsets,
    std::vector<int64_t>& buffer_cursor) {
  switch (array->type_id()) {
    case arrow::BooleanType::type_id: {
      // Boolean type
      auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);
      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          auto value = bool_array->Value(i);
          memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(bool));
        }
      }
      break;
    }
    case arrow::Int8Type::type_id: {
      // Byte type
      auto int8_array = std::static_pointer_cast<arrow::Int8Array>(array);
      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          auto value = int8_array->Value(i);
          memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(int8_t));
        }
      }
      break;
    }
    case arrow::Int16Type::type_id: {
      // Short type
      auto int16_array = std::static_pointer_cast<arrow::Int16Array>(array);
      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          auto value = int16_array->Value(i);
          memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(int16_t));
        }
      }
      break;
    }
    case arrow::Int32Type::type_id: {
      // Integer type
      auto int32_array = std::static_pointer_cast<arrow::Int32Array>(array);
      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          auto value = int32_array->Value(i);
          memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(int32_t));
        }
      }
      break;
    }
    case arrow::Int64Type::type_id: {
      // Long type
      auto int64_array = std::static_pointer_cast<arrow::Int64Array>(array);
      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          auto value = int64_array->Value(i);
          memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(int64_t));
        }
      }
      break;
    }
    case arrow::FloatType::type_id: {
      // Float type
      auto float_array = std::static_pointer_cast<arrow::FloatArray>(array);
      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          auto value = float_array->Value(i);
          memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(float));
        }
      }
      break;
    }
    case arrow::DoubleType::type_id: {
      // Double type
      auto double_array = std::static_pointer_cast<arrow::DoubleArray>(array);
      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          auto value = double_array->Value(i);
          memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(double));
        }
      }
      break;
    }
    case arrow::BinaryType::type_id: {
      // Binary type
      auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
      using offset_type = typename arrow::BinaryType::offset_type;

      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          offset_type length;
          auto value = binary_array->GetValue(i, &length);
          // write the variable value
          memcpy(buffer_address + offsets[i] + buffer_cursor[i], value, length);
          // write the offset and size
          int64_t offsetAndSize = (buffer_cursor[i] << 32) | length;
          memcpy(buffer_address + offsets[i] + field_offset, &offsetAndSize,
                 sizeof(int64_t));
          buffer_cursor[i] += length;
        }
      }
      break;
    }
    case arrow::StringType::type_id: {
      // String type
      auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
      using offset_type = typename arrow::StringType::offset_type;
      offset_type length;
      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          offset_type length;
          auto value = string_array->GetValue(i, &length);
          // write the variable value
          memcpy(buffer_address + offsets[i] + buffer_cursor[i], value, length);
          // write the offset and size
          int64_t offsetAndSize = (buffer_cursor[i] << 32) | length;
          memcpy(buffer_address + offsets[i] + field_offset, &offsetAndSize,
                 sizeof(int64_t));
          buffer_cursor[i] += length;
        }
      }
      break;
    }
    case arrow::Decimal128Type::type_id: {
      auto out_array = dynamic_cast<arrow::Decimal128Array*>(array.get());
      auto dtype = dynamic_cast<arrow::Decimal128Type*>(out_array->type().get());

      int32_t precision = dtype->precision();
      int32_t scale = dtype->scale();

      for (auto i = 0; i < num_rows; i++) {
        const arrow::Decimal128 out_value(out_array->GetValue(i));
        bool flag = out_array->IsNull(i);

        if (precision <= 18) {
          if (!flag) {
            // Get the long value and write the long value
            // Refer to the int64_t() method of Decimal128
            int64_t long_value = static_cast<int64_t>(out_value.low_bits());
            memcpy(buffer_address + offsets[i] + field_offset, &long_value, sizeof(long));
          } else {
            SetNullAt(buffer_address, offsets[i], field_offset, col_index);
          }
        } else {
          if (flag) {
            SetNullAt(buffer_address, offsets[i], field_offset, col_index);
          } else {
            int32_t size;
            auto out = ToByteArray(out_value, &size);
            assert(size <= 16);

            // write the variable value
            memcpy(buffer_address + buffer_cursor[i] + offsets[i], &out[0], size);
            // write the offset and size
            int64_t offsetAndSize = (buffer_cursor[i] << 32) | size;
            memcpy(buffer_address + offsets[i] + field_offset, &offsetAndSize,
                   sizeof(int64_t));
          }

          // Update the cursor of the buffer.
          int64_t new_cursor = buffer_cursor[i] + 16;
          buffer_cursor[i] = new_cursor;
        }
      }
      break;
    }
    case arrow::Date32Type::type_id: {
      auto date32_array = std::static_pointer_cast<arrow::Date32Array>(array);
      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          auto value = date32_array->Value(i);
          memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(int32_t));
        }
      }
      break;
    }
    case arrow::TimestampType::type_id: {
      auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(array);
      for (auto i = 0; i < num_rows; i++) {
        bool is_null = array->IsNull(i);
        if (is_null) {
          SetNullAt(buffer_address, offsets[i], field_offset, col_index);
        } else {
          auto value = timestamp_array->Value(i);
          memcpy(buffer_address + offsets[i] + field_offset, &value, sizeof(int64_t));
        }
      }
      break;
    }
    default:
      return arrow::Status::Invalid("Unsupported data type: " + array->type_id());
  }
  return arrow::Status::OK();
}

arrow::Status ArrowColumnarToRowConverter::Write() {
  for (auto i = 0; i < num_cols_; i++) {
    auto array = rb_->column(i);
    int64_t field_offset = GetFieldOffset(nullBitsetWidthInBytes_, i);
    WriteValue(buffer_address_, field_offset, array, i, num_rows_, offsets_,
               buffer_cursor_);
  }
  return arrow::Status::OK();
}

}  // namespace columnartorow
}  // namespace gluten
