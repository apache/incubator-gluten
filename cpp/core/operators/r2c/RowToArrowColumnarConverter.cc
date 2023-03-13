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

#include "operators/r2c/RowToArrowColumnarConverter.h"

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/decimal.h>

#include <immintrin.h>
#include <x86intrin.h>
#include <algorithm>

#include "utils/exception.h"

namespace gluten {

inline int64_t CalculateBitSetWidthInBytes(int32_t numFields) {
  return ((numFields + 63) / 64) * 8;
}

inline int64_t GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index) {
  return nullBitsetWidthInBytes + 8L * index;
}

inline bool IsNull(uint8_t* buffer_address, int32_t index) {
  int64_t mask = 1L << (index & 0x3f); // mod 64 and shift
  int64_t wordOffset = (index >> 6) * 8;
  int64_t value = *((int64_t*)(buffer_address + wordOffset));
  return (value & mask) != 0;
}

inline int32_t CalculateHeaderPortionInBytes(int32_t num_elements) {
  return 8 + ((num_elements + 63) / 64) * 8;
}

inline arrow::Status CreateArrayData(
    int32_t row_start,
    std::shared_ptr<arrow::Schema> schema,
    int32_t batch_rows,
    std::vector<int64_t>& offsets,
    uint8_t* memory_address_,
    bool support_avx512,
    std::vector<arrow::Type::type>& typevec,
    std::vector<uint8_t>& typewidth,
    std::vector<std::shared_ptr<arrow::ArrayData>>& columns,
    int num_fields,
    std::vector<int64_t>& field_offset_vec) {
  for (auto columnar_id = 0; columnar_id < num_fields; columnar_id++) {
    auto& array = (columns[columnar_id]);
    int64_t fieldOffset = field_offset_vec[columnar_id];
    switch (typevec[columnar_id]) {
      case arrow::BooleanType::type_id: {
        auto array_data = array->buffers[1]->mutable_data();
        int64_t position = row_start;
        int64_t null_count = 0;

        auto out_is_valid = array->buffers[0]->mutable_data();
        while (position < row_start + batch_rows) {
          bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
          if (is_null) {
            null_count++;
            arrow::bit_util::SetBitTo(out_is_valid, position, false);
          } else {
            bool value = *(bool*)(memory_address_ + offsets[position] + fieldOffset);
            arrow::bit_util::SetBitTo(array_data, position, value);
            arrow::bit_util::SetBitTo(out_is_valid, position, true);
          }
          position++;
        }
        array->null_count += null_count;
        break;
      }
      case arrow::Decimal128Type::type_id: {
        auto field = schema->field(columnar_id);
        auto type = field->type();
        auto dtype = std::dynamic_pointer_cast<arrow::Decimal128Type>(type);
        int32_t precision = dtype->precision();

        auto array_data = array->GetMutableValues<arrow::Decimal128>(1);
        int64_t position = row_start;
        int64_t null_count = 0;
        auto out_is_valid = array->buffers[0]->mutable_data();
        while (position < row_start + batch_rows) {
          bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
          if (is_null) {
            null_count++;
            arrow::bit_util::SetBitTo(out_is_valid, position, false);
            array_data[position] = arrow::Decimal128{};
          } else {
            arrow::bit_util::SetBitTo(out_is_valid, position, true);
            if (precision <= 18) {
              int64_t low_value;
              memcpy(&low_value, memory_address_ + offsets[position] + fieldOffset, 8);
              arrow::Decimal128 value = arrow::Decimal128(arrow::BasicDecimal128(low_value));
              array_data[position] = value;
            } else {
              int64_t offsetAndSize;
              memcpy(&offsetAndSize, memory_address_ + offsets[position] + fieldOffset, sizeof(int64_t));
              int32_t length = int32_t(offsetAndSize);
              int32_t wordoffset = int32_t(offsetAndSize >> 32);
              uint8_t bytesValue[length];
              memcpy(bytesValue, memory_address_ + offsets[position] + wordoffset, length);
              uint8_t bytesValue2[16]{};
              for (int k = length - 1; k >= 0; k--) {
                bytesValue2[length - 1 - k] = bytesValue[k];
              }
              if (int8_t(bytesValue[0]) < 0) {
                for (int k = length; k < 16; k++) {
                  bytesValue2[k] = 255;
                }
              }
              arrow::Decimal128 value = arrow::Decimal128(arrow::BasicDecimal128(bytesValue2));
              array_data[position] = value;
            }
          }
          position++;
        }
        array->null_count += null_count;
        break;
      }
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        using offset_type = typename arrow::StringType::offset_type;
        auto validity_buffer = array->buffers[0]->mutable_data();
        auto array_offset = array->GetMutableValues<offset_type>(1);
        auto array_data = array->buffers[2]->mutable_data();
        int64_t null_count = 0;

        array_offset[0] = 0;
        for (int64_t position = row_start; position < row_start + batch_rows; position++) {
          bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
          if (!is_null) {
            int64_t offsetAndSize = *(int64_t*)(memory_address_ + offsets[position] + fieldOffset);
            offset_type length = int32_t(offsetAndSize);
            int32_t wordoffset = int32_t(offsetAndSize >> 32);
            auto value_offset = array_offset[position + 1] = array_offset[position] + length;
            uint64_t size = array->buffers[2]->size();

            if (ARROW_PREDICT_FALSE(value_offset >= size)) {
              // allocate value buffer again
              // enlarge the buffer by 1.5x
              size = size + std::max((size >> 1), (uint64_t)length);
              auto value_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(array->buffers[2]);
              ARROW_RETURN_NOT_OK(value_buffer->Resize(size));
              array_data = value_buffer->mutable_data();
            }

            auto dst_value_base = array_data + array_offset[position];
            auto value_src_ptr = memory_address_ + offsets[position] + wordoffset;
#ifdef __AVX512BW__
            if (ARROW_PREDICT_TRUE(support_avx512)) {
              // write the variable value
              uint32_t k;
              for (k = 0; k + 32 < length; k += 32) {
                __m256i v = _mm256_loadu_si256((const __m256i*)(value_src_ptr + k));
                _mm256_storeu_si256((__m256i*)(dst_value_base + k), v);
              }
              auto mask = (1L << (length - k)) - 1;
              __m256i v = _mm256_maskz_loadu_epi8(mask, value_src_ptr + k);
              _mm256_mask_storeu_epi8(dst_value_base + k, mask, v);
            } else
#endif
            {
              memcpy(dst_value_base, value_src_ptr, length);
            }
          } else {
            arrow::bit_util::SetBitTo(validity_buffer, position, false);
            array_offset[position + 1] = array_offset[position];
            null_count++;
          }
        }
        array->null_count += null_count;
        break;
      }
      default: {
        auto array_data = array->buffers[1]->mutable_data();
        int64_t position = row_start;
        int64_t null_count = 0;
        auto out_is_valid = array->buffers[0]->mutable_data();

        if (typewidth[columnar_id] > 0) {
          while (position < row_start + batch_rows) {
            const uint8_t* srcptr = (memory_address_ + offsets[position] + fieldOffset);
            bool is_null = IsNull(memory_address_ + offsets[position], columnar_id);
            auto mask = (1L << (typewidth[columnar_id])) - 1;
            (void)mask; // suppress warning
            auto shift = _tzcnt_u32(typewidth[columnar_id]);
            uint8_t* destptr = array_data + (position << shift);
            if (!is_null) {
#ifdef __AVX512BW__
              if (ARROW_PREDICT_TRUE(support_avx512)) {
                __m256i v = _mm256_maskz_loadu_epi8(mask, srcptr);
                _mm256_mask_storeu_epi8(destptr, mask, v);
              } else
#endif
              {
                memcpy(destptr, srcptr, typewidth[columnar_id]);
              }
            } else {
              null_count++;
              arrow::bit_util::SetBitTo(out_is_valid, position, false);
              memset(destptr, 0, typewidth[columnar_id]);
            }
            position++;
          }
          array->null_count += null_count;
          break;
        } else {
          return arrow::Status::Invalid("Unsupported data type: " + typevec[columnar_id]);
        }
      }
    }
  }
  return arrow::Status::OK();
}

std::shared_ptr<arrow::RecordBatch> RowToColumnarConverter::convert() {
  support_avx512_ = __builtin_cpu_supports("avx512bw");
  auto num_fields = schema_->num_fields();
  int64_t nullBitsetWidthInBytes = CalculateBitSetWidthInBytes(num_fields);
  for (auto i = 0; i < num_rows_; i++) {
    offsets_.push_back(0);
  }
  for (auto i = 1; i < num_rows_; i++) {
    offsets_[i] = offsets_[i - 1] + row_length_[i - 1];
  }
  std::vector<std::shared_ptr<arrow::Array>> arrays;

  std::vector<arrow::Type::type> typevec;
  std::vector<uint8_t> typewidth;
  std::vector<int64_t> field_offset_vec;
  typevec.resize(num_fields);
  // Store bytes for different fixed width types
  typewidth.resize(num_fields);
  field_offset_vec.resize(num_fields);

  std::vector<std::shared_ptr<arrow::ArrayData>> columns;
  columns.resize(num_fields);

  // Allocate Buffers firstly
  for (auto i = 0; i < num_fields; i++) {
    auto field = schema_->field(i);
    typevec[i] = field->type()->id();
    typewidth[i] = arrow::bit_width(typevec[i]) >> 3;
    field_offset_vec[i] = GetFieldOffset(nullBitsetWidthInBytes, i);

    switch (typevec[i]) {
      case arrow::BooleanType::type_id: {
        arrow::ArrayData out_data;
        out_data.length = num_rows_;
        out_data.buffers.resize(2);
        out_data.type = arrow::TypeTraits<arrow::BooleanType>::type_singleton();
        out_data.null_count = 0;

        GLUTEN_ASSIGN_OR_THROW(out_data.buffers[1], AllocateBitmap(num_rows_, m_pool_));
        GLUTEN_ASSIGN_OR_THROW(out_data.buffers[0], AllocateBitmap(num_rows_, m_pool_));
        auto validity_buffer = out_data.buffers[0]->mutable_data();
        // initialize all true once allocated
        memset(validity_buffer, 0xff, out_data.buffers[0]->capacity());
        columns[i] = std::make_shared<arrow::ArrayData>(std::move(out_data));
        break;
      }
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        arrow::ArrayData out_data;
        out_data.length = num_rows_;
        out_data.buffers.resize(3);
        out_data.type = field->type();
        out_data.null_count = 0;
        using offset_type = typename arrow::StringType::offset_type;
        GLUTEN_ASSIGN_OR_THROW(out_data.buffers[0], AllocateBitmap(num_rows_, m_pool_));
        GLUTEN_ASSIGN_OR_THROW(out_data.buffers[1], AllocateBuffer(sizeof(offset_type) * (num_rows_ + 1), m_pool_));
        GLUTEN_ASSIGN_OR_THROW(out_data.buffers[2], AllocateResizableBuffer(20 * num_rows_, m_pool_));
        auto validity_buffer = out_data.buffers[0]->mutable_data();
        // initialize all true once allocated
        memset(validity_buffer, 0xff, out_data.buffers[0]->capacity());
        columns[i] = std::make_shared<arrow::ArrayData>(std::move(out_data));
        break;
      }
      case arrow::Decimal128Type::type_id: {
        auto dtype = std::dynamic_pointer_cast<arrow::Decimal128Type>(field->type());
        int32_t precision = dtype->precision();
        int32_t scale = dtype->scale();

        arrow::ArrayData out_data;
        out_data.length = num_rows_;
        out_data.buffers.resize(2);
        out_data.type = arrow::decimal128(precision, scale);
        out_data.null_count = 0;
        GLUTEN_ASSIGN_OR_THROW(out_data.buffers[1], AllocateBuffer(16 * num_rows_, m_pool_));
        GLUTEN_ASSIGN_OR_THROW(out_data.buffers[0], AllocateBitmap(num_rows_, m_pool_));
        auto validity_buffer = out_data.buffers[0]->mutable_data();
        // initialize all true once allocated
        memset(validity_buffer, 0xff, out_data.buffers[0]->capacity());
        columns[i] = std::make_shared<arrow::ArrayData>(std::move(out_data));
        break;
      }
      default: {
        arrow::ArrayData out_data;
        out_data.length = num_rows_;
        out_data.buffers.resize(2);
        out_data.type = field->type();
        out_data.null_count = 0;
        GLUTEN_ASSIGN_OR_THROW(out_data.buffers[1], AllocateBuffer(typewidth[i] * num_rows_, m_pool_));
        GLUTEN_ASSIGN_OR_THROW(out_data.buffers[0], AllocateBitmap(num_rows_, m_pool_));
        auto validity_buffer = out_data.buffers[0]->mutable_data();
        // initialize all true once allocated
        memset(validity_buffer, 0xff, out_data.buffers[0]->capacity());
        columns[i] = std::make_shared<arrow::ArrayData>(std::move(out_data));
        break;
      }
    }
  }

  const int32_t kBatchRowNum = 16;
  int row = 0;
  for (; row + kBatchRowNum < num_rows_; row += kBatchRowNum) {
    GLUTEN_THROW_NOT_OK(CreateArrayData(
        row,
        schema_,
        kBatchRowNum,
        offsets_,
        memory_address_,
        support_avx512_,
        typevec,
        typewidth,
        columns,
        num_fields,
        field_offset_vec));
  }
  for (; row < num_rows_; row++) {
    GLUTEN_THROW_NOT_OK(CreateArrayData(
        row,
        schema_,
        1,
        offsets_,
        memory_address_,
        support_avx512_,
        typevec,
        typewidth,
        columns,
        num_fields,
        field_offset_vec));
  }

  for (auto i = 0; i < num_fields; i++) {
    auto array = arrow::MakeArray(columns[i]);
    arrays.push_back(array);
  }

  return arrow::RecordBatch::Make(schema_, num_rows_, arrays);
}

} // namespace gluten
