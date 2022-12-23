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

#include "ArrowColumnarToRowConverter.h"

#include <arrow/array/array_decimal.h>
#include <arrow/util/decimal.h>
#include <immintrin.h>
#include <iostream>

namespace gluten {

uint32_t x_7[8] __attribute__((aligned(32))) = {0x7, 0x7, 0x7, 0x7, 0x7, 0x7, 0x7, 0x7};
uint32_t x_8[8] __attribute__((aligned(32))) = {0x8, 0x8, 0x8, 0x8, 0x8, 0x8, 0x8, 0x8};

arrow::Status ArrowColumnarToRowConverter::Init() {
  support_avx512_ = __builtin_cpu_supports("avx512bw");
  // std::cout << "support_avx512_:" << support_avx512_ << std::endl;

  num_rows_ = rb_->num_rows();
  num_cols_ = rb_->num_columns();
  // Calculate the initial size
  nullBitsetWidthInBytes_ = CalculateBitSetWidthInBytes(num_cols_);

  int32_t fixed_size_per_row = CalculatedFixeSizePerRow(rb_->schema(), num_cols_);

  // Initialize the offsets_ , lengths_, buffer_cursor_
  lengths_.resize(num_rows_, fixed_size_per_row);
  std::fill(lengths_.begin(), lengths_.end(), fixed_size_per_row);

  offsets_.resize(num_rows_ + 1);
  buffer_cursor_.resize(num_rows_, nullBitsetWidthInBytes_ + 8 * num_cols_);

  // Calculated the lengths_
  for (auto i = 0; i < num_cols_; i++) {
    auto array = rb_->column(i);
    if (arrow::is_binary_like(array->type_id())) {
      auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(array);
      using offset_type = typename arrow::BinaryType::offset_type;
      const offset_type* offsetarray = binary_array->raw_value_offsets();
      int32_t j = 0;
      int32_t* length_data = lengths_.data();

#ifdef __AVX512BW__
      if (ARROW_PREDICT_TRUE(support_avx512_)) {
        __m256i x7_8x = _mm256_load_si256((__m256i*)x_7);
        __m256i x8_8x = _mm256_load_si256((__m256i*)x_8);
        __m256i offsetarray_1_8x;
        if (j + 16 < num_rows_) {
          // The address shall be 64-bytes aligned. But currently if use
          // _mm256_load_si256, will crash. Track with
          // https://github.com/oap-project/gazelle_plugin/issues/927
          offsetarray_1_8x = _mm256_loadu_si256((__m256i*)&offsetarray[j]);
        }
        for (j; j + 16 < num_rows_; j += 8) {
          __m256i offsetarray_8x = offsetarray_1_8x;
          offsetarray_1_8x = _mm256_loadu_si256((__m256i*)&offsetarray[j + 8]);

          __m256i length_8x = _mm256_alignr_epi32(offsetarray_1_8x, offsetarray_8x, 0x1);
          length_8x = _mm256_sub_epi32(length_8x, offsetarray_8x);

          __m256i reminder_8x = _mm256_and_si256(length_8x, x7_8x);
          reminder_8x = _mm256_sub_epi32(x8_8x, reminder_8x);
          reminder_8x = _mm256_and_si256(reminder_8x, x7_8x);
          reminder_8x = _mm256_add_epi32(reminder_8x, length_8x);
          __m256i dst_length_8x = _mm256_load_si256((__m256i*)length_data);
          dst_length_8x = _mm256_add_epi32(dst_length_8x, reminder_8x);
          _mm256_store_si256((__m256i*)length_data, dst_length_8x);
          length_data += 8;
          _mm_prefetch(&offsetarray[j + (128 + 128) / sizeof(offset_type)], _MM_HINT_T0);
        }
      }
#endif

      for (j; j < num_rows_; j++) {
        offset_type length = offsetarray[j + 1] - offsetarray[j];
        *length_data += RoundNumberOfBytesToNearestWord(length);
        length_data++;
      }
    }
  }
  // Calculated the offsets_  and total memory size based on lengths_
  int64_t total_memory_size = lengths_[0];
  offsets_[0] = 0;
  for (auto i = 1; i < num_rows_; i++) {
    offsets_[i] = total_memory_size;
    total_memory_size += lengths_[i];
  }
  offsets_[num_rows_] = total_memory_size;

  // allocate one more cache line to ease avx operations
  if (buffer_ == nullptr || buffer_->capacity() < total_memory_size + 64) {
    ARROW_ASSIGN_OR_RAISE(buffer_, AllocateBuffer(total_memory_size + 64, arrow_pool_.get()));
#ifdef __AVX512BW__
    if (ARROW_PREDICT_TRUE(support_avx512_)) {
      memset(buffer_->mutable_data() + total_memory_size, 0, buffer_->capacity() - total_memory_size);
    } else
#endif
    {
      memset(buffer_->mutable_data(), 0, buffer_->capacity());
    }
  }

  buffer_address_ = buffer_->mutable_data();

  return arrow::Status::OK();
}

arrow::Status ArrowColumnarToRowConverter::FillBuffer(
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
    bool support_avx512) {
#ifdef __AVX512BW__
  if (ARROW_PREDICT_TRUE(support_avx512)) {
    __m256i fill_0_8x;
    fill_0_8x = _mm256_xor_si256(fill_0_8x, fill_0_8x);
    for (auto j = row_start; j < row_start + batch_rows; j++) {
      auto rowlength = offsets[j + 1] - offsets[j];
      for (auto p = 0; p < rowlength + 32; p += 32) {
        _mm256_storeu_si256((__m256i*)(buffer_address + offsets[j]), fill_0_8x);
        _mm_prefetch(buffer_address + offsets[j] + 128, _MM_HINT_T0);
      }
    }
  }
#endif

  for (auto col_index = 0; col_index < num_cols; col_index++) {
    auto& array = arrays[col_index];
    int64_t field_offset = nullBitsetWidthInBytes + (col_index << 3L);

    switch (typevec[col_index]) {
      // We should keep supported types consistent with that in #buildCheck of GlutenColumnarToRowExec.scala.
      case arrow::BooleanType::type_id: {
        // Boolean type
        auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);

        for (auto j = row_start; j < row_start + batch_rows; j++) {
          bool is_null = array->IsNull(j);
          if (nullvec[col_index] || (!array->IsNull(j))) {
            auto value = bool_array->Value(j);
            memcpy(buffer_address + offsets[j] + field_offset, &value, sizeof(bool));
          } else {
            SetNullAt(buffer_address, offsets[j], field_offset, col_index);
          }
        }
        break;
      }
      case arrow::StringType::type_id:
      case arrow::BinaryType::type_id: {
        // Binary type
        auto binary_array = (arrow::BinaryArray*)(array.get());
        using offset_type = typename arrow::BinaryType::offset_type;
        offset_type* BinaryOffsets = (offset_type*)(dataptrs[col_index][1]);
        for (auto j = row_start; j < row_start + batch_rows; j++) {
          if (nullvec[col_index] || (!array->IsNull(j))) {
            offset_type length = BinaryOffsets[j + 1] - BinaryOffsets[j];
            auto value = &dataptrs[col_index][2][BinaryOffsets[j]];

#ifdef __AVX512BW__
            if (ARROW_PREDICT_TRUE(support_avx512)) {
              // write the variable value
              offset_type k;
              for (k = 0; k + 32 < length; k += 32) {
                __m256i v = _mm256_loadu_si256((const __m256i*)(value + k));
                _mm256_storeu_si256((__m256i*)(buffer_address + offsets[j] + buffer_cursor[j] + k), v);
              }
              // create some bits of "1", num equals length
              auto mask = (1L << (length - k)) - 1;
              __m256i v = _mm256_maskz_loadu_epi8(mask, value + k);
              _mm256_mask_storeu_epi8(buffer_address + offsets[j] + buffer_cursor[j] + k, mask, v);
            } else
#endif
            {
              // write the variable value
              memcpy(buffer_address + offsets[j] + buffer_cursor[j], value, length);
            }

            // write the offset and size
            int64_t offsetAndSize = ((int64_t)buffer_cursor[j] << 32) | length;
            *(int64_t*)(buffer_address + offsets[j] + field_offset) = offsetAndSize;
            buffer_cursor[j] += RoundNumberOfBytesToNearestWord(length);
          } else {
            SetNullAt(buffer_address, offsets[j], field_offset, col_index);
          }
        }
        break;
      }
      case arrow::Decimal128Type::type_id: {
        auto out_array = dynamic_cast<arrow::Decimal128Array*>(array.get());
        auto dtype = dynamic_cast<arrow::Decimal128Type*>(out_array->type().get());

        int32_t precision = dtype->precision();
        int32_t scale = dtype->scale();

        for (auto j = row_start; j < row_start + batch_rows; j++) {
          const arrow::Decimal128 out_value(out_array->GetValue(j));
          bool flag = out_array->IsNull(j);

          if (precision <= 18) {
            if (!flag) {
              // Get the long value and write the long value
              // Refer to the int64_t() method of Decimal128
              int64_t long_value = static_cast<int64_t>(out_value.low_bits());
              memcpy(buffer_address + offsets[j] + field_offset, &long_value, sizeof(long));
            } else {
              SetNullAt(buffer_address, offsets[j], field_offset, col_index);
            }
          } else {
            if (flag) {
              SetNullAt(buffer_address, offsets[j], field_offset, col_index);
            } else {
              int32_t size;
              auto out = ToByteArray(out_value, &size);
              assert(size <= 16);

              // write the variable value
              memcpy(buffer_address + buffer_cursor[j] + offsets[j], &out[0], size);
              // write the offset and size
              int64_t offsetAndSize = ((int64_t)buffer_cursor[j] << 32) | size;
              memcpy(buffer_address + offsets[j] + field_offset, &offsetAndSize, sizeof(int64_t));
            }

            // Update the cursor of the buffer.
            int64_t new_cursor = buffer_cursor[j] + 16;
            buffer_cursor[j] = new_cursor;
          }
        }
        break;
      }
      default: {
        if (typewidth[col_index] > 0) {
          auto dataptr = dataptrs[col_index][1];
          auto mask = (1L << (typewidth[col_index])) - 1;
          auto shift = _tzcnt_u32(typewidth[col_index]);
          auto buffer_address_tmp = buffer_address + field_offset;
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            if (nullvec[col_index] || (!array->IsNull(j))) {
              const uint8_t* srcptr = dataptr + (j << shift);
#ifdef __AVX512BW__
              if (ARROW_PREDICT_TRUE(support_avx512)) {
                __m256i v = _mm256_maskz_loadu_epi8(mask, srcptr);
                _mm256_mask_storeu_epi8(buffer_address_tmp + offsets[j], mask, v);
                _mm_prefetch(srcptr + 64, _MM_HINT_T0);
              } else
#endif
              {
                memcpy(buffer_address_tmp + offsets[j], srcptr, typewidth[col_index]);
              }
            } else {
              SetNullAt(buffer_address, offsets[j], field_offset, col_index);
            }
          }
          break;
        } else {
          return arrow::Status::Invalid("Unsupported data type: " + typevec[col_index]);
        }
      }
    }
  }
  return arrow::Status::OK();
}

arrow::Status ArrowColumnarToRowConverter::Write() {
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::vector<const uint8_t*>> dataptrs;
  std::vector<int64_t> col_arrdata_offsets;
  dataptrs.resize(num_cols_);
  col_arrdata_offsets.resize(num_cols_);
  std::vector<uint8_t> nullvec;
  nullvec.resize(num_cols_, 0);

  std::vector<arrow::Type::type> typevec;
  std::vector<uint8_t> typewidth;

  typevec.resize(num_cols_);
  // Store bytes for different fixed width types
  typewidth.resize(num_cols_);

  for (auto col_index = 0; col_index < num_cols_; col_index++) {
    auto array = rb_->column(col_index);
    arrays.push_back(array);
    auto arraydata = array->data();
    auto bufs = arraydata->buffers;
    col_arrdata_offsets[col_index] = arraydata->offset;

    // If nullvec[col_index]  equals 1, means no null value in this column
    nullvec[col_index] = (array->null_count() == 0);
    typevec[col_index] = array->type_id();
    // calculate bytes from bit_num
    typewidth[col_index] = arrow::bit_width(typevec[col_index]) >> 3;

    if (arrow::bit_width(array->type_id()) > 1) {
      if (bufs[0]) {
        dataptrs[col_index].push_back(bufs[0]->data());
      } else {
        dataptrs[col_index].push_back(nullptr);
      }
      dataptrs[col_index].push_back(bufs[1]->data() + arraydata->offset * (arrow::bit_width(array->type_id()) >> 3));
    } else if (array->type_id() == arrow::StringType::type_id || array->type_id() == arrow::BinaryType::type_id) {
      if (bufs[0]) {
        dataptrs[col_index].push_back(bufs[0]->data());
      } else {
        dataptrs[col_index].push_back(nullptr);
      }

      auto binary_array = (arrow::BinaryArray*)(array.get());
      dataptrs[col_index].push_back((uint8_t*)(binary_array->raw_value_offsets()));
      dataptrs[col_index].push_back((uint8_t*)(binary_array->raw_data()));
    }
  }

  int32_t i = 0;
#define BATCH_ROW_NUM 16
  for (i; i + BATCH_ROW_NUM < num_rows_; i += BATCH_ROW_NUM) {
    FillBuffer(
        i,
        BATCH_ROW_NUM,
        dataptrs,
        nullvec,
        buffer_address_,
        offsets_,
        buffer_cursor_,
        num_cols_,
        num_rows_,
        nullBitsetWidthInBytes_,
        typevec,
        typewidth,
        arrays,
        support_avx512_);
  }

  for (i; i < num_rows_; i++) {
    FillBuffer(
        i,
        1,
        dataptrs,
        nullvec,
        buffer_address_,
        offsets_,
        buffer_cursor_,
        num_cols_,
        num_rows_,
        nullBitsetWidthInBytes_,
        typevec,
        typewidth,
        arrays,
        support_avx512_);
  }

  return arrow::Status::OK();
}

} // namespace gluten
