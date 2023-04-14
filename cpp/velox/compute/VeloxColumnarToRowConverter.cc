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

#include "VeloxColumnarToRowConverter.h"

#include <arrow/util/logging.h>
#include <arrow/util/decimal.h>

#include "ArrowTypeUtils.h"
#include "arrow/c/helpers.h"
#include "arrow/util/decimal.h"
#include "include/arrow/c/bridge.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/row/UnsafeRowSerializer.h"
#include "velox/type/UnscaledLongDecimal.h"
#include "velox/type/UnscaledShortDecimal.h"
#include "velox/vector/arrow/Bridge.h"
#if defined(__x86_64__)
#include <immintrin.h>
#endif
using namespace facebook;

namespace gluten {

#define ASSIGN_VALUE(datatype)                                                        \
  auto values = reinterpret_cast<const datatype*>(dataptrs[col_index]);               \
  auto p = reinterpret_cast<datatype*>(buffer_address_ + offsets_[j] + field_offset); \
  *p = values[j]

uint32_t x_7[8] __attribute__((aligned(32))) = {0x7, 0x7, 0x7, 0x7, 0x7, 0x7, 0x7, 0x7};
uint32_t x_8[8] __attribute__((aligned(32))) = {0x8, 0x8, 0x8, 0x8, 0x8, 0x8, 0x8, 0x8};
uint32_t x_seq[8] __attribute__((aligned(32))) = {0x0, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70};

arrow::Status VeloxColumnarToRowConverter::Init() {
  support_avx512_ = false;
#if defined(__x86_64__)
  support_avx512_ = __builtin_cpu_supports("avx512bw");
#endif

  num_rows_ = rv_->size();
  num_cols_ = rv_->childrenSize();

  ArrowSchema c_schema{};
  velox::exportToArrow(rv_, c_schema);
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
  lengths_.resize(num_rows_, fixed_size_per_row);
  offsets_.resize(num_rows_ + 1, 0);
  buffer_cursor_.resize(num_rows_, nullBitsetWidthInBytes_ + 8 * num_cols_);

  // Calculated the lengths_
  for (int64_t col_idx = 0; col_idx < num_cols_; col_idx++) {
    std::shared_ptr<arrow::Field> field = schema_->field(col_idx);
    if (arrow::is_binary_like(field->type()->id())) {
      auto str_views = vecs_[col_idx]->asFlatVector<velox::StringView>()->rawValues();
      int j = 0;
      // StringView: {size; prefix; inline}
      int32_t* length_data = lengths_.data();
#ifdef __AVX512BW__
      if (ARROW_PREDICT_TRUE(support_avx512_)) {
        __m256i x7_8x = _mm256_load_si256((__m256i*)x_7);
        __m256i x8_8x = _mm256_load_si256((__m256i*)x_8);
        __m256i vidx_8x = _mm256_load_si256((__m256i*)x_seq);

        for (; j + 16 < num_rows_; j += 8) {
          __m256i length_8x = _mm256_i32gather_epi32((int*)&str_views[j], vidx_8x, 1);

          __m256i reminder_8x = _mm256_and_si256(length_8x, x7_8x);
          reminder_8x = _mm256_sub_epi32(x8_8x, reminder_8x);
          reminder_8x = _mm256_and_si256(reminder_8x, x7_8x);
          reminder_8x = _mm256_add_epi32(reminder_8x, length_8x);
          __m256i dst_length_8x = _mm256_load_si256((__m256i*)length_data);
          dst_length_8x = _mm256_add_epi32(dst_length_8x, reminder_8x);
          _mm256_store_si256((__m256i*)length_data, dst_length_8x);
          length_data += 8;
          _mm_prefetch(&str_views[j + (128 + 128) / sizeof(velox::StringView)], _MM_HINT_T0);
        }
      }
#endif
      for (; j < num_rows_; j++) {
        auto length = str_views[j].size();
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
  // make sure destination buffer is less than 4G because offsets are uint32
  ARROW_CHECK_LE(total_memory_size, 0xFFFFFFFF);
  avg_rowsize_ = total_memory_size / num_rows_;

  // allocate one more cache line to ease avx operations
  if (buffer_ == nullptr) {
    ARROW_ASSIGN_OR_RAISE(buffer_, arrow::AllocateResizableBuffer(total_memory_size + 64, arrow_pool_.get()));
  }else if (buffer_->capacity() < total_memory_size + 64){
    ARROW_ASSIGN_OR_RAISE(buffer_, arrow::ResizePoolBuffer(buffer_, total_memory_size + 64, arrow_pool_.get()));
  }
  //memset the padding 
  memset(buffer_->mutable_data() + total_memory_size, 0, buffer_->capacity() - total_memory_size);

  buffer_address_ = buffer_->mutable_data();

  return arrow::Status::OK();
}

void VeloxColumnarToRowConverter::ResumeVeloxVector() {
  vecs_.reserve(num_cols_);
  for (int col_idx = 0; col_idx < num_cols_; col_idx++) {
    auto& child = rv_->childAt(col_idx);
    if (child->isFlatEncoding()) {
      vecs_.push_back(rv_->childAt(col_idx));
    } else {
      // Perform copy to flatten dictionary vectors.
      velox::VectorPtr copy = velox::BaseVector::create(child->type(), child->size(), child->pool());
      copy->copy(child.get(), 0, 0, child->size());
      vecs_.push_back(copy);
    }
  }
}

arrow::Status VeloxColumnarToRowConverter::FillBuffer(
    int32_t& row_start,
    int32_t batch_rows,
    std::vector<const uint8_t*>& dataptrs,
    std::vector<uint8_t> nullvec,
    std::vector<arrow::Type::type>& typevec,
    std::vector<uint8_t>& typewidth) {
#ifdef __AVX512BW__
  if (ARROW_PREDICT_TRUE(support_avx512_)) {
    __m256i fill_0_8x = {0LL};
    fill_0_8x = _mm256_xor_si256(fill_0_8x, fill_0_8x);
    // memset 0, force to bring the row buffer into L1 cache
    auto rowlength = offsets_[row_start + batch_rows] - offsets_[row_start];
    for (auto p = 0; p < rowlength + 32; p += 32) {
      _mm256_storeu_si256((__m256i*)(buffer_address_ + offsets_[row_start] + p), fill_0_8x);
      _mm_prefetch(buffer_address_ + offsets_[row_start] + 128, _MM_HINT_T0);
    }
  } else
#endif
  {
    auto rowlength = offsets_[row_start + batch_rows] - offsets_[row_start];
    memset(buffer_address_ + offsets_[row_start], 0, rowlength);
  }
  // fill the row by one column each time
  for (auto col_index = 0; col_index < num_cols_; col_index++) {
    auto& array = vecs_[col_index];

    int64_t field_offset = nullBitsetWidthInBytes_ + (col_index << 3L);
    if (nullvec[col_index]) {
      switch (typevec[col_index]) {
        // We should keep supported types consistent with that in #buildCheck of GlutenColumnarToRowExec.scala.
        case arrow::BooleanType::type_id: {
          // Boolean type
          auto bool_array = array->asFlatVector<bool>();
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            auto bits = reinterpret_cast<const uint8_t*>(dataptrs[col_index]);
            bool value = (bits[j >> 3] >> (j & 0x07)) & 1;
            auto p = reinterpret_cast<bool*>(buffer_address_ + offsets_[j] + field_offset);
            *p = value;
          }
          break;
        }
        case arrow::Int8Type::type_id: {
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            ASSIGN_VALUE(uint8_t);
          }
          break;
        }
        case arrow::Int16Type::type_id: {
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            ASSIGN_VALUE(uint16_t);
          }
          break;
        }
        case arrow::FloatType::type_id:
        case arrow::Int32Type::type_id:
        case arrow::Date32Type::type_id: {
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            ASSIGN_VALUE(uint32_t);
          }
          break;
        }
        case arrow::Int64Type::type_id:
        case arrow::DoubleType::type_id: {
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            ASSIGN_VALUE(uint64_t);
          }
          break;
        }
        case arrow::StringType::type_id:
        case arrow::BinaryType::type_id: {
          // Binary type
          facebook::velox::StringView* valuebuffers = (facebook::velox::StringView*)(dataptrs[col_index]);
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            size_t length = valuebuffers[j].size();
            const char* value = valuebuffers[j].data();

#ifdef __AVX512BW__
            if (ARROW_PREDICT_TRUE(support_avx512_)) {
              // write the variable value
              if (j < row_start + batch_rows - 2) {
                _mm_prefetch(valuebuffers[j + 1].data(), _MM_HINT_T0);
                _mm_prefetch(valuebuffers[j + 2].data(), _MM_HINT_T0);
              }
              size_t k;
              for (k = 0; k + 32 < length; k += 32) {
                __m256i v = _mm256_loadu_si256((const __m256i*)(value + k));
                _mm256_storeu_si256((__m256i*)(buffer_address_ + offsets_[j] + buffer_cursor_[j] + k), v);
              }
              // create some bits of "1", num equals length
              auto mask = (1L << (length - k)) - 1;
              __m256i v = _mm256_maskz_loadu_epi8(mask, value + k);
              _mm256_mask_storeu_epi8(buffer_address_ + offsets_[j] + buffer_cursor_[j] + k, mask, v);
              _mm_prefetch(&valuebuffers[j + 128 / sizeof(facebook::velox::StringView)], _MM_HINT_T0);
              _mm_prefetch(&offsets_[j], _MM_HINT_T1);
            } else
#endif
            {
              // write the variable value
              memcpy(buffer_address_ + offsets_[j] + buffer_cursor_[j], value, length);
            }

            // write the offset and size
            int64_t offsetAndSize = ((int64_t)buffer_cursor_[j] << 32) | length;
            *(int64_t*)(buffer_address_ + offsets_[j] + field_offset) = offsetAndSize;
            buffer_cursor_[j] += RoundNumberOfBytesToNearestWord(length);
          }
          break;
        }
        case arrow::Decimal128Type::type_id: {
          for (auto j = 0; j < num_rows_; j++) {
            if (array->typeKind() == velox::TypeKind::SHORT_DECIMAL) {
              auto shortDecimal = array->asFlatVector<velox::UnscaledShortDecimal>()->rawValues();
              // Get the long value and write the long value
              // Refer to the int64_t() method of Decimal128
              int64_t long_value = shortDecimal[j].unscaledValue();
              memcpy(buffer_address_ + offsets_[j] + field_offset, &long_value, sizeof(long));
            } else {
              auto longDecimal = array->asFlatVector<velox::UnscaledLongDecimal>()->rawValues();

              int32_t size;
              velox::int128_t veloxInt128 = longDecimal[j].unscaledValue();

              velox::int128_t orignal_value = veloxInt128;
              int64_t high = veloxInt128 >> 64;
              uint64_t lower = (uint64_t)orignal_value;

              auto out = ToByteArray(arrow::Decimal128(high, lower), &size);
              assert(size <= 16);
              // write the variable value
              memcpy(buffer_address_ + buffer_cursor_[j] + offsets_[j], &out[0], size);
              // write the offset and size
              int64_t offsetAndSize = ((int64_t)buffer_cursor_[j] << 32) | size;
              memcpy(buffer_address_ + offsets_[j] + field_offset, &offsetAndSize, sizeof(int64_t));

              // Update the cursor of the buffer.
              buffer_cursor_[j] += 16;
            }
          }
          break;
        }
        case arrow::Time64Type::type_id: {
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            auto write_address = reinterpret_cast<char*>(buffer_address_ + offsets_[j] + field_offset);
            velox::row::UnsafeRowSerializer::serialize<velox::TimestampType>(array, write_address, j);
          }
          break;
        }
        default: {
          return arrow::Status::Invalid("Unsupported data type: " + typevec[col_index]);
        }
      }
    } else {
      switch (typevec[col_index]) {
        // We should keep supported types consistent with that in #buildCheck of GlutenColumnarToRowExec.scala.
        case arrow::BooleanType::type_id: {
          // Boolean type
          auto bool_array = array->asFlatVector<bool>();
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            if (!array->isNullAt(j)) {
              auto value = bool_array->valueAt(j);
              auto p = reinterpret_cast<bool*>(buffer_address_ + offsets_[j] + field_offset);
              *p = value;
            } else {
              SetNullAt(buffer_address_, offsets_[j], field_offset, col_index);
            }
          }
          break;
        }
        case arrow::Int8Type::type_id: {
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            if (!array->isNullAt(j)) {
              ASSIGN_VALUE(uint8_t);
            } else {
              SetNullAt(buffer_address_, offsets_[j], field_offset, col_index);
            }
          }
          break;
        }
        case arrow::Int16Type::type_id: {
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            if (!array->isNullAt(j)) {
              ASSIGN_VALUE(uint16_t);
            } else {
              SetNullAt(buffer_address_, offsets_[j], field_offset, col_index);
            }
          }
          break;
        }
        case arrow::FloatType::type_id:
        case arrow::Int32Type::type_id:
        case arrow::Date32Type::type_id: {
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            if (!array->isNullAt(j)) {
              ASSIGN_VALUE(uint32_t);
            } else {
              SetNullAt(buffer_address_, offsets_[j], field_offset, col_index);
            }
          }
          break;
        }
        case arrow::Int64Type::type_id:
        case arrow::DoubleType::type_id: {
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            if (!array->isNullAt(j)) {
              ASSIGN_VALUE(uint64_t);
            } else {
              SetNullAt(buffer_address_, offsets_[j], field_offset, col_index);
            }
          }
          break;
        }
        case arrow::StringType::type_id:
        case arrow::BinaryType::type_id: {
          // Binary type
          facebook::velox::StringView* valuebuffers = (facebook::velox::StringView*)(dataptrs[col_index]);
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            if (!array->isNullAt(j)) {
              size_t length = valuebuffers[j].size();
              const char* value = valuebuffers[j].data();

#ifdef __AVX512BW__
              if (ARROW_PREDICT_TRUE(support_avx512_)) {
                // write the variable value
                if (j < row_start + batch_rows - 2) {
                  _mm_prefetch(valuebuffers[j + 1].data(), _MM_HINT_T0);
                  _mm_prefetch(valuebuffers[j + 2].data(), _MM_HINT_T0);
                }
                size_t k;
                for (k = 0; k + 32 < length; k += 32) {
                  __m256i v = _mm256_loadu_si256((const __m256i*)(value + k));
                  _mm256_storeu_si256((__m256i*)(buffer_address_ + offsets_[j] + buffer_cursor_[j] + k), v);
                }
                // create some bits of "1", num equals length
                auto mask = (1L << (length - k)) - 1;
                __m256i v = _mm256_maskz_loadu_epi8(mask, value + k);
                _mm256_mask_storeu_epi8(buffer_address_ + offsets_[j] + buffer_cursor_[j] + k, mask, v);
                _mm_prefetch(&valuebuffers[j + 128 / sizeof(facebook::velox::StringView)], _MM_HINT_T0);
              } else
#endif
              {
                // write the variable value
                memcpy(buffer_address_ + offsets_[j] + buffer_cursor_[j], value, length);
              }

              // write the offset and size
              int64_t offsetAndSize = ((int64_t)buffer_cursor_[j] << 32) | length;
              *(int64_t*)(buffer_address_ + offsets_[j] + field_offset) = offsetAndSize;
              buffer_cursor_[j] += RoundNumberOfBytesToNearestWord(length);
            } else {
              SetNullAt(buffer_address_, offsets_[j], field_offset, col_index);
            }
          }
          break;
        }
        case arrow::Decimal128Type::type_id: {
          for (auto j = 0; j < num_rows_; j++) {
            if (array->isNullAt(j)) {
              SetNullAt(buffer_address_, offsets_[j], field_offset, col_index);
            } else {
              if (array->typeKind() == velox::TypeKind::SHORT_DECIMAL) {
                auto shortDecimal = array->asFlatVector<velox::UnscaledShortDecimal>()->rawValues();
                // Get the long value and write the long value
                // Refer to the int64_t() method of Decimal128
                int64_t long_value = shortDecimal[j].unscaledValue();
                memcpy(buffer_address_ + offsets_[j] + field_offset, &long_value, sizeof(long));
              } else {
                auto longDecimal = array->asFlatVector<velox::UnscaledLongDecimal>()->rawValues();
                int32_t size;
                velox::int128_t veloxInt128 = longDecimal[j].unscaledValue();

                velox::int128_t orignal_value = veloxInt128;
                int64_t high = veloxInt128 >> 64;
                uint64_t lower = (uint64_t)orignal_value;

                auto out = ToByteArray(arrow::Decimal128(high, lower), &size);
                assert(size <= 16);

                // write the variable value
                memcpy(buffer_address_ + buffer_cursor_[j] + offsets_[j], &out[0], size);
                // write the offset and size
                int64_t offsetAndSize = ((int64_t)buffer_cursor_[j] << 32) | size;
                memcpy(buffer_address_ + offsets_[j] + field_offset, &offsetAndSize, sizeof(int64_t));

                // Update the cursor of the buffer.
                buffer_cursor_[j] += 16;
              }
            }
          }
          break;
        }
        case arrow::Time64Type::type_id: {
          for (auto j = row_start; j < row_start + batch_rows; j++) {
            if (!array->isNullAt(j)) {
              auto write_address = reinterpret_cast<char*>(buffer_address_ + offsets_[j] + field_offset);
              auto vec = vecs_[col_index];
              velox::row::UnsafeRowSerializer::serialize<velox::TimestampType>(vec, write_address, j);
            } else {
              SetNullAt(buffer_address_, offsets_[j], field_offset, col_index);
            }
          }
          break;
        }
        default: {
          return arrow::Status::Invalid("Unsupported data type: " + typevec[col_index]);
        }
      }
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxColumnarToRowConverter::Write() {
  std::vector<facebook::velox::VectorPtr>& arrays = vecs_;
  std::vector<const uint8_t*> dataptrs;

  dataptrs.resize(num_cols_);
  std::vector<uint8_t> nullvec;
  nullvec.resize(num_cols_, 0);

  std::vector<arrow::Type::type> typevec;
  std::vector<uint8_t> typewidth;

  typevec.resize(num_cols_);
  // Store bytes for different fixed width types
  typewidth.resize(num_cols_);

  for (auto col_index = 0; col_index < num_cols_; col_index++) {
    facebook::velox::VectorPtr array = arrays[col_index];
    auto buf = array->values();

    // If nullvec[col_index]  equals 1, means no null value in this column
    nullvec[col_index] = !(array->mayHaveNulls());
    typevec[col_index] = schema_->field(col_index)->type()->id();
    // calculate bytes from bit_num
    typewidth[col_index] = arrow::bit_width(typevec[col_index]) >> 3;

    if (arrow::bit_width(typevec[col_index]) > 1 || typevec[col_index] == arrow::StringType::type_id ||
        typevec[col_index] == arrow::BinaryType::type_id || typevec[col_index] == arrow::BooleanType::type_id) {
      dataptrs[col_index] = buf->as<uint8_t>();
    } else {
      return arrow::Status::Invalid(
          "Type " + schema_->field(col_index)->type()->name() + " is not supported in VeloxToRow conversion.");
    }
  }

  int32_t i = 0;
  // iterate 16K each batch, since the L1 cache is 48K
  auto batch_num = 16 * 1024 / (16 + avg_rowsize_);
  batch_num = (batch_num == 0 || batch_num > num_rows_) ? num_rows_ : batch_num;
  // file BATCH_ROW_NUM rows each time, fill by column. Make sure the row is in L1/L2 cache
  for (; i + batch_num < num_rows_; i += batch_num) {
    RETURN_NOT_OK(FillBuffer(i, batch_num, dataptrs, nullvec, typevec, typewidth));
  }

  if (i < num_rows_)
    RETURN_NOT_OK(FillBuffer(i, num_rows_ - i, dataptrs, nullvec, typevec, typewidth));

  return arrow::Status::OK();
}
} // namespace gluten
