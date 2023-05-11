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
#include <arrow/status.h>
#include <arrow/util/decimal.h>
#if defined(__x86_64__)
#include <immintrin.h>
#endif

namespace gluten {

uint32_t x7[8] __attribute__((aligned(32))) = {0x7, 0x7, 0x7, 0x7, 0x7, 0x7, 0x7, 0x7};
uint32_t x8[8] __attribute__((aligned(32))) = {0x8, 0x8, 0x8, 0x8, 0x8, 0x8, 0x8, 0x8};

arrow::Status ArrowColumnarToRowConverter::init() {
  supportAvx512_ = false;
#if defined(__x86_64__)
  supportAvx512_ = __builtin_cpu_supports("avx512bw");
#endif

  numRows_ = rb_->num_rows();
  numCols_ = rb_->num_columns();
  // Calculate the initial size
  nullBitsetWidthInBytes_ = calculateBitSetWidthInBytes(numCols_);

  int32_t fixedSizePerRow = calculatedFixeSizePerRow(rb_->schema(), numCols_);

  // Initialize the offsets_ , lengths_, buffer_cursor_
  lengths_.resize(numRows_, fixedSizePerRow);
  std::fill(lengths_.begin(), lengths_.end(), fixedSizePerRow);

  offsets_.resize(numRows_ + 1);
  bufferCursor_.resize(numRows_, nullBitsetWidthInBytes_ + 8 * numCols_);

  // Calculated the lengths_
  for (auto i = 0; i < numCols_; i++) {
    auto array = rb_->column(i);
    if (arrow::is_binary_like(array->type_id())) {
      auto binaryArray = std::static_pointer_cast<arrow::BinaryArray>(array);
      using offset_type = typename arrow::BinaryType::offset_type;
      const offset_type* offsetarray = binaryArray->raw_value_offsets();
      int32_t j = 0;
      int32_t* lengthData = lengths_.data();

#ifdef __AVX512BW__
      if (ARROW_PREDICT_TRUE(supportAvx512_)) {
        __m256i x78x = _mm256_load_si256((__m256i*)x7);
        __m256i x88x = _mm256_load_si256((__m256i*)x8);
        __m256i offsetarray18x;
        if (j + 16 < numRows_) {
          // The address shall be 64-bytes aligned. But currently if use
          // _mm256_load_si256, will crash. Track with
          // https://github.com/oap-project/gazelle_plugin/issues/927
          offsetarray18x = _mm256_loadu_si256((__m256i*)&offsetarray[j]);
        }
        for (; j + 16 < numRows_; j += 8) {
          __m256i offsetarray8x = offsetarray18x;
          offsetarray18x = _mm256_loadu_si256((__m256i*)&offsetarray[j + 8]);

          __m256i length8x = _mm256_alignr_epi32(offsetarray18x, offsetarray8x, 0x1);
          length8x = _mm256_sub_epi32(length8x, offsetarray8x);

          __m256i reminder8x = _mm256_and_si256(length8x, x78x);
          reminder8x = _mm256_sub_epi32(x88x, reminder8x);
          reminder8x = _mm256_and_si256(reminder8x, x78x);
          reminder8x = _mm256_add_epi32(reminder8x, length8x);
          __m256i dstLength8x = _mm256_load_si256((__m256i*)lengthData);
          dstLength8x = _mm256_add_epi32(dstLength8x, reminder8x);
          _mm256_store_si256((__m256i*)lengthData, dstLength8x);
          lengthData += 8;
          _mm_prefetch(&offsetarray[j + (128 + 128) / sizeof(offset_type)], _MM_HINT_T0);
        }
      }
#endif

      for (; j < numRows_; j++) {
        offset_type length = offsetarray[j + 1] - offsetarray[j];
        *lengthData += roundNumberOfBytesToNearestWord(length);
        lengthData++;
      }
    }
  }
  // Calculated the offsets_  and total memory size based on lengths_
  int64_t totalMemorySize = lengths_[0];
  offsets_[0] = 0;
  for (auto i = 1; i < numRows_; i++) {
    offsets_[i] = totalMemorySize;
    totalMemorySize += lengths_[i];
  }
  offsets_[numRows_] = totalMemorySize;

  // allocate one more cache line to ease avx operations
  if (buffer_ == nullptr || buffer_->capacity() < totalMemorySize + 64) {
    ARROW_ASSIGN_OR_RAISE(buffer_, AllocateBuffer(totalMemorySize + 64, arrowPool_.get()));
    memset(buffer_->mutable_data(), 0, buffer_->capacity());
  }

  bufferAddress_ = buffer_->mutable_data();

  return arrow::Status::OK();
}

arrow::Status ArrowColumnarToRowConverter::fillBuffer(
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
    bool supportAvx512) {
#ifdef __AVX512BW__
  if (ARROW_PREDICT_TRUE(supportAvx512)) {
    __m256i fill08x = {0LL};
    fill08x = _mm256_xor_si256(fill08x, fill08x);
    for (auto j = rowStart; j < rowStart + batchRows; j++) {
      auto rowlength = offsets[j + 1] - offsets[j];
      for (auto p = 0; p < rowlength + 32; p += 32) {
        _mm256_storeu_si256((__m256i*)(bufferAddress + offsets[j]), fill08x);
        _mm_prefetch(bufferAddress + offsets[j] + 128, _MM_HINT_T0);
      }
    }
  }
#endif

  for (auto colIndex = 0; colIndex < numCols; colIndex++) {
    auto& array = arrays[colIndex];
    int64_t fieldOffset = nullBitsetWidthInBytes + (colIndex << 3L);

    switch (typevec[colIndex]) {
      // We should keep supported types consistent with that in #buildCheck of GlutenColumnarToRowExec.scala.
      case arrow::BooleanType::type_id: {
        // Boolean type
        auto boolArray = std::static_pointer_cast<arrow::BooleanArray>(array);

        for (auto j = rowStart; j < rowStart + batchRows; j++) {
          if (nullvec[colIndex] || (!array->IsNull(j))) {
            auto value = boolArray->Value(j);
            memcpy(bufferAddress + offsets[j] + fieldOffset, &value, sizeof(bool));
          } else {
            setNullAt(bufferAddress, offsets[j], fieldOffset, colIndex);
          }
        }
        break;
      }
      case arrow::StringType::type_id:
      case arrow::BinaryType::type_id: {
        // Binary type
        using offset_type = typename arrow::BinaryType::offset_type;
        offset_type* binaryOffsets = (offset_type*)(dataptrs[colIndex][1]);
        for (auto j = rowStart; j < rowStart + batchRows; j++) {
          if (nullvec[colIndex] || (!array->IsNull(j))) {
            offset_type length = binaryOffsets[j + 1] - binaryOffsets[j];
            auto value = &dataptrs[colIndex][2][binaryOffsets[j]];

#ifdef __AVX512BW__
            if (ARROW_PREDICT_TRUE(supportAvx512)) {
              // write the variable value
              offset_type k;
              for (k = 0; k + 32 < length; k += 32) {
                __m256i v = _mm256_loadu_si256((const __m256i*)(value + k));
                _mm256_storeu_si256((__m256i*)(bufferAddress + offsets[j] + bufferCursor[j] + k), v);
              }
              // create some bits of "1", num equals length
              auto mask = (1L << (length - k)) - 1;
              __m256i v = _mm256_maskz_loadu_epi8(mask, value + k);
              _mm256_mask_storeu_epi8(bufferAddress + offsets[j] + bufferCursor[j] + k, mask, v);
            } else
#endif
            {
              // write the variable value
              memcpy(bufferAddress + offsets[j] + bufferCursor[j], value, length);
            }

            // write the offset and size
            int64_t offsetAndSize = ((int64_t)bufferCursor[j] << 32) | length;
            *(int64_t*)(bufferAddress + offsets[j] + fieldOffset) = offsetAndSize;
            bufferCursor[j] += roundNumberOfBytesToNearestWord(length);
          } else {
            setNullAt(bufferAddress, offsets[j], fieldOffset, colIndex);
          }
        }
        break;
      }
      case arrow::Decimal128Type::type_id: {
        auto outArray = dynamic_cast<arrow::Decimal128Array*>(array.get());
        auto dtype = dynamic_cast<arrow::Decimal128Type*>(outArray->type().get());

        int32_t precision = dtype->precision();

        for (auto j = rowStart; j < rowStart + batchRows; j++) {
          const arrow::Decimal128 outValue(outArray->GetValue(j));
          bool flag = outArray->IsNull(j);

          if (precision <= 18) {
            if (!flag) {
              // Get the long value and write the long value
              // Refer to the int64_t() method of Decimal128
              int64_t longValue = static_cast<int64_t>(outValue.low_bits());
              memcpy(bufferAddress + offsets[j] + fieldOffset, &longValue, sizeof(long));
            } else {
              setNullAt(bufferAddress, offsets[j], fieldOffset, colIndex);
            }
          } else {
            if (flag) {
              setNullAt(bufferAddress, offsets[j], fieldOffset, colIndex);
            } else {
              int32_t size;
              auto out = toByteArray(outValue, &size);
              assert(size <= 16);

              // write the variable value
              memcpy(bufferAddress + bufferCursor[j] + offsets[j], &out[0], size);
              // write the offset and size
              int64_t offsetAndSize = ((int64_t)bufferCursor[j] << 32) | size;
              memcpy(bufferAddress + offsets[j] + fieldOffset, &offsetAndSize, sizeof(int64_t));
            }

            // Update the cursor of the buffer.
            int64_t newCursor = bufferCursor[j] + 16;
            bufferCursor[j] = newCursor;
          }
        }
        break;
      }
      default: {
        if (typewidth[colIndex] > 0) {
          auto dataptr = dataptrs[colIndex][1];
          auto mask = (1L << (typewidth[colIndex])) - 1;
          (void)mask; // suppress warning
#if defined(__x86_64__)
          auto shift = _tzcnt_u32(typewidth[colIndex]);
#else
          auto shift = __builtin_ctz((uint32_t)typewidth[colIndex]);
#endif
          auto bufferAddressTmp = bufferAddress + fieldOffset;
          for (auto j = rowStart; j < rowStart + batchRows; j++) {
            if (nullvec[colIndex] || (!array->IsNull(j))) {
              const uint8_t* srcptr = dataptr + (j << shift);
#ifdef __AVX512BW__
              if (ARROW_PREDICT_TRUE(supportAvx512)) {
                __m256i v = _mm256_maskz_loadu_epi8(mask, srcptr);
                _mm256_mask_storeu_epi8(bufferAddressTmp + offsets[j], mask, v);
                _mm_prefetch(srcptr + 64, _MM_HINT_T0);
              } else
#endif
              {
                memcpy(bufferAddressTmp + offsets[j], srcptr, typewidth[colIndex]);
              }
            } else {
              setNullAt(bufferAddress, offsets[j], fieldOffset, colIndex);
            }
          }
          break;
        } else {
          return arrow::Status::Invalid("Unsupported data type: " + std::to_string(typevec[colIndex]));
        }
      }
    }
  }
  return arrow::Status::OK();
}

arrow::Status ArrowColumnarToRowConverter::write() {
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::vector<const uint8_t*>> dataptrs;
  std::vector<int64_t> colArrdataOffsets;
  dataptrs.resize(numCols_);
  colArrdataOffsets.resize(numCols_);
  std::vector<uint8_t> nullvec;
  nullvec.resize(numCols_, 0);

  std::vector<arrow::Type::type> typevec;
  std::vector<uint8_t> typewidth;

  typevec.resize(numCols_);
  // Store bytes for different fixed width types
  typewidth.resize(numCols_);

  for (auto colIndex = 0; colIndex < numCols_; colIndex++) {
    auto array = rb_->column(colIndex);
    arrays.push_back(array);
    auto arraydata = array->data();
    auto bufs = arraydata->buffers;
    colArrdataOffsets[colIndex] = arraydata->offset;

    // If nullvec[col_index]  equals 1, means no null value in this column
    nullvec[colIndex] = (array->null_count() == 0);
    typevec[colIndex] = array->type_id();
    // calculate bytes from bit_num
    typewidth[colIndex] = arrow::bit_width(typevec[colIndex]) >> 3;

    if (arrow::bit_width(array->type_id()) > 1) {
      if (bufs[0]) {
        dataptrs[colIndex].push_back(bufs[0]->data());
      } else {
        dataptrs[colIndex].push_back(nullptr);
      }
      dataptrs[colIndex].push_back(bufs[1]->data() + arraydata->offset * (arrow::bit_width(array->type_id()) >> 3));
    } else if (array->type_id() == arrow::StringType::type_id || array->type_id() == arrow::BinaryType::type_id) {
      if (bufs[0]) {
        dataptrs[colIndex].push_back(bufs[0]->data());
      } else {
        dataptrs[colIndex].push_back(nullptr);
      }

      auto binaryArray = (arrow::BinaryArray*)(array.get());
      dataptrs[colIndex].push_back((uint8_t*)(binaryArray->raw_value_offsets()));
      dataptrs[colIndex].push_back((uint8_t*)(binaryArray->raw_data()));
    }
  }

  int32_t i = 0;
#define BATCH_ROW_NUM 16
  for (; i + BATCH_ROW_NUM < numRows_; i += BATCH_ROW_NUM) {
    RETURN_NOT_OK(fillBuffer(
        i,
        BATCH_ROW_NUM,
        dataptrs,
        nullvec,
        bufferAddress_,
        offsets_,
        bufferCursor_,
        numCols_,
        numRows_,
        nullBitsetWidthInBytes_,
        typevec,
        typewidth,
        arrays,
        supportAvx512_));
  }

  for (; i < numRows_; i++) {
    RETURN_NOT_OK(fillBuffer(
        i,
        1,
        dataptrs,
        nullvec,
        bufferAddress_,
        offsets_,
        bufferCursor_,
        numCols_,
        numRows_,
        nullBitsetWidthInBytes_,
        typevec,
        typewidth,
        arrays,
        supportAvx512_));
  }

  return arrow::Status::OK();
}

} // namespace gluten
