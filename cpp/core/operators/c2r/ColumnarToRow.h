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

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>

#include <boost/align.hpp>

namespace gluten {

class ColumnarToRowConverter {
 public:
  ColumnarToRowConverter(std::shared_ptr<arrow::MemoryPool> arrow_pool) : arrow_pool_(arrow_pool) {
  support_avx512_ = false;
#if defined(__x86_64__)
  support_avx512_ = __builtin_cpu_supports("avx512bw");
#endif
  }

  virtual ~ColumnarToRowConverter() = default;

  virtual arrow::Status Init() = 0;
  virtual arrow::Status Write() = 0;

  uint8_t* GetBufferAddress() {
    return buffer_address_;
  }

  const std::vector<int32_t>& GetOffsets() const {
    return offsets_;
  }

  const std::vector<int32_t, boost::alignment::aligned_allocator<int32_t, 32>>& GetLengths() const {
    return lengths_;
  }

 protected:
  bool support_avx512_;
  std::shared_ptr<arrow::MemoryPool> arrow_pool_;
  std::vector<int32_t> buffer_cursor_;
  std::shared_ptr<arrow::Buffer> buffer_;
  int32_t nullBitsetWidthInBytes_;
  int32_t num_cols_;
  int32_t num_rows_;
  uint8_t* buffer_address_;
  std::vector<int32_t> offsets_;
  uint32_t avg_rowsize_;
  std::vector<int32_t, boost::alignment::aligned_allocator<int32_t, 32>> lengths_;

  inline int64_t CalculateBitSetWidthInBytes(int32_t numFields) {
    return ((numFields + 63) >> 6) << 3;
  }

  inline int32_t RoundNumberOfBytesToNearestWord(int32_t numBytes) {
    int32_t remainder = numBytes & 0x07; // This is equivalent to `numBytes % 8`
    return numBytes + ((8 - remainder) & 0x7);
  }

  int64_t CalculatedFixeSizePerRow(std::shared_ptr<arrow::Schema> schema, int64_t num_cols);

  inline int64_t GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index) {
    return nullBitsetWidthInBytes + 8L * index;
  }

  inline void BitSet(uint8_t* buffer_address, int32_t index) {
    int64_t mask = 1L << (index & 0x3f); // mod 64 and shift
    int64_t wordOffset = (index >> 6) * 8;
    int64_t word;
    word = *(int64_t*)(buffer_address + wordOffset);
    int64_t value = word | mask;
    *(int64_t*)(buffer_address + wordOffset) = value;
  }

  inline void SetNullAt(uint8_t* buffer_address, int64_t row_offset, int64_t field_offset, int32_t col_index) {
    BitSet(buffer_address + row_offset, col_index);
    // set the value to 0
    // already reset all block
    //*(int64_t*)(buffer_address + row_offset + field_offset) = 0;
  }

  inline int32_t FirstNonzeroLongNum(const std::vector<int32_t>& mag, int32_t length) {
    int32_t fn = 0;
    int32_t i;
    for (i = length - 1; i >= 0 && mag[i] == 0; i--)
      ;
    fn = length - i - 1;
    return fn;
  }

  inline int32_t GetInt(int32_t n, int32_t sig, const std::vector<int32_t>& mag, int32_t length) {
    if (n < 0)
      return 0;
    if (n >= length)
      return sig < 0 ? -1 : 0;

    int32_t magInt = mag[length - n - 1];
    return (sig >= 0 ? magInt : (n <= FirstNonzeroLongNum(mag, length) ? -magInt : ~magInt));
  }
  inline int32_t GetNumberOfLeadingZeros(uint32_t i) {
    // TODO: we can get faster implementation by gcc build-in function
    // HD, Figure 5-6
    if (i == 0)
      return 32;
    int32_t n = 1;
    if (i >> 16 == 0) {
      n += 16;
      i <<= 16;
    }
    if (i >> 24 == 0) {
      n += 8;
      i <<= 8;
    }
    if (i >> 28 == 0) {
      n += 4;
      i <<= 4;
    }
    if (i >> 30 == 0) {
      n += 2;
      i <<= 2;
    }
    n -= i >> 31;
    return n;
  }
  inline int32_t GetBitLengthForInt(uint32_t n) {
    return 32 - GetNumberOfLeadingZeros(n);
  }

  inline int32_t GetBitCount(uint32_t i) {
    // HD, Figure 5-2
    i = i - ((i >> 1) & 0x55555555);
    i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
    i = (i + (i >> 4)) & 0x0f0f0f0f;
    i = i + (i >> 8);
    i = i + (i >> 16);
    return i & 0x3f;
  }

  int32_t GetBitLength(int32_t sig, const std::vector<int32_t>& mag, int32_t len);

  std::vector<uint32_t> ConvertMagArray(int64_t new_high, uint64_t new_low, int32_t* size);

  /// This method refer to the BigInterger#toByteArray() method in Java side.
  std::array<uint8_t, 16> ToByteArray(arrow::Decimal128 value, int32_t* length);
};

} // namespace gluten
