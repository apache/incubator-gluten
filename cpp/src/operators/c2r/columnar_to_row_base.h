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
  ColumnarToRowConverter(std::shared_ptr<arrow::MemoryPool> arrow_pool) : arrow_pool_(arrow_pool) {}

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
  std::vector<int32_t, boost::alignment::aligned_allocator<int32_t, 32>> lengths_;

  int64_t CalculateBitSetWidthInBytes(int32_t numFields);

  int32_t RoundNumberOfBytesToNearestWord(int32_t numBytes);

  int64_t CalculatedFixeSizePerRow(std::shared_ptr<arrow::Schema> schema, int64_t num_cols);

  int64_t GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index);

  void BitSet(uint8_t* buffer_address, int32_t index);

  void SetNullAt(uint8_t* buffer_address, int64_t row_offset, int64_t field_offset, int32_t col_index);

  int32_t FirstNonzeroLongNum(const std::vector<int32_t>& mag, int32_t length) const;

  int32_t GetInt(int32_t n, int32_t sig, const std::vector<int32_t>& mag, int32_t length) const;

  int32_t GetNumberOfLeadingZeros(uint32_t i) const;

  int32_t GetBitLengthForInt(uint32_t n) const;

  int32_t GetBitCount(uint32_t i) const;

  int32_t GetBitLength(int32_t sig, const std::vector<int32_t>& mag, int32_t len);

  std::vector<uint32_t> ConvertMagArray(int64_t new_high, uint64_t new_low, int32_t* size);

  /// This method refer to the BigInterger#toByteArray() method in Java side.
  std::array<uint8_t, 16> ToByteArray(arrow::Decimal128 value, int32_t* length);
};

} // namespace gluten
