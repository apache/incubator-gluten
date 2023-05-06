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
  ColumnarToRowConverter(std::shared_ptr<arrow::MemoryPool> arrowPool) : arrowPool_(arrowPool) {}

  virtual ~ColumnarToRowConverter() = default;

  virtual arrow::Status init() = 0;
  virtual arrow::Status write() = 0;

  uint8_t* getBufferAddress() {
    return bufferAddress_;
  }

  const std::vector<int32_t>& getOffsets() const {
    return offsets_;
  }

  const std::vector<int32_t, boost::alignment::aligned_allocator<int32_t, 32>>& getLengths() const {
    return lengths_;
  }

 protected:
  bool supportAvx512_;
  std::shared_ptr<arrow::MemoryPool> arrowPool_;
  std::vector<int32_t> bufferCursor_;
  std::shared_ptr<arrow::Buffer> buffer_;
  int32_t nullBitsetWidthInBytes_;
  int32_t numCols_;
  int32_t numRows_;
  uint8_t* bufferAddress_;
  std::vector<int32_t> offsets_;
  std::vector<int32_t, boost::alignment::aligned_allocator<int32_t, 32>> lengths_;

  static int64_t calculateBitSetWidthInBytes(int32_t numFields) {
    return ((numFields + 63) >> 6) << 3;
  }

  static int32_t roundNumberOfBytesToNearestWord(int32_t numBytes) {
    int32_t remainder = numBytes & 0x07; // This is equivalent to `numBytes % 8`
    return numBytes + ((8 - remainder) & 0x7);
  }

  static int64_t calculatedFixeSizePerRow(std::shared_ptr<arrow::Schema> schema, int64_t numCols);

  static int64_t getFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index) {
    return nullBitsetWidthInBytes + 8L * index;
  }

  static void bitSet(uint8_t* bufferAddress, int32_t index) {
    int64_t mask = 1L << (index & 0x3f); // mod 64 and shift
    int64_t wordOffset = (index >> 6) * 8;
    int64_t word;
    word = *(int64_t*)(bufferAddress + wordOffset);
    int64_t value = word | mask;
    *(int64_t*)(bufferAddress + wordOffset) = value;
  }

  static void setNullAt(uint8_t* bufferAddress, int64_t rowOffset, int64_t fieldOffset, int32_t colIndex) {
    bitSet(bufferAddress + rowOffset, colIndex);
    // set the value to 0
    *(int64_t*)(bufferAddress + rowOffset + fieldOffset) = 0;
  }

  static int32_t firstNonzeroLongNum(const std::vector<int32_t>& mag, int32_t length) {
    int32_t fn = 0;
    int32_t i;
    for (i = length - 1; i >= 0 && mag[i] == 0; i--)
      ;
    fn = length - i - 1;
    return fn;
  }

  static int32_t getInt(int32_t n, int32_t sig, const std::vector<int32_t>& mag, int32_t length) {
    if (n < 0)
      return 0;
    if (n >= length)
      return sig < 0 ? -1 : 0;

    int32_t magInt = mag[length - n - 1];
    return (sig >= 0 ? magInt : (n <= firstNonzeroLongNum(mag, length) ? -magInt : ~magInt));
  }

  static int32_t getNumberOfLeadingZeros(uint32_t i);

  static int32_t getBitLengthForInt(uint32_t n) {
    return 32 - getNumberOfLeadingZeros(n);
  }

  static int32_t getBitCount(uint32_t i) {
    // HD, Figure 5-2
    i = i - ((i >> 1) & 0x55555555);
    i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
    i = (i + (i >> 4)) & 0x0f0f0f0f;
    i = i + (i >> 8);
    i = i + (i >> 16);
    return i & 0x3f;
  }

  static int32_t getBitLength(int32_t sig, const std::vector<int32_t>& mag, int32_t len);

  static std::vector<uint32_t> convertMagArray(int64_t newHigh, uint64_t newLow, int32_t* size);

  /// This method refer to the BigInterger#toByteArray() method in Java side.
  static std::array<uint8_t, 16> toByteArray(arrow::Decimal128 value, int32_t* length);
};

} // namespace gluten
