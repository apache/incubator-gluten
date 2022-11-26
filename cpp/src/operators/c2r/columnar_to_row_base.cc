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

#include "columnar_to_row_base.h"

#include <arrow/util/decimal.h>
namespace gluten {
namespace columnartorow {

int64_t ColumnarToRowConverterBase::CalculateBitSetWidthInBytes(int32_t numFields) {
  return ((numFields + 63) >> 6) << 3;
}

int32_t ColumnarToRowConverterBase::RoundNumberOfBytesToNearestWord(int32_t numBytes) {
  int32_t remainder = numBytes & 0x07; // This is equivalent to `numBytes % 8`

  return numBytes + ((8 - remainder) & 0x7);
  /*if (remainder == 0) {
    return numBytes;
  } else {
    return numBytes + (8 - remainder);
  }*/
}

int64_t ColumnarToRowConverterBase::CalculatedFixeSizePerRow(std::shared_ptr<arrow::Schema> schema, int64_t num_cols) {
  std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();
  // Calculate the decimal col num when the precision >18
  int32_t count = 0;
  for (auto i = 0; i < num_cols; i++) {
    auto type = fields[i]->type();
    if (type->id() == arrow::Decimal128Type::type_id) {
      auto dtype = dynamic_cast<arrow::Decimal128Type*>(type.get());
      int32_t precision = dtype->precision();
      if (precision > 18)
        count++;
    }
  }

  int64_t fixed_size = CalculateBitSetWidthInBytes(num_cols) + num_cols * 8;
  int64_t decimal_cols_size = count * 16;
  return fixed_size + decimal_cols_size;
}

int64_t ColumnarToRowConverterBase::GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index) {
  return nullBitsetWidthInBytes + 8L * index;
}

void ColumnarToRowConverterBase::BitSet(uint8_t* buffer_address, int32_t index) {
  int64_t mask = 1L << (index & 0x3f); // mod 64 and shift
  int64_t wordOffset = (index >> 6) * 8;
  int64_t word;
  word = *(int64_t*)(buffer_address + wordOffset);
  int64_t value = word | mask;
  *(int64_t*)(buffer_address + wordOffset) = value;
}

void ColumnarToRowConverterBase::SetNullAt(
    uint8_t* buffer_address,
    int64_t row_offset,
    int64_t field_offset,
    int32_t col_index) {
  BitSet(buffer_address + row_offset, col_index);
  // set the value to 0
  *(int64_t*)(buffer_address + row_offset + field_offset) = 0;

  return;
}

int32_t ColumnarToRowConverterBase::FirstNonzeroLongNum(std::vector<int32_t> mag, int32_t length) {
  int32_t fn = 0;
  int32_t i;
  for (i = length - 1; i >= 0 && mag[i] == 0; i--)
    ;
  fn = length - i - 1;
  return fn;
}

int32_t ColumnarToRowConverterBase::GetInt(int32_t n, int32_t sig, std::vector<int32_t> mag, int32_t length) {
  if (n < 0)
    return 0;
  if (n >= length)
    return sig < 0 ? -1 : 0;

  int32_t magInt = mag[length - n - 1];
  return (sig >= 0 ? magInt : (n <= FirstNonzeroLongNum(mag, length) ? -magInt : ~magInt));
}

int32_t ColumnarToRowConverterBase::GetNumberOfLeadingZeros(uint32_t i) {
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

int32_t ColumnarToRowConverterBase::GetBitLengthForInt(uint32_t n) {
  return 32 - GetNumberOfLeadingZeros(n);
}

int32_t ColumnarToRowConverterBase::GetBitCount(uint32_t i) {
  // HD, Figure 5-2
  i = i - ((i >> 1) & 0x55555555);
  i = (i & 0x33333333) + ((i >> 2) & 0x33333333);
  i = (i + (i >> 4)) & 0x0f0f0f0f;
  i = i + (i >> 8);
  i = i + (i >> 16);
  return i & 0x3f;
}

int32_t ColumnarToRowConverterBase::GetBitLength(int32_t sig, std::vector<int32_t> mag, int32_t len) {
  int32_t n = -1;
  if (len == 0) {
    n = 0;
  } else {
    // Calculate the bit length of the magnitude
    int32_t mag_bit_length = ((len - 1) << 5) + GetBitLengthForInt((uint32_t)mag[0]);
    if (sig < 0) {
      // Check if magnitude is a power of two
      bool pow2 = (GetBitCount((uint32_t)mag[0]) == 1);
      for (int i = 1; i < len && pow2; i++)
        pow2 = (mag[i] == 0);

      n = (pow2 ? mag_bit_length - 1 : mag_bit_length);
    } else {
      n = mag_bit_length;
    }
  }
  return n;
}

std::vector<uint32_t> ColumnarToRowConverterBase::ConvertMagArray(int64_t new_high, uint64_t new_low, int32_t* size) {
  std::vector<uint32_t> mag;
  int64_t orignal_low = new_low;
  int64_t orignal_high = new_high;
  mag.push_back(new_high >>= 32);
  mag.push_back((uint32_t)orignal_high);
  mag.push_back(new_low >>= 32);
  mag.push_back((uint32_t)orignal_low);

  int32_t start = 0;
  // remove the front 0
  for (int32_t i = 0; i < 4; i++) {
    if (mag[i] == 0)
      start++;
    if (mag[i] != 0)
      break;
  }

  int32_t length = 4 - start;
  std::vector<uint32_t> new_mag;
  // get the mag after remove the high 0
  for (int32_t i = start; i < 4; i++) {
    new_mag.push_back(mag[i]);
  }

  *size = length;
  return new_mag;
}

/*
 *  This method refer to the BigInterger#toByteArray() method in Java side.
 */
std::array<uint8_t, 16> ColumnarToRowConverterBase::ToByteArray(arrow::Decimal128 value, int32_t* length) {
  int64_t high = value.high_bits();
  uint64_t low = value.low_bits();
  arrow::Decimal128 new_value;
  int32_t sig;
  if (value > 0) {
    new_value = value;
    sig = 1;
  } else if (value < 0) {
    new_value = value.Abs();
    sig = -1;
  } else {
    new_value = value;
    sig = 0;
  }

  int64_t new_high = new_value.high_bits();
  uint64_t new_low = new_value.low_bits();

  std::vector<uint32_t> mag;
  int32_t size;
  mag = ConvertMagArray(new_high, new_low, &size);

  std::vector<int32_t> final_mag;
  for (auto i = 0; i < size; i++) {
    final_mag.push_back(mag[i]);
  }

  int32_t byte_length = GetBitLength(sig, final_mag, size) / 8 + 1;

  std::array<uint8_t, 16> out{{0}};
  uint32_t next_int = 0;
  for (int32_t i = byte_length - 1, bytes_copied = 4, int_index = 0; i >= 0; i--) {
    if (bytes_copied == 4) {
      next_int = GetInt(int_index++, sig, final_mag, size);
      bytes_copied = 1;
    } else {
      next_int >>= 8;
      bytes_copied++;
    }

    out[i] = (uint8_t)next_int;
  }
  *length = byte_length;
  return out;
}
} // namespace columnartorow
} // namespace gluten
