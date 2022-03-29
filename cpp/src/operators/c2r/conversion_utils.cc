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

#include "conversion_utils.h"

namespace gazellejni {
namespace columnartorow {

int64_t CalculateBitSetWidthInBytes(int32_t numFields) {
  return ((numFields + 63) / 64) * 8;
}

int64_t RoundNumberOfBytesToNearestWord(int64_t numBytes) {
  int64_t remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
  if (remainder == 0) {
    return numBytes;
  } else {
    return numBytes + (8 - remainder);
  }
}

int64_t CalculatedFixeSizePerRow(std::shared_ptr<arrow::Schema> schema,
                                 int64_t num_cols) {
  std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();
  // Calculate the decimal col num when the precision >18
  int32_t count = 0;
  for (auto i = 0; i < num_cols; i++) {
    auto type = fields[i]->type();
    if (type->id() == arrow::Decimal128Type::type_id) {
      auto dtype = dynamic_cast<arrow::Decimal128Type*>(type.get());
      int32_t precision = dtype->precision();
      if (precision > 18) count++;
    }
  }

  int64_t fixed_size = CalculateBitSetWidthInBytes(num_cols) + num_cols * 8;
  int64_t decimal_cols_size = count * 16;
  return fixed_size + decimal_cols_size;
}

int64_t GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index) {
  return nullBitsetWidthInBytes + 8L * index;
}

}  // namespace columnartorow
}  // namespace gazellejni