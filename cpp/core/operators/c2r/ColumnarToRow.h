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

#include <cstdint>
#include "memory/ColumnarBatch.h"

namespace gluten {

class ColumnarToRowConverter {
 public:
  ColumnarToRowConverter() {}

  virtual ~ColumnarToRowConverter() = default;

  // We will start conversion from the 'rowId' row of 'cb'. The maximum memory consumption during the grabbing and
  // swapping process is 'memoryThreshold' bytes. The number of rows successfully converted is stored in the 'numRows_'
  // variable.
  virtual void convert(std::shared_ptr<ColumnarBatch> cb = nullptr, int64_t startRow = 0) = 0;

  virtual int32_t numRows() {
    return numRows_;
  }

  uint8_t* getBufferAddress() const {
    return bufferAddress_;
  }

  const std::vector<int32_t>& getOffsets() const {
    return offsets_;
  }

  const std::vector<int32_t>& getLengths() const {
    return lengths_;
  }

 protected:
  int32_t numCols_;
  int32_t numRows_;
  uint8_t* bufferAddress_;
  std::vector<int32_t> offsets_;
  std::vector<int32_t> lengths_;
};

} // namespace gluten
