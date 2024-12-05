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
#include <velox/common/base/SuccinctPrinter.h>
#include <cstdint>

#include "memory/VeloxColumnarBatch.h"
#include "utils/Exception.h"
#include "velox/row/UnsafeRowDeserializers.h"
#include "velox/row/UnsafeRowFast.h"

using namespace facebook;

namespace gluten {

std::vector<size_t> VeloxColumnarToRowConverter::refreshStates(
    facebook::velox::RowVectorPtr rowVector,
    int64_t startRow) {
  auto vectorLength = rowVector->size();
  numCols_ = rowVector->childrenSize();

  fast_ = std::make_unique<velox::row::UnsafeRowFast>(rowVector);

  int64_t totalMemorySize;

  std::vector<size_t> bufferOffsets;
  bufferOffsets.push_back(0);

  if (auto fixedRowSize = velox::row::UnsafeRowFast::fixedRowSize(velox::asRowType(rowVector->type()))) {
    auto rowSize = fixedRowSize.value();
    // make sure it has at least one row
    numRows_ = std::max<int32_t>(1, std::min<int64_t>(memThreshold_ / rowSize, vectorLength - startRow));
    totalMemorySize = numRows_ * rowSize;
    bufferOffsets.reserve(numRows_);
    for (auto i = 1; i < numRows_; ++i) {
      bufferOffsets.push_back(i * rowSize);
    }
  } else {
    // Calculate the first row size
    totalMemorySize = fast_->rowSize(startRow);

    auto endRow = startRow + 1;
    for (; endRow < vectorLength; ++endRow) {
      auto rowSize = fast_->rowSize(endRow);
      if (UNLIKELY(totalMemorySize + rowSize > memThreshold_)) {
        break;
      } else {
        bufferOffsets.push_back(totalMemorySize);
        totalMemorySize += rowSize;
      }
    }
    // Make sure the threshold is larger than the first row size
    numRows_ = endRow - startRow;
  }

  offsets_.clear();
  lengths_.clear();
  offsets_.reserve(numRows_);
  lengths_.reserve(numRows_);

  offsets_.push_back(0);
  for (auto i = 1; i < numRows_; ++i) {
    lengths_.push_back(bufferOffsets[i] - bufferOffsets[i - 1]);
    offsets_.push_back(bufferOffsets[i]);
  }
  lengths_.push_back(totalMemorySize - bufferOffsets.back());

  if (nullptr == veloxBuffers_) {
    veloxBuffers_ = velox::AlignedBuffer::allocate<uint8_t>(totalMemorySize, veloxPool_.get());
  } else if (veloxBuffers_->capacity() < totalMemorySize) {
    velox::AlignedBuffer::reallocate<uint8_t>(&veloxBuffers_, totalMemorySize);
  }

  bufferAddress_ = veloxBuffers_->asMutable<uint8_t>();
  memset(bufferAddress_, 0, sizeof(int8_t) * totalMemorySize);
  return bufferOffsets;
}

void VeloxColumnarToRowConverter::convert(std::shared_ptr<ColumnarBatch> cb, int64_t startRow) {
  auto veloxBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
  auto bufferOffsets = refreshStates(veloxBatch->getRowVector(), startRow);
  fast_->serialize(startRow, numRows_, bufferOffsets.data(), (char*)bufferAddress_);
}

} // namespace gluten
