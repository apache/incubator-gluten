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
#include "utils/exception.h"
#include "velox/row/UnsafeRowDeserializers.h"
#include "velox/row/UnsafeRowFast.h"

using namespace facebook;

namespace gluten {

void VeloxColumnarToRowConverter::refreshStates(
    facebook::velox::RowVectorPtr rowVector,
    int64_t rowId,
    int64_t memoryThreshold) {
  auto vectorLength = rowVector->size();
  numCols_ = rowVector->childrenSize();

  fast_ = std::make_unique<velox::row::UnsafeRowFast>(rowVector);

  size_t totalMemorySize = 0;
  if (auto fixedRowSize = velox::row::UnsafeRowFast::fixedRowSize(velox::asRowType(rowVector->type()))) {
    if (memoryThreshold < fixedRowSize.value()) {
      memoryThreshold = fixedRowSize.value();
      LOG(ERROR) << "spark.gluten.sql.columnarToRowMemoryThreshold(" + velox::succinctBytes(memoryThreshold) +
              ") is too small, it can't hold even one row(" + velox::succinctBytes(fixedRowSize.value()) + ")";
    }
    auto rowSize = fixedRowSize.value();
    numRows_ = std::min<int64_t>(memoryThreshold / rowSize, vectorLength - rowId);
    totalMemorySize = rowSize * numRows_;
  } else {
    int64_t i = rowId;
    for (; i < vectorLength; ++i) {
      auto rowSize = fast_->rowSize(i);
      if (UNLIKELY(totalMemorySize + rowSize > memoryThreshold)) {
        if (i == rowId) {
          memoryThreshold = rowSize;
          LOG(ERROR) << "spark.gluten.sql.columnarToRowMemoryThreshold(" + velox::succinctBytes(memoryThreshold) +
                  ") is too small, it can't hold even one row(" + velox::succinctBytes(rowSize) + ")";
        }
        break;
      } else {
        totalMemorySize += rowSize;
      }
    }
    numRows_ = i - rowId;
  }

  if (veloxBuffers_ == nullptr) {
    veloxBuffers_ = velox::AlignedBuffer::allocate<uint8_t>(memoryThreshold, veloxPool_.get());
  }

  bufferAddress_ = veloxBuffers_->asMutable<uint8_t>();
  memset(bufferAddress_, 0, sizeof(int8_t) * totalMemorySize);
}

void VeloxColumnarToRowConverter::convert(std::shared_ptr<ColumnarBatch> cb, int64_t rowId, int64_t memoryThreshold) {
  auto veloxBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
  refreshStates(veloxBatch->getRowVector(), rowId, memoryThreshold);

  // Initialize the offsets_ , lengths_
  lengths_.clear();
  offsets_.clear();
  lengths_.resize(numRows_, 0);
  offsets_.resize(numRows_, 0);

  size_t offset = 0;
  for (auto i = 0; i < numRows_; ++i) {
    auto rowSize = fast_->serialize(rowId + i, (char*)(bufferAddress_ + offset));
    lengths_[i] = rowSize;
    if (i > 0) {
      offsets_[i] = offsets_[i - 1] + lengths_[i - 1];
    }
    offset += rowSize;
  }
}

} // namespace gluten
