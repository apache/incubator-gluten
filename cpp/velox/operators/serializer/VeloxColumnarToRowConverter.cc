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

#include <arrow/array/array_base.h>
#include <arrow/buffer.h>
#include <arrow/c/abi.h>
#include <arrow/type_traits.h>
#include <arrow/util/decimal.h>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/row/UnsafeRowDeserializers.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;
using arrow::MemoryPool;

namespace gluten {

arrow::Status VeloxColumnarToRowConverter::init() {
  numRows_ = rv_->size();
  numCols_ = rv_->childrenSize();

  fast_ = std::make_unique<velox::row::UnsafeRowFast>(rv_);

  size_t totalMemorySize = 0;
  if (auto fixedRowSize = velox::row::UnsafeRowFast::fixedRowSize(velox::asRowType(rv_->type()))) {
    totalMemorySize += fixedRowSize.value() * numRows_;

  } else {
    for (auto i = 0; i < numRows_; ++i) {
      totalMemorySize += fast_->rowSize(i);
    }
  }

  if (veloxBuffers_ == nullptr) {
    // First allocate memory
    veloxBuffers_ = velox::AlignedBuffer::allocate<uint8_t>(totalMemorySize, veloxPool_.get());
  }

  if (veloxBuffers_->capacity() < totalMemorySize) {
    velox::AlignedBuffer::reallocate<uint8_t>(&veloxBuffers_, totalMemorySize);
  }

  bufferAddress_ = veloxBuffers_->asMutable<uint8_t>();
  memset(bufferAddress_, 0, sizeof(int8_t) * totalMemorySize);
  return arrow::Status::OK();
}

arrow::Status VeloxColumnarToRowConverter::write(std::shared_ptr<ColumnarBatch> cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  rv_ = veloxBatch->getRowVector();
  RETURN_NOT_OK(init());

  // Initialize the offsets_ , lengths_
  lengths_.clear();
  offsets_.clear();
  lengths_.resize(numRows_, 0);
  offsets_.resize(numRows_, 0);

  size_t offset = 0;
  for (auto rowIdx = 0; rowIdx < numRows_; ++rowIdx) {
    auto rowSize = fast_->serialize(rowIdx, (char*)(bufferAddress_ + offset));
    lengths_[rowIdx] = rowSize;
    if (rowIdx > 0) {
      offsets_[rowIdx] = offsets_[rowIdx - 1] + lengths_[rowIdx - 1];
    }
    offset += rowSize;
  }

  return arrow::Status::OK();
}

} // namespace gluten
