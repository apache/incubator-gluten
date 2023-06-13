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

#include "ArrowTypeUtils.h"
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

  // ArrowSchema cSchema{};
  // velox::exportToArrow(rv_, cSchema);
  // ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, arrow::ImportSchema(&cSchema));
  // if (numCols_ != schema->num_fields()) {
  //   return arrow::Status::Invalid("Mismatch: num_cols_ != schema->num_fields()");
  // }
  // schema_ = schema;

  // The input is Arrow batch. We need to resume Velox Vector here.
  // vecs_.clear();
  // resumeVeloxVector();

  fast_ = std::make_unique<velox::row::UnsafeRowFast>(rv_);

  // // Calculate the initial size
  // nullBitsetWidthInBytes_ = calculateBitSetWidthInBytes(numCols_);
  // int64_t fixedSizePerRow = calculatedFixeSizePerRow(schema_, numCols_);

  // // Initialize the offsets_ , lengths_, buffer_cursor_
  // lengths_.clear();
  // offsets_.clear();
  // bufferCursor_.clear();

  // lengths_.resize(numRows_, fixedSizePerRow);
  // offsets_.resize(numRows_, 0);

  lengths_.resize(numRows_, 0);
  offsets_.resize(numRows_, 0);

  size_t totalSize = 0;
  if (auto fixedRowSize = velox::row::UnsafeRowFast::fixedRowSize(velox::asRowType(rv_->type()))) {
    totalSize += fixedRowSize.value() * numRows_;

    for (auto i = 0; i < numRows_; ++i) {
      lengths_[i] = fixedRowSize.value();
    }

  } else {
    for (auto i = 0; i < numRows_; ++i) {
      auto rowSize = fast_->rowSize(i);
      totalSize += rowSize;
      lengths_[i] = rowSize;
    }
  }

  for (auto rowIdx = 1; rowIdx < numRows_; rowIdx++) {
    offsets_[rowIdx] = offsets_[rowIdx - 1] + lengths_[rowIdx - 1];
  }

  // bufferCursor_.resize(numRows_, nullBitsetWidthInBytes_ + 8 * numCols_);

  // // Calculated the lengths_
  // for (int64_t colIdx = 0; colIdx < numCols_; colIdx++) {
  //   std::shared_ptr<arrow::Field> field = schema_->field(colIdx);
  //   if (arrow::is_binary_like(field->type()->id())) {
  //     auto strViews = vecs_[colIdx]->asFlatVector<velox::StringView>()->rawValues();
  //     for (int rowIdx = 0; rowIdx < numRows_; rowIdx++) {
  //       auto length = strViews[rowIdx].size();
  //       int64_t bytes = roundNumberOfBytesToNearestWord(length);
  //       lengths_[rowIdx] += bytes;
  //     }
  //   }
  // }

  // // Calculated the offsets_  and total memory size based on lengths_
  // int64_t totalMemorySize = lengths_[0];
  // for (int64_t rowIdx = 1; rowIdx < numRows_; rowIdx++) {
  //   offsets_[rowIdx] = offsets_[rowIdx - 1] + lengths_[rowIdx - 1];
  //   totalMemorySize += lengths_[rowIdx];
  // }

  ARROW_ASSIGN_OR_RAISE(buffer_, arrow::AllocateBuffer(totalSize, arrowPool_.get()));
  // if (buffer_ == nullptr) {
  //   // First allocate memory
  //   ARROW_ASSIGN_OR_RAISE(buffer_, arrow::AllocateBuffer(totalMemorySize * 1.2, arrowPool_.get()));
  // } else if (buffer_->capacity() < totalMemorySize) {
  //   ARROW_ASSIGN_OR_RAISE(buffer_, arrow::AllocateBuffer(totalMemorySize * 1.2, arrowPool_.get()));
  // }

  bufferAddress_ = buffer_->mutable_data();
  memset(bufferAddress_, 0, sizeof(int8_t) * totalSize);
  return arrow::Status::OK();
}

void VeloxColumnarToRowConverter::resumeVeloxVector() {
  vecs_.reserve(numCols_);
  for (int colIdx = 0; colIdx < numCols_; colIdx++) {
    vecs_.push_back(rv_->childAt(colIdx));
  }
}

arrow::Status VeloxColumnarToRowConverter::write(std::shared_ptr<ColumnarBatch> cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  rv_ = veloxBatch->getRowVector();
  RETURN_NOT_OK(init());
  size_t offset = 0;
  for (auto i = 0; i < numRows_; ++i) {
    auto rowSize = fast_->serialize(i, (char*)(bufferAddress_ + offset));
    offset += rowSize;
  }
  return arrow::Status::OK();
}

} // namespace gluten
