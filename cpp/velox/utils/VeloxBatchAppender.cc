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

#include "VeloxBatchAppender.h"

namespace gluten {

gluten::VeloxBatchAppender::VeloxBatchAppender(
    facebook::velox::memory::MemoryPool* pool,
    int32_t minOutputBatchSize,
    std::unique_ptr<ColumnarBatchIterator> in)
    : pool_(pool), minOutputBatchSize_(minOutputBatchSize), in_(std::move(in)) {}

std::shared_ptr<ColumnarBatch> VeloxBatchAppender::next() {
  auto cb = in_->next();
  if (cb == nullptr) {
    // Input iterator was drained.
    return nullptr;
  }
  if (cb->numRows() >= minOutputBatchSize_) {
    // Fast flush path.
    return cb;
  }

  auto vb = VeloxColumnarBatch::from(pool_, cb);
  auto rv = vb->getRowVector();
  auto buffer = facebook::velox::RowVector::createEmpty(rv->type(), pool_);
  buffer->append(rv.get());

  for (auto nextCb = in_->next(); nextCb != nullptr; nextCb = in_->next()) {
    auto nextVb = VeloxColumnarBatch::from(pool_, nextCb);
    auto nextRv = nextVb->getRowVector();
    buffer->append(nextRv.get());
    if (buffer->size() >= minOutputBatchSize_) {
      // Buffer is full.
      break;
    }
  }
  return std::make_shared<VeloxColumnarBatch>(buffer);
}

int64_t VeloxBatchAppender::spillFixedSize(int64_t size) {
  return in_->spillFixedSize(size);
}
} // namespace gluten
