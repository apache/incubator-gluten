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

#include "ShuffleWriter.h"

#include <arrow/result.h>

#include "ShuffleSchema.h"
#include "utils/macros.h"

namespace gluten {

#ifndef SPLIT_BUFFER_SIZE
// by default, allocate 8M block, 2M page size
#define SPLIT_BUFFER_SIZE 16 * 1024 * 1024
#endif

ShuffleWriterOptions ShuffleWriterOptions::defaults() {
  return {};
}

ShuffleBufferPool::ShuffleBufferPool(std::shared_ptr<arrow::MemoryPool> pool) : pool_(pool) {}

arrow::Status ShuffleBufferPool::init() {
  // Allocate first buffer for split reducer
  ARROW_ASSIGN_OR_RAISE(combineBuffer_, arrow::AllocateResizableBuffer(0, pool_.get()));
  RETURN_NOT_OK(combineBuffer_->Resize(0, /*shrink_to_fit =*/false));
  return arrow::Status::OK();
}

arrow::Status ShuffleBufferPool::allocate(std::shared_ptr<arrow::Buffer>& buffer, int64_t size) {
  // if size is already larger than buffer pool size, allocate it directly
  // make size 64byte aligned
  size = ROUND_TO_LINE(size, kDefaultBufferAlignment);
  if (size > SPLIT_BUFFER_SIZE) {
    ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateResizableBuffer(size, pool_.get()));
    return arrow::Status::OK();
  } else if (combineBuffer_->capacity() - combineBuffer_->size() < size) {
    // memory pool is not enough
    ARROW_ASSIGN_OR_RAISE(combineBuffer_, arrow::AllocateResizableBuffer(SPLIT_BUFFER_SIZE, pool_.get()));
    RETURN_NOT_OK(combineBuffer_->Resize(0, /*shrink_to_fit = */ false));
  }
  buffer = arrow::SliceMutableBuffer(combineBuffer_, combineBuffer_->size(), size);
  RETURN_NOT_OK(combineBuffer_->Resize(combineBuffer_->size() + size, /*shrink_to_fit = */ false));
  return arrow::Status::OK();
}

arrow::Status ShuffleBufferPool::allocateDirectly(std::shared_ptr<arrow::ResizableBuffer>& buffer, int64_t size) {
  size = ROUND_TO_LINE(size, kDefaultBufferAlignment);
  ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateResizableBuffer(size, pool_.get()));
  return arrow::Status::OK();
}

int64_t ShuffleBufferPool::bytesAllocated() const {
  return pool_->bytes_allocated();
}

std::shared_ptr<arrow::Schema> ShuffleWriter::writeSchema() {
  if (writeSchema_ != nullptr) {
    return writeSchema_;
  }

  writeSchema_ = toWriteSchema(*schema_);
  return writeSchema_;
}

std::shared_ptr<arrow::Schema> ShuffleWriter::compressWriteSchema() {
  if (compressWriteSchema_ != nullptr) {
    return compressWriteSchema_;
  }

  compressWriteSchema_ = toCompressWriteSchema(*schema_);
  return compressWriteSchema_;
}

void ShuffleWriter::clearCachedPayloads(uint32_t partitionId) {
  partitionCachedRecordbatch()[partitionId].clear();
  setPartitionCachedRecordbatchSize(partitionId, 0);
}
} // namespace gluten
