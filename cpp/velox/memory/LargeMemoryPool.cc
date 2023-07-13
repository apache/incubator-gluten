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

#include "LargeMemoryPool.h"
#include <arrow/util/logging.h>
#include <sys/mman.h>
#include "utils/macros.h"

#include <numeric>

namespace gluten {

arrow::Status LargeMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  if (size == 0) {
    return delegated_->Allocate(0, alignment, out);
  }
  // make sure the size is cache line size aligned
  size = ROUND_TO_LINE(size, alignment);
  if (buffers_.empty() || size > buffers_.back().size - buffers_.back().allocated) {
    // Allocate new page. Align to kHugePageSize.
    uint64_t allocSize = size > kLargeBufferSize ? ROUND_TO_LINE(size, kHugePageSize) : kLargeBufferSize;

    uint8_t* allocAddr;
    RETURN_NOT_OK(doAlloc(allocSize, kHugePageSize, &allocAddr));
    if (!allocAddr) {
      return arrow::Status::Invalid("doAlloc failed.");
    }
    madvise(allocAddr, size, MADV_WILLNEED);
    buffers_.push_back({allocAddr, allocAddr, allocSize, 0, 0});
  }
  auto& last = buffers_.back();
  *out = last.startAddr + last.allocated;
  last.lastAllocAddr = *out;
  last.allocated += size;
  return arrow::Status::OK();
}

void LargeMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t alignment) {
  if (size == 0) {
    return;
  }
  // make sure the size is cache line size aligned
  size = ROUND_TO_LINE(size, alignment);

  auto its = std::find_if(buffers_.begin(), buffers_.end(), [buffer](BufferAllocated& buf) {
    return buffer >= buf.startAddr && buffer < buf.startAddr + buf.size;
  });
  ARROW_CHECK_NE(its, buffers_.end());
  its->freed += size;
  if (its->freed && its->freed == its->allocated) {
    doFree(its->startAddr, its->size);
    buffers_.erase(its);
  }
}

arrow::Status LargeMemoryPool::Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) {
  if (oldSize == 0) {
    return arrow::Status::Invalid("Cannot call reallocated on oldSize == 0");
  }
  if (newSize == 0) {
    return arrow::Status::Invalid("Cannot call reallocated on newSize == 0");
  }
  auto* oldPtr = *ptr;
  auto& lastBuffer = buffers_.back();
  if (!(oldPtr >= lastBuffer.lastAllocAddr && oldPtr < lastBuffer.startAddr + lastBuffer.size)) {
    return arrow::Status::Invalid("Reallocate can only be called for the last buffer");
  }

  // shrink-to-fit
  if (newSize <= oldSize) {
    lastBuffer.allocated -= (oldSize - newSize);
    return arrow::Status::OK();
  }

  if (newSize - oldSize > lastBuffer.size - lastBuffer.allocated) {
    RETURN_NOT_OK(Allocate(newSize, alignment, ptr));
    memcpy(*ptr, oldPtr, std::min(oldSize, newSize));
    Free(oldPtr, oldSize, alignment);
  } else {
    lastBuffer.allocated += (newSize - oldSize);
  }
  return arrow::Status::OK();
}

int64_t LargeMemoryPool::bytes_allocated() const {
  return std::accumulate(
      buffers_.begin(), buffers_.end(), 0LL, [](uint64_t size, const BufferAllocated& buf) { return size + buf.size; });
}

int64_t LargeMemoryPool::max_memory() const {
  return delegated_->max_memory();
}

std::string LargeMemoryPool::backend_name() const {
  return "LargeMemoryPool";
}

int64_t LargeMemoryPool::total_bytes_allocated() const {
  return delegated_->total_bytes_allocated();
}

int64_t LargeMemoryPool::num_allocations() const {
  return delegated_->num_allocations();
}

arrow::Status LargeMemoryPool::doAlloc(int64_t size, int64_t alignment, uint8_t** out) {
  return delegated_->Allocate(size, alignment, out);
}

void LargeMemoryPool::doFree(uint8_t* buffer, int64_t size) {
  delegated_->Free(buffer, size);
}

LargeMemoryPool::~LargeMemoryPool() {
  ARROW_CHECK(buffers_.size() == 0);
}

MMapMemoryPool::~MMapMemoryPool() {
  ARROW_CHECK(buffers_.size() == 0);
}

arrow::Status MMapMemoryPool::doAlloc(int64_t size, int64_t alignment, uint8_t** out) {
  *out = static_cast<uint8_t*>(mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  if (*out == MAP_FAILED) {
    return arrow::Status::OutOfMemory(" mmap error ", size);
  } else {
    madvise(*out, size, MADV_WILLNEED);
    return arrow::Status::OK();
  }
}

void MMapMemoryPool::doFree(uint8_t* buffer, int64_t size) {
  munmap((void*)(buffer), size);
}
} // namespace gluten
