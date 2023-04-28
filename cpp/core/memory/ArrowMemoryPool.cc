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

#include "ArrowMemoryPool.h"

namespace gluten {

std::shared_ptr<arrow::MemoryPool> AsWrappedArrowMemoryPool(MemoryAllocator* allocator) {
  return std::make_shared<WrappedArrowMemoryPool>(allocator);
}

std::shared_ptr<arrow::MemoryPool> GetDefaultWrappedArrowMemoryPool() {
  static auto static_pool = AsWrappedArrowMemoryPool(DefaultMemoryAllocator().get());
  return static_pool;
}

arrow::Status WrappedArrowMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  if (!allocator_->AllocateAligned(alignment, size, reinterpret_cast<void**>(out))) {
    return arrow::Status::Invalid("WrappedMemoryPool: Error allocating " + std::to_string(size) + " bytes");
  }
  return arrow::Status::OK();
}

arrow::Status WrappedArrowMemoryPool::Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) {
  if (!allocator_->ReallocateAligned(*ptr, alignment, old_size, new_size, reinterpret_cast<void**>(ptr))) {
    return arrow::Status::Invalid("WrappedMemoryPool: Error reallocating " + std::to_string(new_size) + " bytes");
  }
  return arrow::Status::OK();
}

void WrappedArrowMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t alignment) {
  allocator_->Free(buffer, size);
}

int64_t WrappedArrowMemoryPool::bytes_allocated() const {
  // fixme use self accountant
  return allocator_->GetBytes();
}

std::string WrappedArrowMemoryPool::backend_name() const {
  return "gluten allocator";
}

} // namespace gluten
