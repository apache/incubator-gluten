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

#include "MemoryAllocator.h"

namespace gluten {

std::shared_ptr<arrow::MemoryPool> AsWrappedArrowMemoryPool(MemoryAllocator* allocator);

std::shared_ptr<arrow::MemoryPool> GetDefaultArrowMemoryPool();

class WrappedArrowMemoryPool final : public arrow::MemoryPool {
 public:
  explicit WrappedArrowMemoryPool(MemoryAllocator* allocator) : allocator_(allocator) {}

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;

  arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override;

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;

  int64_t bytes_allocated() const override;

  std::string backend_name() const override;

 private:
  MemoryAllocator* allocator_;
};

} // namespace gluten
