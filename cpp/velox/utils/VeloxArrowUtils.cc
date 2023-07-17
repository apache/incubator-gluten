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

#include "utils/VeloxArrowUtils.h"

#include <ComplexVector.h>
#include <velox/vector/arrow/Bridge.h>
#include "memory/VeloxColumnarBatch.h"

namespace gluten {

arrow::Result<std::shared_ptr<ColumnarBatch>> recordBatch2VeloxColumnarBatch(const arrow::RecordBatch& rb) {
  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  RETURN_NOT_OK(arrow::ExportRecordBatch(rb, &arrowArray, &arrowSchema));
  auto vp =
      facebook::velox::importFromArrowAsOwner(arrowSchema, arrowArray, gluten::defaultLeafVeloxMemoryPool().get());
  return std::make_shared<VeloxColumnarBatch>(std::dynamic_pointer_cast<facebook::velox::RowVector>(vp));
}

arrow::Status MyMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  if (bytes_allocated() + size > capacity_) {
    return arrow::Status::OutOfMemory("malloc of size ", size, " failed");
  }
  RETURN_NOT_OK(pool_->Allocate(size, out));
  stats_.UpdateAllocatedBytes(size);
  return arrow::Status::OK();
}

arrow::Status MyMemoryPool::Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) {
  if (newSize > capacity_) {
    return arrow::Status::OutOfMemory("malloc of size ", newSize, " failed");
  }
  // auto old_ptr = *ptr;
  RETURN_NOT_OK(pool_->Reallocate(oldSize, newSize, ptr));
  stats_.UpdateAllocatedBytes(newSize - oldSize);
  return arrow::Status::OK();
}

void MyMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t alignment) {
  pool_->Free(buffer, size);
  stats_.UpdateAllocatedBytes(-size);
}

int64_t MyMemoryPool::bytes_allocated() const {
  return stats_.bytes_allocated();
}

int64_t MyMemoryPool::max_memory() const {
  return pool_->max_memory();
}

int64_t MyMemoryPool::total_bytes_allocated() const {
  return pool_->total_bytes_allocated();
}

int64_t MyMemoryPool::num_allocations() const {
  throw pool_->num_allocations();
}

std::string MyMemoryPool::backend_name() const {
  return pool_->backend_name();
}

} // namespace gluten
