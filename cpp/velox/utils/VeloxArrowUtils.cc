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

using namespace facebook;

void toArrowSchema(const velox::TypePtr& rowType, facebook::velox::memory::MemoryPool* pool, struct ArrowSchema* out) {
  exportToArrow(velox::BaseVector::create(rowType, 0, pool), *out);
}

std::shared_ptr<arrow::Schema> toArrowSchema(const velox::TypePtr& rowType, facebook::velox::memory::MemoryPool* pool) {
  ArrowSchema arrowSchema;
  toArrowSchema(rowType, pool, &arrowSchema);
  GLUTEN_ASSIGN_OR_THROW(auto outputSchema, arrow::ImportSchema(&arrowSchema));
  return outputSchema;
}

velox::TypePtr fromArrowSchema(const std::shared_ptr<arrow::Schema>& schema) {
  ArrowSchema cSchema;
  GLUTEN_THROW_NOT_OK(arrow::ExportSchema(*schema, &cSchema));
  velox::TypePtr typePtr = velox::importFromArrow(cSchema);
  // It should be velox::importFromArrow's duty to release the imported arrow c schema.
  // Since exported Velox type prt doesn't hold memory from the c schema.
  ArrowSchemaRelease(&cSchema); // otherwise the c schema leaks memory
  return typePtr;
}

arrow::Result<std::shared_ptr<ColumnarBatch>> recordBatch2VeloxColumnarBatch(const arrow::RecordBatch& rb) {
  ArrowArray arrowArray;
  ArrowSchema arrowSchema;
  RETURN_NOT_OK(arrow::ExportRecordBatch(rb, &arrowArray, &arrowSchema));
  auto vp = velox::importFromArrowAsOwner(arrowSchema, arrowArray, gluten::defaultLeafVeloxMemoryPool().get());
  return std::make_shared<VeloxColumnarBatch>(std::dynamic_pointer_cast<velox::RowVector>(vp));
}

arrow::Status MyMemoryPool::Allocate(int64_t size, uint8_t** out) {
  if (bytes_allocated() + size > capacity_) {
    return arrow::Status::OutOfMemory("malloc of size ", size, " failed");
  }
  RETURN_NOT_OK(pool_->Allocate(size, out));
  stats_.UpdateAllocatedBytes(size);
  return arrow::Status::OK();
}

arrow::Status MyMemoryPool::Reallocate(int64_t oldSize, int64_t newSize, uint8_t** ptr) {
  if (newSize > capacity_) {
    return arrow::Status::OutOfMemory("malloc of size ", newSize, " failed");
  }
  // auto old_ptr = *ptr;
  RETURN_NOT_OK(pool_->Reallocate(oldSize, newSize, ptr));
  stats_.UpdateAllocatedBytes(newSize - oldSize);
  return arrow::Status::OK();
}

void MyMemoryPool::Free(uint8_t* buffer, int64_t size) {
  pool_->Free(buffer, size);
  stats_.UpdateAllocatedBytes(-size);
}

int64_t MyMemoryPool::bytes_allocated() const {
  return stats_.bytes_allocated();
}

int64_t MyMemoryPool::max_memory() const {
  return pool_->max_memory();
}

std::string MyMemoryPool::backend_name() const {
  return pool_->backend_name();
}

} // namespace gluten
