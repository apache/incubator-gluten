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

#include "compute/ResultIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/vector/ComplexVectorStream.h"

namespace gluten {
class RowVectorStream final : public facebook::velox::RowVectorStream {
 public:
  explicit RowVectorStream(
      std::shared_ptr<facebook::velox::memory::MemoryPool> pool,
      std::shared_ptr<ResultIterator> iterator,
      const facebook::velox::RowTypePtr& outputType)
      : pool_(pool), iterator_(iterator), outputType_(outputType) {}

  bool hasNext() {
    return iterator_->HasNext();
  }

  // Convert arrow batch to rowvector and use new output columns
  facebook::velox::RowVectorPtr next() {
    auto vp = convertBatch(pool_, iterator_->Next());
    return std::make_shared<facebook::velox::RowVector>(
        vp->pool(), outputType_, facebook::velox::BufferPtr(0), vp->size(), std::move(vp->children()));
  }

 private:
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<ResultIterator> iterator_;
  const facebook::velox::RowTypePtr outputType_;
};
} // namespace gluten
