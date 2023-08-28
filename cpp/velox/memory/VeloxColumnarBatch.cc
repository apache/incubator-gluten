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
#include "VeloxColumnarBatch.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace gluten {

using namespace facebook;
using namespace facebook::velox;

namespace {

RowVectorPtr makeRowVector(
    std::vector<std::string> childNames,
    const std::vector<VectorPtr>& children,
    velox::memory::MemoryPool* pool) {
  std::vector<std::shared_ptr<const Type>> childTypes;
  childTypes.resize(children.size());
  for (int i = 0; i < children.size(); i++) {
    childTypes[i] = children[i]->type();
  }
  auto rowType = ROW(std::move(childNames), std::move(childTypes));
  const size_t vectorSize = children.empty() ? 0 : children.front()->size();

  return std::make_shared<RowVector>(pool, rowType, BufferPtr(nullptr), vectorSize, std::move(children));
}
} // namespace

void VeloxColumnarBatch::ensureFlattened() {
  if (flattened_ != nullptr) {
    return;
  }
  auto startTime = std::chrono::steady_clock::now();
  // Make sure to load lazy vector if not loaded already.
  for (auto& child : rowVector_->children()) {
    child->loadedVector();
  }

  // Perform copy to flatten dictionary vectors.
  velox::RowVectorPtr copy = std::dynamic_pointer_cast<velox::RowVector>(
      velox::BaseVector::create(rowVector_->type(), rowVector_->size(), rowVector_->pool()));
  copy->copy(rowVector_.get(), 0, 0, rowVector_->size());
  flattened_ = copy;
  auto endTime = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
  exportNanos_ += duration;
}

std::shared_ptr<ArrowSchema> VeloxColumnarBatch::exportArrowSchema() {
  auto out = std::make_shared<ArrowSchema>();
  ensureFlattened();
  velox::exportToArrow(flattened_, *out);
  return out;
}

std::shared_ptr<ArrowArray> VeloxColumnarBatch::exportArrowArray() {
  auto out = std::make_shared<ArrowArray>();
  ensureFlattened();
  velox::exportToArrow(flattened_, *out, flattened_->pool());
  return out;
}

int64_t VeloxColumnarBatch::numBytes() {
  ensureFlattened();
  return flattened_->estimateFlatSize();
}

velox::RowVectorPtr VeloxColumnarBatch::getRowVector() const {
  return rowVector_;
}

velox::RowVectorPtr VeloxColumnarBatch::getFlattenedRowVector() {
  ensureFlattened();
  return flattened_;
}

std::shared_ptr<VeloxColumnarBatch> VeloxColumnarBatch::from(
    facebook::velox::memory::MemoryPool* pool,
    std::shared_ptr<ColumnarBatch> cb) {
  if (cb->getType() == "velox") {
    return std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  }
  if (cb->getType() == "composite") {
    auto composite = std::dynamic_pointer_cast<gluten::CompositeColumnarBatch>(cb);
    auto children = composite->getBatches();
    std::vector<std::string> childNames;
    std::vector<VectorPtr> childVectors;

    for (const auto& child : children) {
      auto asVelox = from(pool, child);
      auto names = facebook::velox::asRowType(asVelox->getRowVector()->type())->names();
      for (const auto& name : names) {
        childNames.push_back(name);
      }
      auto vectors = asVelox->getRowVector()->children();
      for (const auto& vector : vectors) {
        childVectors.push_back(vector);
      }
    }

    auto compositeVeloxVector = makeRowVector(childNames, childVectors, pool);
    return std::make_shared<VeloxColumnarBatch>(compositeVeloxVector);
  }
  auto vp = velox::importFromArrowAsOwner(*cb->exportArrowSchema(), *cb->exportArrowArray(), pool);
  return std::make_shared<VeloxColumnarBatch>(std::dynamic_pointer_cast<velox::RowVector>(vp));
}

} // namespace gluten
