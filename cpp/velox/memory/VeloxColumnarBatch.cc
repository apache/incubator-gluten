#include "VeloxColumnarBatch.h"
#include "velox/type/Type.h"

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

  return std::make_shared<RowVector>(pool, rowType, BufferPtr(nullptr), vectorSize, children);
}

// destroyed input vector
RowVectorPtr addColumnToVector(int32_t index, RowVectorPtr vector, RowVectorPtr col) {
  auto names = asRowType(vector->type())->names();
  auto newNames = std::move(names);
  newNames.insert(newNames.begin() + index, asRowType(col->type())->nameOf(0));

  auto childern = vector->children();
  auto newChildren = std::move(childern);
  newChildren.insert(newChildren.begin() + index, col->childAt(0));
  return makeRowVector(newNames, newChildren, vector->pool());
}
} // namespace

void VeloxColumnarBatch::EnsureFlattened() {
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
  EnsureFlattened();
  velox::exportToArrow(flattened_, *out);
  return out;
}

std::shared_ptr<ArrowArray> VeloxColumnarBatch::exportArrowArray() {
  auto out = std::make_shared<ArrowArray>();
  EnsureFlattened();
  velox::exportToArrow(flattened_, *out, GetDefaultWrappedVeloxMemoryPool().get());
  return out;
}

int64_t VeloxColumnarBatch::GetBytes() {
  EnsureFlattened();
  return flattened_->estimateFlatSize();
}

std::shared_ptr<ColumnarBatch> VeloxColumnarBatch::addColumn(int32_t index, std::shared_ptr<ColumnarBatch> col) {
  auto cb = std::dynamic_pointer_cast<ArrowCStructColumnarBatch>(col);
  auto rvCol = std::dynamic_pointer_cast<RowVector>(
      velox::importFromArrowAsOwner(*cb->exportArrowSchema(), *cb->exportArrowArray(), rowVector_->pool()));
  auto newVector = addColumnToVector(index, rowVector_, rvCol);
  return std::make_shared<VeloxColumnarBatch>(newVector);
}

void VeloxColumnarBatch::saveToFile(std::shared_ptr<ArrowWriter> writer) {
  auto schema = exportArrowSchema();
  auto maybeBatch = arrow::ImportRecordBatch(exportArrowArray().get(), schema.get());
  if (!maybeBatch.ok()) {
    throw gluten::GlutenException("Get batch failed!");
    return;
  }
  GLUTEN_THROW_NOT_OK(writer->initWriter(*maybeBatch.ValueOrDie()->schema().get()));
  GLUTEN_THROW_NOT_OK(writer->writeInBatches(maybeBatch.ValueOrDie()));
}

velox::RowVectorPtr VeloxColumnarBatch::getRowVector() const {
  return rowVector_;
}

velox::RowVectorPtr VeloxColumnarBatch::getFlattenedRowVector() {
  EnsureFlattened();
  return flattened_;
}

} // namespace gluten
