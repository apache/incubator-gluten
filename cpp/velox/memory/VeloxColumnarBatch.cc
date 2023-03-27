#include "VeloxColumnarBatch.h"

namespace gluten {

using namespace facebook;

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
  velox::exportToArrow(flattened_, *out, GetDefaultWrappedVeloxMemoryPool());
  return out;
}

int64_t VeloxColumnarBatch::GetBytes() {
  EnsureFlattened();
  return flattened_->estimateFlatSize();
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
