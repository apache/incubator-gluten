#pragma once

#include "BatchIteratorWrapper.h"

namespace gluten {

class ParquetBatchVectorIterator final : public ParquetBatchIterator {
 public:
  explicit ParquetBatchVectorIterator(const std::string& path) : ParquetBatchIterator(path) {
    CreateReader();
    CollectBatches();

    iter_ = batches_.begin();
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "ParquetBatchVectorIterator open file: " << path << std::endl;
    std::cout << "Number of input batches: " << std::to_string(batches_.size()) << std::endl;
    if (iter_ != batches_.cend()) {
      std::cout << "columns: " << (*iter_)->num_columns() << std::endl;
      std::cout << "rows: " << (*iter_)->num_rows() << std::endl;
    }
#endif
  }

  arrow::Result<std::shared_ptr<gluten::ColumnarBatch>> Next() override {
    if (iter_ == batches_.cend()) {
      return nullptr;
    }
    return std::make_shared<gluten::ArrowColumnarBatch>(*iter_++);
  }

 private:
  void CollectBatches() {
    auto startTime = std::chrono::steady_clock::now();
    GLUTEN_ASSIGN_OR_THROW(batches_, recordBatchReader_->ToRecordBatches());
    auto endTime = std::chrono::steady_clock::now();
    collectBatchTime_ += std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
  }

  arrow::RecordBatchVector batches_;
  std::vector<std::shared_ptr<arrow::RecordBatch>>::const_iterator iter_;
};

inline std::shared_ptr<gluten::ResultIterator> getParquetInputFromBatchVector(const std::string& path) {
  return std::make_shared<gluten::ResultIterator>(std::make_unique<ParquetBatchVectorIterator>(path));
}

class OrcBatchVectorIterator final : public OrcBatchIterator {
 public:
  explicit OrcBatchVectorIterator(const std::string& path) : OrcBatchIterator(path) {
    CreateReader();
    CollectBatches();

    iter_ = batches_.begin();
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "OrcBatchVectorIterator open file: " << path << std::endl;
    std::cout << "Number of input batches: " << std::to_string(batches_.size()) << std::endl;
    if (iter_ != batches_.cend()) {
      std::cout << "columns: " << (*iter_)->num_columns() << std::endl;
      std::cout << "rows: " << (*iter_)->num_rows() << std::endl;
    }
#endif
  }

  arrow::Result<std::shared_ptr<gluten::ColumnarBatch>> Next() override {
    if (iter_ == batches_.cend()) {
      return nullptr;
    }
    return std::make_shared<gluten::ArrowColumnarBatch>(*iter_++);
  }

 private:
  void CollectBatches() {
    auto startTime = std::chrono::steady_clock::now();
    GLUTEN_ASSIGN_OR_THROW(batches_, recordBatchReader_->ToRecordBatches());
    auto endTime = std::chrono::steady_clock::now();
    collectBatchTime_ += std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
  }

  arrow::RecordBatchVector batches_;
  std::vector<std::shared_ptr<arrow::RecordBatch>>::const_iterator iter_;
};

inline std::shared_ptr<gluten::ResultIterator> getOrcInputFromBatchVector(const std::string& path) {
  return std::make_shared<gluten::ResultIterator>(std::make_unique<OrcBatchVectorIterator>(path));
}

} // namespace gluten
