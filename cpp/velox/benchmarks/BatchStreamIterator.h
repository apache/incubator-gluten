#pragma once

#include "BatchIteratorWrapper.h"

namespace gluten {

class ParquetBatchStreamIterator final : public ParquetBatchIterator {
 public:
  explicit ParquetBatchStreamIterator(const std::string& path) : ParquetBatchIterator(path) {
    CreateReader();
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "ParquetBatchStreamIterator open file: " << path << std::endl;
#endif
  }

  std::shared_ptr<gluten::ColumnarBatch> next() override {
    auto startTime = std::chrono::steady_clock::now();
    GLUTEN_ASSIGN_OR_THROW(auto batch, recordBatchReader_->Next());
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "ParquetBatchStreamIterator get a batch, num rows: " << (batch ? batch->num_rows() : 0) << std::endl;
#endif
    collectBatchTime_ +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - startTime).count();
    if (batch == nullptr) {
      return nullptr;
    }
    return std::make_shared<gluten::ArrowColumnarBatch>(batch);
  }
};

inline std::shared_ptr<gluten::ResultIterator> getParquetInputFromBatchStream(const std::string& path) {
  return std::make_shared<gluten::ResultIterator>(std::make_unique<ParquetBatchStreamIterator>(path));
}

class OrcBatchStreamIterator final : public OrcBatchIterator {
 public:
  explicit OrcBatchStreamIterator(const std::string& path) : OrcBatchIterator(path) {
    CreateReader();
  }

  std::shared_ptr<gluten::ColumnarBatch> next() override {
    auto startTime = std::chrono::steady_clock::now();
    GLUTEN_ASSIGN_OR_THROW(auto batch, recordBatchReader_->Next());
#ifdef GLUTEN_PRINT_DEBUG
    std::cout << "OrcBatchStreamIterator get a batch, num rows: " << (batch ? batch->num_rows() : 0) << std::endl;
#endif
    collectBatchTime_ +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - startTime).count();
    if (batch == nullptr) {
      return nullptr;
    }
    return std::make_shared<gluten::ArrowColumnarBatch>(batch);
  }
};

inline std::shared_ptr<gluten::ResultIterator> getOrcInputFromBatchStream(const std::string& path) {
  return std::make_shared<gluten::ResultIterator>(std::make_unique<OrcBatchStreamIterator>(path));
}

} // namespace gluten
