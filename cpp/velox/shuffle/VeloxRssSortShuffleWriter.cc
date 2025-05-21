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

#include "shuffle/VeloxRssSortShuffleWriter.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "shuffle/ShuffleSchema.h"
#include "utils/Common.h"
#include "utils/Macros.h"
#include "utils/VeloxArrowUtils.h"

#include "velox/common/base/Nulls.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

arrow::Result<std::shared_ptr<VeloxShuffleWriter>> VeloxRssSortShuffleWriter::create(
    uint32_t numPartitions,
    std::unique_ptr<PartitionWriter> partitionWriter,
    ShuffleWriterOptions options,
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
    arrow::MemoryPool* arrowPool) {
  std::shared_ptr<VeloxRssSortShuffleWriter> res(new VeloxRssSortShuffleWriter(
      numPartitions, std::move(partitionWriter), std::move(options), veloxPool, arrowPool));
  RETURN_NOT_OK(res->init());
  return res;
} // namespace gluten

arrow::Status VeloxRssSortShuffleWriter::init() {
  rowVectorIndexMap_.reserve(numPartitions_);
  bufferOutputStream_ = std::make_unique<BufferOutputStream>(veloxPool_.get());

  return arrow::Status::OK();
}

arrow::Status VeloxRssSortShuffleWriter::doSort(facebook::velox::RowVectorPtr rv, int64_t memLimit) {
  currentInputColumnBytes_ += rv->estimateFlatSize();
  batches_.push_back(rv);
  if (currentInputColumnBytes_ > memLimit) {
    for (auto pid = 0; pid < numPartitions(); ++pid) {
      RETURN_NOT_OK(evictRowVector(pid));
    }
    batches_.clear();
    currentInputColumnBytes_ = 0;
  }
  setSortState(SortState::kSortInit);
  return arrow::Status::OK();
}

arrow::Status VeloxRssSortShuffleWriter::write(std::shared_ptr<ColumnarBatch> cb, int64_t /* memLimit */) {
  if (options_.partitioning == Partitioning::kSingle) {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    auto rv = veloxColumnBatch->getFlattenedRowVector();
    RETURN_NOT_OK(initFromRowVector(*rv.get()));
    RETURN_NOT_OK(doSort(rv, partitionWriter_.get()->options().sortBufferMaxSize));
  } else if (options_.partitioning == Partitioning::kRange) {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    const int32_t numColumns = veloxColumnBatch->numColumns();
    VELOX_CHECK(numColumns >= 2);
    auto pidBatch = veloxColumnBatch->select(veloxPool_.get(), {0});
    auto pidArr = getFirstColumn(*(pidBatch->getRowVector()));
    START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
    setSortState(SortState::kSort);
    RETURN_NOT_OK(partitioner_->compute(pidArr, pidBatch->numRows(), batches_.size(), rowVectorIndexMap_));
    END_TIMING();
    std::vector<int32_t> range;
    for (int32_t i = 1; i < numColumns; i++) {
      range.push_back(i);
    }
    auto rvBatch = veloxColumnBatch->select(veloxPool_.get(), range);
    auto rv = rvBatch->getFlattenedRowVector();
    RETURN_NOT_OK(initFromRowVector(*rv.get()));
    RETURN_NOT_OK(doSort(rv, partitionWriter_.get()->options().sortBufferMaxSize));
  } else {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    facebook::velox::RowVectorPtr rv;
    START_TIMING(cpuWallTimingList_[CpuWallTimingFlattenRV]);
    rv = veloxColumnBatch->getFlattenedRowVector();
    END_TIMING();
    if (partitioner_->hasPid()) {
      auto pidArr = getFirstColumn(*rv);
      START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
      setSortState(SortState::kSort);
      RETURN_NOT_OK(partitioner_->compute(pidArr, rv->size(), batches_.size(), rowVectorIndexMap_));
      END_TIMING();
      auto strippedRv = getStrippedRowVector(*rv);
      RETURN_NOT_OK(initFromRowVector(*strippedRv));
      RETURN_NOT_OK(doSort(strippedRv, partitionWriter_.get()->options().sortBufferMaxSize));
    } else {
      RETURN_NOT_OK(initFromRowVector(*rv));
      START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
      setSortState(SortState::kSort);
      RETURN_NOT_OK(partitioner_->compute(nullptr, rv->size(), batches_.size(), rowVectorIndexMap_));
      END_TIMING();
      RETURN_NOT_OK(doSort(rv, partitionWriter_.get()->options().sortBufferMaxSize));
    }
  }
  return arrow::Status::OK();
}

arrow::Status VeloxRssSortShuffleWriter::evictBatch(uint32_t partitionId) {
  bufferOutputStream_->seekp(0);
  batch_->flush(bufferOutputStream_.get());
  auto buffer = bufferOutputStream_->getBuffer();
  auto arrowBuffer = std::make_shared<arrow::Buffer>(buffer->as<uint8_t>(), buffer->size());
  ARROW_ASSIGN_OR_RAISE(
      auto payload, BlockPayload::fromBuffers(Payload::kRaw, 0, {std::move(arrowBuffer)}, nullptr, nullptr, nullptr));
  RETURN_NOT_OK(partitionWriter_->evict(partitionId, std::move(payload), stopped_));
  batch_ = std::make_unique<facebook::velox::VectorStreamGroup>(veloxPool_.get(), serde_.get());
  batch_->createStreamTree(rowType_, options_.bufferSize, &serdeOptions_);
  return arrow::Status::OK();
}

arrow::Status VeloxRssSortShuffleWriter::evictRowVector(uint32_t partitionId) {
  int32_t accumulatedRows = 0;
  const int32_t maxRowsPerBatch = options_.bufferSize;

  if (options_.partitioning != Partitioning::kSingle) {
    if (auto it = rowVectorIndexMap_.find(partitionId); it != rowVectorIndexMap_.end()) {
      const auto& rowIndices = it->second;
      VELOX_DCHECK(!rowIndices.empty());

      size_t idx = 0;
      const auto outputSize = rowIndices.size();
      while (idx < outputSize) {
        auto combinedRowIndex = rowIndices[idx];
        auto inputVectorIndex = static_cast<int32_t>(combinedRowIndex >> 32);
        auto startRow = static_cast<int32_t>(combinedRowIndex & 0xFFFFFFFFLL);

        int32_t numRowsInRange = 1;
        std::vector<facebook::velox::IndexRange> groupedIndices;

        while (++idx < outputSize && (rowIndices[idx] >> 32) == inputVectorIndex) {
          auto row = static_cast<int32_t>(rowIndices[idx] & 0xFFFFFFFFLL);
          if (row == startRow + numRowsInRange) {
            numRowsInRange++;
          } else {
            groupedIndices.push_back({startRow, numRowsInRange});
            accumulatedRows += numRowsInRange;
            startRow = row;
            numRowsInRange = 1;
          }
        }
        groupedIndices.push_back({startRow, numRowsInRange});
        batch_->append(batches_[inputVectorIndex], groupedIndices);

        accumulatedRows += numRowsInRange;
        // Check whether to evict the data after gathering all rows from one input RowVector.
        if (accumulatedRows >= maxRowsPerBatch) {
          RETURN_NOT_OK(evictBatch(partitionId));
          accumulatedRows = 0;
        }
      }
      rowVectorIndexMap_.erase(partitionId);
    }
  } else {
    for (facebook::velox::RowVectorPtr rowVectorPtr : batches_) {
      batch_->append(rowVectorPtr);
      accumulatedRows += rowVectorPtr->size();
      if (accumulatedRows >= maxRowsPerBatch) {
        RETURN_NOT_OK(evictBatch(partitionId));
        accumulatedRows = 0;
      }
    }
  }
  if (accumulatedRows > 0) {
    RETURN_NOT_OK(evictBatch(partitionId));
  }
  return arrow::Status::OK();
}

arrow::Status VeloxRssSortShuffleWriter::stop() {
  stopped_ = true;
  for (auto pid = 0; pid < numPartitions(); ++pid) {
    RETURN_NOT_OK(evictRowVector(pid));
  }
  batches_.clear();
  currentInputColumnBytes_ = 0;
  {
    SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingStop]);
    setSortState(SortState::kSortStop);
    RETURN_NOT_OK(partitionWriter_->stop(&metrics_));
  }

  stat();

  return arrow::Status::OK();
}

arrow::Status VeloxRssSortShuffleWriter::initFromRowVector(const facebook::velox::RowVector& rv) {
  if (!rowType_) {
    rowType_ = facebook::velox::asRowType(rv.type());
    serdeOptions_ = {false, arrowCompressionTypeToVelox(partitionWriter_->options().compressionType)};
    batch_ = std::make_unique<facebook::velox::VectorStreamGroup>(veloxPool_.get(), serde_.get());
    batch_->createStreamTree(rowType_, options_.bufferSize, &serdeOptions_);
  }
  return arrow::Status::OK();
}

arrow::Status VeloxRssSortShuffleWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  if (evictState_ == EvictState::kUnevictable) {
    *actual = 0;
    return arrow::Status::OK();
  }
  EvictGuard evictGuard{evictState_};

  if (sortState_ == SortState::kSortInit) {
    for (auto pid = 0; pid < numPartitions(); ++pid) {
      RETURN_NOT_OK(evictRowVector(pid));
    }
    batches_.clear();
    *actual = currentInputColumnBytes_;
    currentInputColumnBytes_ = 0;
  }
  return arrow::Status::OK();
}

void VeloxRssSortShuffleWriter::stat() const {
#if VELOX_SHUFFLE_WRITER_LOG_FLAG
  for (int i = CpuWallTimingBegin; i != CpuWallTimingEnd; ++i) {
    std::ostringstream oss;
    auto& timing = cpuWallTimingList_[i];
    oss << "Velox shuffle writer stat:" << CpuWallTimingName((CpuWallTimingType)i);
    oss << " " << timing.toString();
    if (timing.count > 0) {
      oss << " wallNanos-avg:" << timing.wallNanos / timing.count;
      oss << " cpuNanos-avg:" << timing.cpuNanos / timing.count;
    }
    LOG(INFO) << oss.str();
  }
#endif
}

void VeloxRssSortShuffleWriter::setSortState(SortState state) {
  sortState_ = state;
}

} // namespace gluten
