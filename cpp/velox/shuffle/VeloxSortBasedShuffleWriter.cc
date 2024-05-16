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

#include "VeloxSortBasedShuffleWriter.h"
#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "shuffle/ShuffleSchema.h"
#include "utils/Common.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/macros.h"
#include "velox/common/base/Nulls.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

#if defined(__x86_64__)
#include <immintrin.h>
#include <x86intrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace gluten {

#define VELOX_SHUFFLE_WRITER_LOG_FLAG 0

// macro to rotate left an 8-bit value 'x' given the shift 's' is a 32-bit integer
// (x is left shifted by 's' modulo 8) OR (x right shifted by (8 - 's' modulo 8))
#if !defined(__x86_64__)
#define rotateLeft(x, s) (x << (s - ((s >> 3) << 3)) | x >> (8 - (s - ((s >> 3) << 3))))
#endif

// on x86 machines, _MM_HINT_T0,T1,T2 are defined as 1, 2, 3
// equivalent mapping to __builtin_prefetch hints is 3, 2, 1
#if defined(__x86_64__)
#define PREFETCHT0(ptr) _mm_prefetch(ptr, _MM_HINT_T0)
#define PREFETCHT1(ptr) _mm_prefetch(ptr, _MM_HINT_T1)
#define PREFETCHT2(ptr) _mm_prefetch(ptr, _MM_HINT_T2)
#else
#define PREFETCHT0(ptr) __builtin_prefetch(ptr, 0, 3)
#define PREFETCHT1(ptr) __builtin_prefetch(ptr, 0, 2)
#define PREFETCHT2(ptr) __builtin_prefetch(ptr, 0, 1)
#endif

arrow::Result<std::shared_ptr<VeloxShuffleWriter>> VeloxSortBasedShuffleWriter::create(
    uint32_t numPartitions,
    std::unique_ptr<PartitionWriter> partitionWriter,
    ShuffleWriterOptions options,
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
    arrow::MemoryPool* arrowPool) {
  std::shared_ptr<VeloxSortBasedShuffleWriter> res(new VeloxSortBasedShuffleWriter(
      numPartitions, std::move(partitionWriter), std::move(options), veloxPool, arrowPool));
  RETURN_NOT_OK(res->init());
  return res;
} // namespace gluten

arrow::Status VeloxSortBasedShuffleWriter::init() {
#if defined(__x86_64__)
  supportAvx512_ = __builtin_cpu_supports("avx512bw");
#else
  supportAvx512_ = false;
#endif

  ARROW_ASSIGN_OR_RAISE(
      partitioner_, Partitioner::make(options_.partitioning, numPartitions_, options_.startPartitionId));
  DLOG(INFO) << "Create partitioning type: " << std::to_string(options_.partitioning);

  partition2RowCount_.resize(numPartitions_);
  rowVectorIndexMap_.reserve(numPartitions_);
  for (auto pid = 0; pid < numPartitions_; ++pid) {
    rowVectorIndexMap_[pid].reserve(options_.bufferSize);
  }

  return arrow::Status::OK();
}

arrow::Status VeloxSortBasedShuffleWriter::doSort(facebook::velox::RowVectorPtr rv, int64_t memLimit) {
  std::cout << "currentInputColumnBytes_:" << currentInputColumnBytes_
            << ", rv->estimateFlatSize():" << rv->estimateFlatSize() << ", memLimit:" << memLimit << std::endl;
  currentInputColumnBytes_ += rv->estimateFlatSize();
  batches_.push_back(rv);
  if (currentInputColumnBytes_ > memLimit) {
    std::cout << "currentInputColumnBytes_ > memLimit:" << currentInputColumnBytes_ << std::endl;
    for (auto pid = 0; pid < numPartitions(); ++pid) {
      RETURN_NOT_OK(evictRowVector(pid));
      partition2RowCount_[pid] = 0;
    }
    batches_.clear();
    currentInputColumnBytes_ = 0;
  }
  setSortState(SortState::kSortInit);
  return arrow::Status::OK();
}

arrow::Status VeloxSortBasedShuffleWriter::write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) {
  if (options_.partitioning == Partitioning::kSingle) {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    auto rv = veloxColumnBatch->getFlattenedRowVector();
    RETURN_NOT_OK(initFromRowVector(*rv.get()));
    RETURN_NOT_OK(doSort(rv, partitionWriter_.get()->options().sortBufferMaxSize));
  } else if (options_.partitioning == Partitioning::kRange) {
    auto compositeBatch = std::dynamic_pointer_cast<CompositeColumnarBatch>(cb);
    VELOX_CHECK_NOT_NULL(compositeBatch);
    auto batches = compositeBatch->getBatches();
    VELOX_CHECK_EQ(batches.size(), 2);
    auto pidBatch = VeloxColumnarBatch::from(veloxPool_.get(), batches[0]);
    auto pidArr = getFirstColumn(*(pidBatch->getRowVector()));
    START_TIMING(cpuWallTimingList_[CpuWallTimingCompute]);
    setSortState(SortState::kSort);
    RETURN_NOT_OK(partitioner_->compute(pidArr, pidBatch->numRows(), batches_.size(), rowVectorIndexMap_));
    END_TIMING();
    auto rvBatch = VeloxColumnarBatch::from(veloxPool_.get(), batches[1]);
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

arrow::Status VeloxSortBasedShuffleWriter::evictBatch(
    uint32_t partitionId,
    std::ostringstream* output,
    facebook::velox::OStreamOutputStream* out,
    facebook::velox::RowTypePtr* rowTypePtr) {
  int64_t rawSize = batch_->size();
  batch_->flush(out);
  const std::string& outputStr = output->str();
  RETURN_NOT_OK(partitionWriter_->evict(partitionId, rawSize, outputStr.c_str(), outputStr.size()));
  batch_.reset();
  output->clear();
  output->str("");
  batch_ = std::make_unique<facebook::velox::VectorStreamGroup>(veloxPool_.get(), serde_.get());
  batch_->createStreamTree(*rowTypePtr, options_.bufferSize, &serdeOptions_);
  return arrow::Status::OK();
}

arrow::Status VeloxSortBasedShuffleWriter::evictRowVector(uint32_t partitionId) {
  int32_t rowNum = 0;
  const int32_t maxBatchNum = options_.bufferSize;
  auto rowTypePtr = std::static_pointer_cast<const facebook::velox::RowType>(rowType_.value());
  std::ostringstream output;
  facebook::velox::OStreamOutputStream out(&output);

  if (options_.partitioning != Partitioning::kSingle) {
    if (auto it = rowVectorIndexMap_.find(partitionId); it != rowVectorIndexMap_.end()) {
      auto rowVectorIndex = it->second;
      const int32_t outputSize = rowVectorIndex.size();

      std::unordered_map<int32_t, std::vector<facebook::velox::IndexRange>> groupedIndices;
      std::unordered_map<int32_t, int64_t> groupedSize;

      int32_t tempVectorIndex = -1;
      int32_t baseRowIndex = -1;
      int32_t tempRowIndex = -1;
      int32_t size = 1;
      for (int start = 0; start < outputSize; start++) {
        const int64_t rowVector = rowVectorIndex[start];
        const int32_t vectorIndex = static_cast<int32_t>(rowVector >> 32);
        const int32_t rowIndex = static_cast<int32_t>(rowVector & 0xFFFFFFFFLL);
        if (tempVectorIndex == -1) {
          tempVectorIndex = vectorIndex;
          baseRowIndex = rowIndex;
          tempRowIndex = rowIndex;
        } else {
          if (vectorIndex == tempVectorIndex && rowIndex == tempRowIndex + 1) {
            size += 1;
            tempRowIndex = rowIndex;
          } else {
            groupedIndices[tempVectorIndex].push_back({baseRowIndex, size});
            groupedSize[tempVectorIndex] += size;
            size = 1;
            tempVectorIndex = vectorIndex;
            baseRowIndex = rowIndex;
            tempRowIndex = rowIndex;
          }
        }
      }
      groupedIndices[tempVectorIndex].push_back({baseRowIndex, size});
      groupedSize[tempVectorIndex] += size;

      for (auto& pair : groupedIndices) {
        batch_->append(batches_[pair.first], pair.second);
        rowNum += groupedSize[pair.first];
        if (rowNum >= maxBatchNum) {
          rowNum = 0;
          RETURN_NOT_OK(evictBatch(partitionId, &output, &out, &rowTypePtr));
        }
      }

      rowVectorIndex.clear();
      rowVectorIndexMap_.erase(partitionId);
    }
  } else {
    for (facebook::velox::RowVectorPtr rowVectorPtr : batches_) {
      rowNum += rowVectorPtr->size();
      batch_->append(rowVectorPtr);
      if (rowNum >= maxBatchNum) {
        RETURN_NOT_OK(evictBatch(partitionId, &output, &out, &rowTypePtr));
        rowNum = 0;
      }
    }
  }
  if (rowNum > 0) {
    RETURN_NOT_OK(evictBatch(partitionId, &output, &out, &rowTypePtr));
  }
  return arrow::Status::OK();
}

arrow::Status VeloxSortBasedShuffleWriter::stop() {
  for (auto pid = 0; pid < numPartitions(); ++pid) {
    RETURN_NOT_OK(evictRowVector(pid));
    partition2RowCount_[pid] = 0;
  }
  batches_.clear();
  currentInputColumnBytes_ = 0;
  {
    SCOPED_TIMER(cpuWallTimingList_[CpuWallTimingStop]);
    setSortState(SortState::kSortStop);
    RETURN_NOT_OK(partitionWriter_->stop(&metrics_));
    partitionBuffers_.clear();
  }

  stat();

  return arrow::Status::OK();
}

arrow::Status VeloxSortBasedShuffleWriter::initFromRowVector(const facebook::velox::RowVector& rv) {
  if (!rowType_.has_value()) {
    rowType_ = rv.type();
    serdeOptions_ = {
        false, facebook::velox::common::stringToCompressionKind(partitionWriter_->options().compressionTypeStr)};
    batch_ = std::make_unique<facebook::velox::VectorStreamGroup>(veloxPool_.get(), serde_.get());
    batch_->createStreamTree(
        std::static_pointer_cast<const facebook::velox::RowType>(rowType_.value()),
        options_.bufferSize,
        &serdeOptions_);
  }
  return arrow::Status::OK();
}

arrow::Status VeloxSortBasedShuffleWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  if (evictState_ == EvictState::kUnevictable) {
    *actual = 0;
    return arrow::Status::OK();
  }
  EvictGuard evictGuard{evictState_};

  if (sortState_ == SortState::kSortInit) {
    for (auto pid = 0; pid < numPartitions(); ++pid) {
      RETURN_NOT_OK(evictRowVector(pid));
      partition2RowCount_[pid] = 0;
    }
    batches_.clear();
    *actual = currentInputColumnBytes_;
    currentInputColumnBytes_ = 0;
  }
  return arrow::Status::OK();
}

void VeloxSortBasedShuffleWriter::stat() const {
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

void VeloxSortBasedShuffleWriter::setSortState(SortState state) {
  sortState_ = state;
}

} // namespace gluten
