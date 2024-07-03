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

#include "shuffle/VeloxSortShuffleWriter.h"
#include <arrow/io/memory.h>
#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "utils/Common.h"

namespace gluten {
arrow::Result<std::shared_ptr<VeloxShuffleWriter>> VeloxSortShuffleWriter::create(
    uint32_t numPartitions,
    std::unique_ptr<PartitionWriter> partitionWriter,
    ShuffleWriterOptions options,
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
    arrow::MemoryPool* arrowPool) {
  std::shared_ptr<VeloxSortShuffleWriter> writer(new VeloxSortShuffleWriter(
      numPartitions, std::move(partitionWriter), std::move(options), std::move(veloxPool), arrowPool));
  writer->init();
  return writer;
}

VeloxSortShuffleWriter::VeloxSortShuffleWriter(
    uint32_t numPartitions,
    std::unique_ptr<PartitionWriter> partitionWriter,
    ShuffleWriterOptions options,
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
    arrow::MemoryPool* pool)
    : VeloxShuffleWriter(numPartitions, std::move(partitionWriter), std::move(options), std::move(veloxPool), pool) {}

facebook::velox::VectorPtr VeloxSortShuffleWriter::localSort(facebook::velox::VectorPtr vector) {
  VELOX_DCHECK_GT(vector->size(), 0);

  std::vector<std::pair<uint32_t, facebook::velox::vector_size_t>> pidWithRowId;
  pidWithRowId.reserve(vector->size());
  for (auto i = 0; i < vector->size(); ++i) {
    auto pid = row2Partition_[i];
    pidWithRowId.emplace_back(pid, i);
  }

  std::sort(pidWithRowId.begin(), pidWithRowId.end());

  // Build dictionary index for partition id.
  facebook::velox::BufferPtr indices =
      facebook::velox::AlignedBuffer::allocate<facebook::velox::vector_size_t>(vector->size(), veloxPool_.get());
  auto rawIndices = indices->asMutable<facebook::velox::vector_size_t>();
  for (facebook::velox::vector_size_t i = 0; i < vector->size(); i++) {
    rawIndices[i] = pidWithRowId[i].second;
  }

  // Sort vector by flattening the dictionary.
  auto sortedVector = facebook::velox::BaseVector::wrapInDictionary(
      facebook::velox::BufferPtr(nullptr), indices, vector->size(), vector);
  facebook::velox::BaseVector::flattenVector(sortedVector);

  // Build pid to RowVector index + row range.
  auto pid = pidWithRowId[0].first;
  auto startRow = 0;
  auto rowVectorIndex = sortedVector_.size();
  facebook::velox::vector_size_t rowCnt = 1;
  for (auto i = 1; i < vector->size(); ++i) {
    if (pidWithRowId[i].first != pid) {
      if (partitionRowIndices_.find(pid) == partitionRowIndices_.end()) {
        partitionRowIndices_[pid] = {};
      }
      partitionRowIndices_[pid].emplace_back(rowVectorIndex, facebook::velox::IndexRange{startRow, rowCnt});
      pid = pidWithRowId[i].first;
      startRow = i;
      rowCnt = 1;
    } else {
      ++rowCnt;
    }
  }

  if (partitionRowIndices_.find(pid) == partitionRowIndices_.end()) {
    partitionRowIndices_[pid] = {};
  }
  partitionRowIndices_[pid].emplace_back(rowVectorIndex, facebook::velox::IndexRange{startRow, rowCnt});
  return sortedVector;
}

arrow::Status VeloxSortShuffleWriter::write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) {
  if (options_.partitioning == Partitioning::kSingle) {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    auto rv = veloxColumnBatch->getFlattenedRowVector();
    initRowType(rv);

    // Serialize.
    arenas_[0] = std::make_unique<facebook::velox::StreamArena>(veloxPool_.get());
    partitionSerializer_[0] = serde_.createIterativeSerializer(rowType_, rv->size(), arenas_[0].get(), &serdeOptions_);
    partitionSerializer_[0]->append(rv);
    RETURN_NOT_OK(evict(0));
    // Free immediately.
    partitionSerializer_[0] = nullptr;
    arenas_[0] = nullptr;
  } else {
    ARROW_ASSIGN_OR_RAISE(auto rv, getPeeledRowVector(cb));
    initRowType(rv);
    auto sorted = localSort(rv);
    sortedVector_.push_back(std::move(sorted));
  }
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  //  RETURN_NOT_OK(partitionWriter_->reclaimFixedSize(size, actual));
  auto beforeReclaim = veloxPool_->usedBytes();
  RETURN_NOT_OK(evictAllPartitions());
  *actual = beforeReclaim - veloxPool_->usedBytes();
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::stop(int64_t memLimit) {
  RETURN_NOT_OK(evictAllPartitions());
  RETURN_NOT_OK(partitionWriter_->stop(&metrics_));
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::evict(uint32_t partitionId) {
  auto serializedSize = partitionSerializer_[partitionId]->maxSerializedSize();
  auto requiredSize = serializedSize + 8; // 8 bytes for serialized size.
  auto& flushBuffer = partitionFlushBuffer_[partitionId];
  if (flushBuffer == nullptr) {
    ARROW_ASSIGN_OR_RAISE(flushBuffer, arrow::AllocateResizableBuffer(requiredSize, partitionBufferPool_.get()));
  } else if (requiredSize > flushBuffer->capacity()) {
    RETURN_NOT_OK(flushBuffer->Reserve(requiredSize));
  }

  auto valueBuffer = arrow::SliceMutableBuffer(flushBuffer, 8, requiredSize);
  auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
  facebook::velox::serializer::presto::PrestoOutputStreamListener listener;
  ArrowFixedSizeBufferOutputStream out(output, &listener);
  partitionSerializer_[partitionId]->flush(&out);

  uint64_t actualSerializedSize = out.tellp();
  fastCopy(flushBuffer->mutable_data_as<char>(), &actualSerializedSize, 8);
  *reinterpret_cast<uint64_t*>(flushBuffer->mutable_data()) = actualSerializedSize;
  RETURN_NOT_OK(
      partitionWriter_->evict(partitionId, serializedSize, flushBuffer->data_as<char>(), actualSerializedSize + 8));
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::evictAllPartitions() {
  for (const auto& kv : partitionRowIndices_) {
    auto pid = kv.first;
    auto indices = kv.second;

    auto remainingRows = partition2RowCount_[pid];
    arenas_[pid] = std::make_unique<facebook::velox::StreamArena>(veloxPool_.get());
    partitionSerializer_[pid] = serde_.createIterativeSerializer(
        rowType_, std::min((uint32_t)options_.bufferSize, remainingRows), arenas_[pid].get(), &serdeOptions_);

    facebook::velox::vector_size_t appendedRows = 0;
    for (const auto& ranges : indices) {
      auto rowVectorIdx = ranges.first;
      auto rowVector = std::dynamic_pointer_cast<facebook::velox::RowVector>(sortedVector_[rowVectorIdx]);

      // Split to small runs to limit evict rows <= 4k.
      if (appendedRows + ranges.second.size > options_.bufferSize) {
        RETURN_NOT_OK(evict(pid));
        // Reset.
        remainingRows -= appendedRows;
        appendedRows = 0;
        partitionSerializer_[pid] = serde_.createIterativeSerializer(
            rowType_, std::min((uint32_t)options_.bufferSize, remainingRows), arenas_[pid].get(), &serdeOptions_);
      }
      appendedRows += ranges.second.size;
      partitionSerializer_[pid]->append(rowVector, folly::Range(&ranges.second, 1));
    }
    RETURN_NOT_OK(evict(pid));
    partitionSerializer_[pid] = nullptr;
    arenas_[pid] = nullptr;
    partition2RowCount_[pid] = 0;
  }
  partitionRowIndices_.clear();
  sortedVector_.clear();

  return arrow::Status::OK();
}

void VeloxSortShuffleWriter::init() {
  partition2RowCount_.resize(numPartitions_, 0);
  partitionSerializer_.resize(numPartitions_);
  partitionFlushBuffer_.resize(numPartitions_);
}

void VeloxSortShuffleWriter::initRowType(const facebook::velox::RowVectorPtr& rv) {
  if (UNLIKELY(!rowType_)) {
    rowType_ = facebook::velox::asRowType(rv->type());
  }
}

arrow::Result<facebook::velox::RowVectorPtr> VeloxSortShuffleWriter::getPeeledRowVector(
    const std::shared_ptr<ColumnarBatch>& cb) {
  if (options_.partitioning == Partitioning::kRange) {
    auto compositeBatch = std::dynamic_pointer_cast<CompositeColumnarBatch>(cb);
    VELOX_CHECK_NOT_NULL(compositeBatch);
    auto batches = compositeBatch->getBatches();
    VELOX_CHECK_EQ(batches.size(), 2);

    auto pidBatch = VeloxColumnarBatch::from(veloxPool_.get(), batches[0]);
    auto pidArr = getFirstColumn(pidBatch->getRowVector());
    RETURN_NOT_OK(partitioner_->compute(
        pidArr->asFlatVector<int32_t>()->rawValues(), pidBatch->numRows(), row2Partition_, partition2RowCount_));

    auto rvBatch = VeloxColumnarBatch::from(veloxPool_.get(), batches[1]);
    return rvBatch->getFlattenedRowVector();
  } else {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    auto rv = veloxColumnBatch->getFlattenedRowVector();
    if (partitioner_->hasPid()) {
      auto pidArr = getFirstColumn(rv);
      RETURN_NOT_OK(partitioner_->compute(
          pidArr->asFlatVector<int32_t>()->rawValues(), rv->size(), row2Partition_, partition2RowCount_));
      return getStrippedRowVector(*rv);
    } else {
      RETURN_NOT_OK(partitioner_->compute(nullptr, rv->size(), row2Partition_, partition2RowCount_));
      return rv;
    }
  }
}
} // namespace gluten
