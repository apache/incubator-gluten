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
#include "utils/Timer.h"

//#include "velox/external/timsort/TimSort.hpp"

namespace gluten {

namespace {
constexpr uint32_t kMaskLower27Bits = (1 << 27) - 1;
constexpr uint64_t kMaskLower40Bits = (1UL << 40) - 1;

uint64_t toCompactRowId(uint32_t partitionId, uint32_t pageNumber, uint32_t offsetInPage) {
  // |63 partitionId(24) |39 inputIndex(13) |26 rowIndex(27) |
  return (uint64_t)partitionId << 40 | (uint64_t)pageNumber << 27 | offsetInPage;
}

uint32_t extractPartitionId(uint64_t compactRowId) {
  return (uint32_t)(compactRowId >> 40);
}

std::pair<uint32_t, uint32_t> extractPageNumberAndOffset(uint64_t compactRowId) {
  return {(compactRowId & kMaskLower40Bits) >> 27, compactRowId & kMaskLower27Bits};
}
} // namespace

arrow::Result<std::shared_ptr<VeloxShuffleWriter>> VeloxSortShuffleWriter::create(
    uint32_t numPartitions,
    std::unique_ptr<PartitionWriter> partitionWriter,
    ShuffleWriterOptions options,
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
    arrow::MemoryPool* arrowPool) {
  std::shared_ptr<VeloxSortShuffleWriter> writer(new VeloxSortShuffleWriter(
      numPartitions, std::move(partitionWriter), std::move(options), std::move(veloxPool), arrowPool));
  RETURN_NOT_OK(writer->init());
  return writer;
}

VeloxSortShuffleWriter::VeloxSortShuffleWriter(
    uint32_t numPartitions,
    std::unique_ptr<PartitionWriter> partitionWriter,
    ShuffleWriterOptions options,
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
    arrow::MemoryPool* pool)
    : VeloxShuffleWriter(numPartitions, std::move(partitionWriter), std::move(options), std::move(veloxPool), pool),
      allocator_{std::make_unique<facebook::velox::HashStringAllocator>(veloxPool_.get())},
      array_{SortArray{Allocator(allocator_.get())}} {}

arrow::Status VeloxSortShuffleWriter::write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) {
  ARROW_ASSIGN_OR_RAISE(auto rv, getPeeledRowVector(cb));
  initRowType(rv);
  RETURN_NOT_OK(insert(rv, memLimit));
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::stop() {
  ARROW_RETURN_IF(evictState_ == EvictState::kUnevictable, arrow::Status::Invalid("Unevictable state in stop."));

  EvictGuard evictGuard{evictState_};

  stopped_ = true;
  RETURN_NOT_OK(evictAllPartitions());
  RETURN_NOT_OK(partitionWriter_->stop(&metrics_));
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  if (evictState_ == EvictState::kUnevictable || offset_ == 0) {
    *actual = 0;
    return arrow::Status::OK();
  }
  EvictGuard evictGuard{evictState_};
  auto beforeReclaim = veloxPool_->usedBytes();
  RETURN_NOT_OK(evictAllPartitions());
  *actual = beforeReclaim - veloxPool_->usedBytes();
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::init() {
  ARROW_RETURN_IF(
      options_.partitioning == Partitioning::kSingle,
      arrow::Status::Invalid("VeloxSortShuffleWriter doesn't support single partition."));
  partition2RowCount_.resize(numPartitions_, 0);
  array_.resize(initialSize_);
  return arrow::Status::OK();
}

void VeloxSortShuffleWriter::initRowType(const facebook::velox::RowVectorPtr& rv) {
  if (UNLIKELY(!rowType_)) {
    rowType_ = facebook::velox::asRowType(rv->type());
    fixedRowSize_ = facebook::velox::row::CompactRow::fixedRowSize(rowType_);
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
    auto pidArr = getFirstColumn(*(pidBatch->getRowVector()));
    RETURN_NOT_OK(partitioner_->compute(pidArr, pidBatch->numRows(), row2Partition_, partition2RowCount_));

    auto rvBatch = VeloxColumnarBatch::from(veloxPool_.get(), batches[1]);
    return rvBatch->getFlattenedRowVector();
  } else {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    auto rv = veloxColumnBatch->getFlattenedRowVector();
    if (partitioner_->hasPid()) {
      auto pidArr = getFirstColumn(*rv);
      RETURN_NOT_OK(partitioner_->compute(pidArr, rv->size(), row2Partition_, partition2RowCount_));
      return getStrippedRowVector(*rv);
    } else {
      RETURN_NOT_OK(partitioner_->compute(nullptr, rv->size(), row2Partition_, partition2RowCount_));
      return rv;
    }
  }
}

arrow::Status VeloxSortShuffleWriter::insert(const facebook::velox::RowVectorPtr& vector, int64_t memLimit) {
  ScopedTimer timer(&c2rTime_);
  auto inputRows = vector->size();
  VELOX_DCHECK_GT(inputRows, 0);

  facebook::velox::row::CompactRow row(vector);

  if (!fixedRowSize_) {
    rowSizes_.resize(inputRows + 1);
    rowSizes_[0] = 0;
    for (auto i = 0; i < inputRows; ++i) {
      rowSizes_[i + 1] = rowSizes_[i] + row.rowSize(i);
    }
  }

  uint32_t rowOffset = 0;
  while (rowOffset < inputRows) {
    auto remainingRows = inputRows - rowOffset;
    auto rows = maxRowsToInsert(rowOffset, remainingRows);
    if (rows == 0) {
      auto minSizeRequired = fixedRowSize_ ? fixedRowSize_.value() : rowSizes_[rowOffset + 1] - rowSizes_[rowOffset];
      acquireNewBuffer(memLimit, minSizeRequired);
      rows = maxRowsToInsert(rowOffset, remainingRows);
      ARROW_RETURN_IF(
          rows == 0, arrow::Status::Invalid("Failed to insert rows. Remaining rows: " + std::to_string(remainingRows)));
    }
    RETURN_NOT_OK(maybeSpill(rows));
    insertRows(row, rowOffset, rows);
    rowOffset += rows;
  }
  return arrow::Status::OK();
}

void VeloxSortShuffleWriter::insertRows(facebook::velox::row::CompactRow& row, uint32_t offset, uint32_t rows) {
  // Allocate newArray can trigger spill.
  growArrayIfNecessary(rows);
  for (auto i = offset; i < offset + rows; ++i) {
    auto size = row.serialize(i, currentPage_ + pageCursor_);
    array_[offset_++] = {toCompactRowId(row2Partition_[i], pageNumber_, pageCursor_), size};
    pageCursor_ += size;
  }
}

arrow::Status VeloxSortShuffleWriter::maybeSpill(int32_t nextRows) {
  if ((uint64_t)offset_ + nextRows > std::numeric_limits<uint32_t>::max()) {
    EvictGuard evictGuard{evictState_};
    RETURN_NOT_OK(evictAllPartitions());
  }
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::evictAllPartitions() {
  {
    ScopedTimer timer(&sortTime_);
    // TODO: Add radix sort to align with Spark.
    //gfx::timsort(array_.begin(), array_.begin() + offset_);
    std::sort(array_.begin(), array_.begin() + offset_);
  }

  size_t begin = 0;
  size_t cur = 0;
  auto pid = extractPartitionId(array_[begin].first);
  while (++cur < offset_) {
    auto curPid = extractPartitionId(array_[cur].first);
    if (curPid != pid) {
      RETURN_NOT_OK(evictPartition(pid, begin, cur));
      pid = curPid;
      begin = cur;
    }
  }
  RETURN_NOT_OK(evictPartition(pid, begin, cur));

  pageCursor_ = 0;
  pages_.clear();
  pageAddresses_.clear();

  offset_ = 0;
  array_.clear();

  sortedBuffer_ = nullptr;

  if (!stopped_) {
    // Allocate array_ can trigger spill.
    array_.resize(initialSize_);
  }
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::evictPartition(uint32_t partitionId, size_t begin, size_t end) {
  // Serialize [begin, end)
  uint32_t numRows = end - begin;
  uint64_t rawSize = numRows * sizeof(RowSizeType);
  for (auto i = begin; i < end; ++i) {
    rawSize += array_[i].second;
  }

  if (sortedBuffer_ == nullptr || sortedBuffer_->size() < rawSize) {
    sortedBuffer_ = nullptr;
    sortedBuffer_ = facebook::velox::AlignedBuffer::allocate<char>(rawSize, veloxPool_.get());
  }
  auto* rawBuffer = sortedBuffer_->asMutable<char>();

  uint64_t offset = 0;
  for (auto i = begin; i < end; ++i) {
    // size(size_t) | bytes
    auto size = array_[i].second;
    memcpy(rawBuffer + offset, &size, sizeof(RowSizeType));
    offset += sizeof(RowSizeType);
    auto index = extractPageNumberAndOffset(array_[i].first);
    memcpy(rawBuffer + offset, pageAddresses_[index.first] + index.second, size);
    offset += size;
  }
  VELOX_CHECK_EQ(offset, rawSize);

  //  std::unique_ptr<BlockPayload> payload;
  auto rawData = sortedBuffer_->as<uint8_t>();
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  buffers.push_back(std::make_shared<arrow::Buffer>(rawData, rawSize));

  auto payload = std::make_unique<InMemoryPayload>(numRows, nullptr, std::move(buffers));
  RETURN_NOT_OK(
      partitionWriter_->evict(partitionId, std::move(payload), Evict::type::kSortSpill, false, false, stopped_));
  return arrow::Status::OK();
}

uint32_t VeloxSortShuffleWriter::maxRowsToInsert(uint32_t offset, uint32_t rows) {
  // Check how many rows can be handled.
  if (pages_.empty()) {
    return 0;
  }
  auto remainingBytes = pages_.back()->size() - pageCursor_;
  if (fixedRowSize_) {
    return std::min((uint32_t)(remainingBytes / (fixedRowSize_.value())), rows);
  }
  auto beginIter = rowSizes_.begin() + 1 + offset;
  auto iter = std::upper_bound(beginIter, rowSizes_.end(), remainingBytes);
  return iter - beginIter;
}

void VeloxSortShuffleWriter::acquireNewBuffer(int64_t memLimit, uint64_t minSizeRequired) {
  auto size = std::max(std::min((uint64_t)memLimit >> 2, 64UL * 1024 * 1024), minSizeRequired);
  // Allocating new buffer can trigger spill.
  auto newBuffer = facebook::velox::AlignedBuffer::allocate<char>(size, veloxPool_.get(), 0);
  pages_.emplace_back(std::move(newBuffer));
  pageCursor_ = 0;
  pageNumber_ = pages_.size() - 1;
  currentPage_ = pages_.back()->asMutable<char>();
  pageAddresses_.emplace_back(currentPage_);
}

void VeloxSortShuffleWriter::growArrayIfNecessary(uint32_t rows) {
  auto arraySize = (uint32_t)array_.size();
  auto usableCapacity = useRadixSort_ ? arraySize / 2 : arraySize;
  while (offset_ + rows > usableCapacity) {
    arraySize <<= 1;
    usableCapacity = useRadixSort_ ? arraySize / 2 : arraySize;
  }
  if (arraySize != array_.size()) {
    array_.resize(arraySize);
  }
}

int64_t VeloxSortShuffleWriter::totalSortTime() const {
  return sortTime_;
}

int64_t VeloxSortShuffleWriter::totalC2RTime() const {
  return c2rTime_;
}
} // namespace gluten
