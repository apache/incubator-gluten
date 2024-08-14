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

namespace gluten {

namespace {
constexpr uint32_t kMaskLower27Bits = (1 << 27) - 1;
constexpr uint64_t kMaskLower40Bits = (1UL << 40) - 1;
constexpr uint32_t kPartitionIdStartByteIndex = 5;
constexpr uint32_t kPartitionIdEndByteIndex = 7;

constexpr uint32_t kSortedBufferSize = 1 * 1024 * 1024;

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
    : VeloxShuffleWriter(numPartitions, std::move(partitionWriter), std::move(options), std::move(veloxPool), pool) {}

arrow::Status VeloxSortShuffleWriter::write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) {
  ARROW_ASSIGN_OR_RAISE(auto rv, getPeeledRowVector(cb));
  initRowType(rv);
  RETURN_NOT_OK(insert(rv, memLimit));
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::stop() {
  ARROW_RETURN_IF(evictState_ == EvictState::kUnevictable, arrow::Status::Invalid("Unevictable state in stop."));

  stopped_ = true;
  if (offset_ > 0) {
    RETURN_NOT_OK(evictAllPartitions());
  }
  array_.reset();
  sortedBuffer_.reset();
  pages_.clear();
  pageAddresses_.clear();
  RETURN_NOT_OK(partitionWriter_->stop(&metrics_));
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::reclaimFixedSize(int64_t size, int64_t* actual) {
  if (evictState_ == EvictState::kUnevictable || offset_ == 0) {
    *actual = 0;
    return arrow::Status::OK();
  }
  auto beforeReclaim = veloxPool_->usedBytes();
  RETURN_NOT_OK(evictAllPartitions());
  *actual = beforeReclaim - veloxPool_->usedBytes();
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::init() {
  ARROW_RETURN_IF(
      options_.partitioning == Partitioning::kSingle,
      arrow::Status::Invalid("VeloxSortShuffleWriter doesn't support single partition."));
  allocateMinimalArray();
  sortedBuffer_ = facebook::velox::AlignedBuffer::allocate<char>(kSortedBufferSize, veloxPool_.get());
  rawBuffer_ = sortedBuffer_->asMutable<uint8_t>();
  return arrow::Status::OK();
}

void VeloxSortShuffleWriter::initRowType(const facebook::velox::RowVectorPtr& rv) {
  if (UNLIKELY(!rowType_)) {
    rowType_ = facebook::velox::asRowType(rv->type());
    fixedRowSize_ = facebook::velox::row::CompactRow::fixedRowSize(rowType_);
    if (fixedRowSize_) {
      *fixedRowSize_ += sizeof(RowSizeType);
    }
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
    RETURN_NOT_OK(partitioner_->compute(pidArr, pidBatch->numRows(), row2Partition_));

    auto rvBatch = VeloxColumnarBatch::from(veloxPool_.get(), batches[1]);
    return rvBatch->getFlattenedRowVector();
  } else {
    auto veloxColumnBatch = VeloxColumnarBatch::from(veloxPool_.get(), cb);
    VELOX_CHECK_NOT_NULL(veloxColumnBatch);
    auto rv = veloxColumnBatch->getFlattenedRowVector();
    if (partitioner_->hasPid()) {
      auto pidArr = getFirstColumn(*rv);
      RETURN_NOT_OK(partitioner_->compute(pidArr, rv->size(), row2Partition_));
      return getStrippedRowVector(*rv);
    } else {
      RETURN_NOT_OK(partitioner_->compute(nullptr, rv->size(), row2Partition_));
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
    rowSize_.resize(inputRows);
    rowSizePrefixSum_.resize(inputRows + 1);
    rowSizePrefixSum_[0] = 0;
    for (auto i = 0; i < inputRows; ++i) {
      auto rowSize = row.rowSize(i) + sizeof(RowSizeType);
      rowSize_[i] = rowSize;
      rowSizePrefixSum_[i + 1] = rowSizePrefixSum_[i] + rowSize;
    }
  } else {
    rowSize_.resize(inputRows, *fixedRowSize_);
  }

  uint32_t rowOffset = 0;
  while (rowOffset < inputRows) {
    auto remainingRows = inputRows - rowOffset;
    auto rows = maxRowsToInsert(rowOffset, remainingRows);
    if (rows == 0) {
      auto minSizeRequired = fixedRowSize_ ? fixedRowSize_.value() : rowSize_[rowOffset];
      acquireNewBuffer((uint64_t)memLimit, minSizeRequired);
      rows = maxRowsToInsert(rowOffset, remainingRows);
      ARROW_RETURN_IF(
          rows == 0, arrow::Status::Invalid("Failed to insert rows. Remaining rows: " + std::to_string(remainingRows)));
    }
    // Spill to avoid offset_ overflow.
    RETURN_NOT_OK(maybeSpill(rows));
    // Allocate newArray can trigger spill.
    growArrayIfNecessary(rows);
    insertRows(row, rowOffset, rows);
    rowOffset += rows;
  }
  return arrow::Status::OK();
}

void VeloxSortShuffleWriter::insertRows(facebook::velox::row::CompactRow& row, uint32_t offset, uint32_t rows) {
  VELOX_CHECK(!pages_.empty());
  for (auto i = offset; i < offset + rows; ++i) {
    auto pid = row2Partition_[i];
    arrayPtr_[offset_++] = toCompactRowId(pid, pageNumber_, pageCursor_);
    // size(RowSize) | bytes
    memcpy(currentPage_ + pageCursor_, &rowSize_[i], sizeof(RowSizeType));
    pageCursor_ += sizeof(RowSizeType);
    auto size = row.serialize(i, currentPage_ + pageCursor_);
    pageCursor_ += size;
    VELOX_DCHECK_LE(pageCursor_, currenPageSize_);
  }
}

arrow::Status VeloxSortShuffleWriter::maybeSpill(uint32_t nextRows) {
  if ((uint64_t)offset_ + nextRows > std::numeric_limits<uint32_t>::max()) {
    RETURN_NOT_OK(evictAllPartitions());
  }
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::evictAllPartitions() {
  VELOX_CHECK(offset_ > 0);
  EvictGuard evictGuard{evictState_};

  auto numRecords = offset_;
  // offset_ is used for checking spillable data.
  offset_ = 0;
  int32_t begin = 0;
  {
    ScopedTimer timer(&sortTime_);
    if (options_.useRadixSort) {
      begin = RadixSort::sort(arrayPtr_, arraySize_, numRecords, kPartitionIdStartByteIndex, kPartitionIdEndByteIndex);
    } else {
      std::sort(arrayPtr_, arrayPtr_ + numRecords);
    }
  }

  auto end = begin + numRecords;
  auto cur = begin;
  auto pid = extractPartitionId(arrayPtr_[begin]);
  while (++cur < end) {
    auto curPid = extractPartitionId(arrayPtr_[cur]);
    if (curPid != pid) {
      RETURN_NOT_OK(evictPartition(pid, begin, cur));
      pid = curPid;
      begin = cur;
    }
  }
  RETURN_NOT_OK(evictPartition(pid, begin, cur));

  if (!stopped_) {
    // Preserve the last page for use.
    auto numPages = pages_.size();
    while (--numPages) {
      pages_.pop_front();
    }
    auto& page = pages_.back();
    // Clear page for serialization.
    memset(page->asMutable<char>(), 0, page->size());
    // currentPage_ should always point to the last page.
    VELOX_CHECK(currentPage_ == page->asMutable<char>());

    pageAddresses_.resize(1);
    pageAddresses_[0] = currentPage_;
    pageNumber_ = 0;
    pageCursor_ = 0;

    // Reset and reallocate array_ to minimal size. Allocate array_ can trigger spill.
    allocateMinimalArray();
  }
  return arrow::Status::OK();
}

arrow::Status VeloxSortShuffleWriter::evictPartition(uint32_t partitionId, size_t begin, size_t end) {
  // Serialize [begin, end)
  uint64_t offset = 0;
  char* addr;
  uint32_t size;

  auto index = begin;
  while (index < end) {
    auto pageIndex = extractPageNumberAndOffset(arrayPtr_[index]);
    addr = pageAddresses_[pageIndex.first] + pageIndex.second;
    size = *(RowSizeType*)addr;
    if (offset + size > kSortedBufferSize) {
      VELOX_CHECK(offset > 0);
      auto payload = std::make_unique<InMemoryPayload>(
          index - begin,
          nullptr,
          std::vector<std::shared_ptr<arrow::Buffer>>{std::make_shared<arrow::Buffer>(rawBuffer_, offset)});
      updateSpillMetrics(payload);
      RETURN_NOT_OK(
          partitionWriter_->evict(partitionId, std::move(payload), Evict::type::kSortSpill, false, false, stopped_));
      begin = index;
      offset = 0;
    }
    // Count copy row time into sortTime_.
    ScopedTimer timer(&sortTime_);
    gluten::fastCopy(rawBuffer_ + offset, addr, size);
    offset += size;
    index++;
  }
  auto payload = std::make_unique<InMemoryPayload>(
      end - begin,
      nullptr,
      std::vector<std::shared_ptr<arrow::Buffer>>{std::make_shared<arrow::Buffer>(rawBuffer_, offset)});
  updateSpillMetrics(payload);
  RETURN_NOT_OK(
      partitionWriter_->evict(partitionId, std::move(payload), Evict::type::kSortSpill, false, false, stopped_));
  return arrow::Status::OK();
}

uint32_t VeloxSortShuffleWriter::maxRowsToInsert(uint32_t offset, uint32_t remainingRows) {
  // Check how many rows can be handled.
  if (pages_.empty()) {
    return 0;
  }
  auto remainingBytes = pages_.back()->size() - pageCursor_;
  if (fixedRowSize_) {
    return std::min((uint32_t)(remainingBytes / (fixedRowSize_.value())), remainingRows);
  }
  auto beginIter = rowSizePrefixSum_.begin() + 1 + offset;
  auto bytesWritten = rowSizePrefixSum_[offset];
  auto iter = std::upper_bound(beginIter, rowSizePrefixSum_.end(), remainingBytes + bytesWritten);
  return iter - beginIter;
}

void VeloxSortShuffleWriter::acquireNewBuffer(uint64_t memLimit, uint64_t minSizeRequired) {
  DLOG_IF(INFO, !pages_.empty()) << "Acquire new buffer. current capacity: " << pages_.back()->capacity()
                                 << ", size: " << pages_.back()->size() << ", pageCursor: " << pageCursor_
                                 << ", unused: " << pages_.back()->capacity() - pageCursor_;
  auto size = std::max(
      std::min<uint64_t>(
          std::max<uint64_t>(memLimit >> 2, facebook::velox::AlignedBuffer::kPaddedSize), 64UL * 1024 * 1024) -
          facebook::velox::AlignedBuffer::kPaddedSize,
      minSizeRequired);
  // Allocating new buffer can trigger spill.
  auto newBuffer = facebook::velox::AlignedBuffer::allocate<char>(size, veloxPool_.get());
  DLOG(INFO) << "Allocated new buffer. capacity: " << newBuffer->capacity() << ", size: " << newBuffer->size();
  auto newBufferSize = newBuffer->capacity();
  newBuffer->setSize(newBufferSize);

  currentPage_ = newBuffer->asMutable<char>();
  currenPageSize_ = newBufferSize;
  memset(currentPage_, 0, newBufferSize);

  // If spill triggered, clear pages_.
  if (offset_ == 0 && pages_.size() > 0) {
    pageAddresses_.clear();
    pages_.clear();
  }
  pageAddresses_.emplace_back(currentPage_);
  pages_.emplace_back(std::move(newBuffer));

  pageCursor_ = 0;
  pageNumber_ = pages_.size() - 1;
}

void VeloxSortShuffleWriter::growArrayIfNecessary(uint32_t rows) {
  auto newSize = newArraySize(rows);
  if (newSize > arraySize_) {
    // May trigger spill.
    auto newSizeBytes = newSize * sizeof(uint64_t);
    auto newArray = facebook::velox::AlignedBuffer::allocate<char>(newSizeBytes, veloxPool_.get());
    // Check if already satisfies (spill has been triggered).
    if (newArraySize(rows) > arraySize_) {
      if (offset_ > 0) {
        gluten::fastCopy(newArray->asMutable<void>(), arrayPtr_, offset_ * sizeof(uint64_t));
      }
      setUpArray(std::move(newArray));
    }
  }
}

uint32_t VeloxSortShuffleWriter::newArraySize(uint32_t rows) {
  auto newSize = arraySize_;
  auto usableCapacity = options_.useRadixSort ? newSize / 2 : newSize;
  while (offset_ + rows > usableCapacity) {
    newSize <<= 1;
    usableCapacity = options_.useRadixSort ? newSize / 2 : newSize;
  }
  return newSize;
}

void VeloxSortShuffleWriter::setUpArray(facebook::velox::BufferPtr&& array) {
  array_.reset();
  array_ = std::move(array);
  // Capacity is a multiple of 8 (bytes).
  auto capacity = array_->capacity() & 0xfffffff8;
  array_->setSize(capacity);
  arraySize_ = capacity >> 3;
  arrayPtr_ = array_->asMutable<uint64_t>();
}

int64_t VeloxSortShuffleWriter::peakBytesAllocated() const {
  return veloxPool_->peakBytes();
}

int64_t VeloxSortShuffleWriter::totalSortTime() const {
  return sortTime_;
}

int64_t VeloxSortShuffleWriter::totalC2RTime() const {
  return c2rTime_;
}

void VeloxSortShuffleWriter::allocateMinimalArray() {
  auto array = facebook::velox::AlignedBuffer::allocate<char>(
      options_.sortBufferInitialSize * sizeof(uint64_t), veloxPool_.get());
  setUpArray(std::move(array));
}

void VeloxSortShuffleWriter::updateSpillMetrics(const std::unique_ptr<InMemoryPayload>& payload) {
  if (!stopped_) {
    metrics_.totalBytesToEvict += payload->rawSize();
  }
}
} // namespace gluten
