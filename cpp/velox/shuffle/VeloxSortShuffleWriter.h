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

#pragma once

#include "shuffle/RadixSort.h"
#include "shuffle/VeloxShuffleWriter.h"

#include <arrow/status.h>
#include <map>
#include <vector>

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/row/CompactRow.h"
#include "velox/vector/BaseVector.h"

namespace gluten {

class VeloxSortShuffleWriter final : public VeloxShuffleWriter {
 public:
  using RowSizeType = uint32_t;

  static arrow::Result<std::shared_ptr<VeloxShuffleWriter>> create(
      uint32_t numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions options,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      arrow::MemoryPool* arrowPool);

  arrow::Status write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) override;

  arrow::Status stop() override;

  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  int64_t peakBytesAllocated() const override;

  int64_t totalSortTime() const override;

  int64_t totalC2RTime() const override;

 private:
  VeloxSortShuffleWriter(
      uint32_t numPartitions,
      std::unique_ptr<PartitionWriter> partitionWriter,
      ShuffleWriterOptions options,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      arrow::MemoryPool* pool);

  arrow::Status init();

  void initRowType(const facebook::velox::RowVectorPtr& rv);

  arrow::Result<facebook::velox::RowVectorPtr> getPeeledRowVector(const std::shared_ptr<ColumnarBatch>& cb);

  arrow::Status insert(const facebook::velox::RowVectorPtr& vector, int64_t memLimit);

  void insertRows(facebook::velox::row::CompactRow& row, uint32_t offset, uint32_t rows);

  arrow::Status maybeSpill(int32_t nextRows);

  arrow::Status evictAllPartitions();

  arrow::Status evictPartition(uint32_t partitionId, size_t begin, size_t end);

  uint32_t maxRowsToInsert(uint32_t offset, uint32_t rows);

  void acquireNewBuffer(uint64_t memLimit, uint64_t minSizeRequired);

  void growArrayIfNecessary(uint32_t rows);

  uint32_t newArraySize(uint32_t rows);

  void initArray();

  static int compare(const void* a, const void* b);

  // Stores compact row id -> row
  facebook::velox::BufferPtr array_;
  uint64_t* arrayPtr_;
  uint32_t arraySize_;
  uint32_t offset_{0};

  std::list<facebook::velox::BufferPtr> pages_;
  std::vector<char*> pageAddresses_;
  char* currentPage_;
  uint32_t pageNumber_;
  uint32_t pageCursor_;
  // For debug.
  uint32_t currenPageSize_;

  facebook::velox::BufferPtr sortedBuffer_;
  uint8_t* rawBuffer_;

  // Row ID -> Partition ID
  // subscript: The index of row in the current input RowVector
  // value: Partition ID
  // Updated for each input RowVector.
  std::vector<uint32_t> row2Partition_;

  std::shared_ptr<const facebook::velox::RowType> rowType_;
  std::optional<int32_t> fixedRowSize_;
  std::vector<RowSizeType> rowSize_;
  std::vector<uint64_t> rowSizePrefixSum_;

  int64_t c2rTime_{0};
  int64_t sortTime_{0};
  bool stopped_{false};
};
} // namespace gluten
