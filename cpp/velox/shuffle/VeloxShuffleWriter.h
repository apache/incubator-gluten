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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "velox/common/time/CpuWallTimer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorStream.h"

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/checked_cast.h>
#include "arrow/array/builder_base.h"

#include "arrow/array/util.h"
#include "arrow/result.h"

#include "memory/VeloxMemoryManager.h"
#include "shuffle/PartitionWriterCreator.h"
#include "shuffle/Partitioner.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/Utils.h"

#include "utils/Print.h"

namespace gluten {

// set 1 to open print
#define VELOX_SHUFFLE_WRITER_PRINT 0

#if VELOX_SHUFFLE_WRITER_PRINT

#define VsPrint Print
#define VsPrintLF PrintLF
#define VsPrintSplit PrintSplit
#define VsPrintSplitLF PrintSplitLF
#define VsPrintVectorRange PrintVectorRange
#define VS_PRINT PRINT
#define VS_PRINTLF PRINTLF
#define VS_PRINT_FUNCTION_NAME PRINT_FUNCTION_NAME
#define VS_PRINT_FUNCTION_SPLIT_LINE PRINT_FUNCTION_SPLIT_LINE
#define VS_PRINT_CONTAINER PRINT_CONTAINER
#define VS_PRINT_CONTAINER_TO_STRING PRINT_CONTAINER_TO_STRING
#define VS_PRINT_CONTAINER_2_STRING PRINT_CONTAINER_2_STRING
#define VS_PRINT_VECTOR_TO_STRING PRINT_VECTOR_TO_STRING
#define VS_PRINT_VECTOR_2_STRING PRINT_VECTOR_2_STRING
#define VS_PRINT_VECTOR_MAPPING PRINT_VECTOR_MAPPING

#else // VELOX_SHUFFLE_WRITER_PRINT

#define VsPrint(...) // NOLINT
#define VsPrintLF(...) // NOLINT
#define VsPrintSplit(...) // NOLINT
#define VsPrintSplitLF(...) // NOLINT
#define VsPrintVectorRange(...) // NOLINT
#define VS_PRINT(a)
#define VS_PRINTLF(a)
#define VS_PRINT_FUNCTION_NAME()
#define VS_PRINT_FUNCTION_SPLIT_LINE()
#define VS_PRINT_CONTAINER(c)
#define VS_PRINT_CONTAINER_TO_STRING(c)
#define VS_PRINT_CONTAINER_2_STRING(c)
#define VS_PRINT_VECTOR_TO_STRING(v)
#define VS_PRINT_VECTOR_2_STRING(v)
#define VS_PRINT_VECTOR_MAPPING(v)

#endif // end of VELOX_SHUFFLE_WRITER_PRINT

enum SplitState { kInit, kPreAlloc, kSplit, kStop };

class VeloxShuffleWriter final : public ShuffleWriter {
  enum { kValidityBufferIndex = 0, kOffsetBufferIndex = 1, kValueBufferIndex = 2 };

 public:
  struct BinaryBuf {
    BinaryBuf(uint8_t* value, uint8_t* offset, uint64_t valueCapacityIn, uint64_t valueOffsetIn)
        : valuePtr(value), offsetPtr(offset), valueCapacity(valueCapacityIn), valueOffset(valueOffsetIn) {}

    BinaryBuf(uint8_t* value, uint8_t* offset, uint64_t valueCapacity) : BinaryBuf(value, offset, valueCapacity, 0) {}

    BinaryBuf() : BinaryBuf(nullptr, nullptr, 0) {}

    uint8_t* valuePtr;
    uint8_t* offsetPtr;
    uint64_t valueCapacity;
    uint64_t valueOffset;
  };

  static arrow::Result<std::shared_ptr<VeloxShuffleWriter>> create(
      uint32_t numPartitions,
      std::shared_ptr<PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool);

  arrow::Status split(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) override;

  arrow::Status stop() override;

  arrow::Status evictFixedSize(int64_t size, int64_t* actual) override;

  arrow::Result<std::unique_ptr<arrow::ipc::IpcPayload>> createPayloadFromBuffer(
      uint32_t partitionId,
      bool reuseBuffers) override;

  arrow::Status evictPayload(uint32_t partitionId, std::unique_ptr<arrow::ipc::IpcPayload> payload) override;

  const uint64_t cachedPayloadSize() const override;

  int64_t rawPartitionBytes() const {
    return std::accumulate(rawPartitionLengths_.begin(), rawPartitionLengths_.end(), 0LL);
  }

  // for testing
  const std::string& dataFile() const {
    return options_.data_file;
  }

  // for debugging
  void printColumnsInfo() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINTLF(fixed_width_column_count_);

    VS_PRINT_CONTAINER(simple_column_indices_);
    VS_PRINT_CONTAINER(binary_column_indices_);
    VS_PRINT_CONTAINER(complex_column_indices_);

    VS_PRINT_VECTOR_2_STRING(velox_column_types_);
    VS_PRINT_VECTOR_TO_STRING(arrow_column_types_);
  }

  void printPartition() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    // row ID -> partition ID
    VS_PRINT_VECTOR_MAPPING(row_2_partition_);

    // partition -> row count
    VS_PRINT_VECTOR_MAPPING(partition_2_row_count_);
  }

  void printPartitionBuffer() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINT_VECTOR_MAPPING(partition_2_buffer_size_);
    VS_PRINT_VECTOR_MAPPING(partitionBufferIdxBase_);
  }

  void printPartition2Row() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINT_VECTOR_MAPPING(partition2RowOffset_);

#if VELOX_SHUFFLE_WRITER_PRINT
    for (auto pid = 0; pid < numPartitions_; ++pid) {
      auto begin = partition2RowOffset_[pid];
      auto end = partition2RowOffset_[pid + 1];
      VsPrint("partition", pid);
      VsPrintVectorRange(rowOffset2RowId_, begin, end);
    }
#endif
  }

  void printInputHasNull() const {
    VS_PRINT_FUNCTION_SPLIT_LINE();
    VS_PRINT_CONTAINER(input_has_null_);
  }

  // Public for test only.
  void setSplitState(SplitState state) {
    splitState_ = state;
  }

  // For test only.
  SplitState getSplitState() {
    return splitState_;
  }

 protected:
  VeloxShuffleWriter(
      uint32_t numPartitions,
      std::shared_ptr<PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool)
      : ShuffleWriter(numPartitions, partitionWriterCreator, options),
        payloadPool_(std::make_shared<ShuffleMemoryPool>(options_.memory_pool)),
        veloxPool_(std::move(veloxPool)) {
    arenas_.resize(numPartitions);
  }

  arrow::Status init();

  arrow::Status initIpcWriteOptions();

  arrow::Status initPartitions();

  arrow::Status initColumnTypes(const facebook::velox::RowVector& rv);

  arrow::Status splitRowVector(const facebook::velox::RowVector& rv);

  arrow::Status initFromRowVector(const facebook::velox::RowVector& rv);

  arrow::Status buildPartition2Row(uint32_t rowNum);

  arrow::Status updateInputHasNull(const facebook::velox::RowVector& rv);

  arrow::Status doSplit(const facebook::velox::RowVector& rv, int64_t memLimit);

  bool beyondThreshold(uint32_t partitionId, uint64_t newSize);

  uint32_t calculatePartitionBufferSize(const facebook::velox::RowVector& rv, int64_t memLimit);

  arrow::Status preAllocPartitionBuffers(uint32_t preAllocBufferSize);

  arrow::Status updateValidityBuffers(uint32_t partitionId, uint32_t newSize, bool reset);

  arrow::Result<std::shared_ptr<arrow::ResizableBuffer>>
  allocateValidityBuffer(uint32_t col, uint32_t partitionId, uint32_t newSize);

  arrow::Status allocatePartitionBuffer(uint32_t partitionId, uint32_t newSize, bool reuseBuffers);

  arrow::Status splitFixedWidthValueBuffer(const facebook::velox::RowVector& rv);

  arrow::Status splitBoolType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs);

  arrow::Status splitValidityBuffer(const facebook::velox::RowVector& rv);

  arrow::Status splitBinaryArray(const facebook::velox::RowVector& rv);

  arrow::Status splitComplexType(const facebook::velox::RowVector& rv);

  arrow::Status evictPartitionBuffer(uint32_t partitionId, uint32_t newSize, bool reuseBuffers);

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> createArrowRecordBatchFromBuffer(
      uint32_t partitionId,
      bool reuseBuffers);

  arrow::Result<std::unique_ptr<arrow::ipc::IpcPayload>> createArrowIpcPayload(
      const arrow::RecordBatch& rb,
      bool reuseBuffers);

  template <typename T>
  arrow::Status splitFixedType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
    assert(numPartitions_ == dstAddrs.size());
    void* startAddrs[numPartitions_];
    size_t size = dstAddrs.size();
    for (auto i = 0; i != size; ++i) {
      startAddrs[i] = dstAddrs[i] + partitionBufferIdxBase_[i] * sizeof(T);
    }

    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      auto dstPidBase = reinterpret_cast<T*>(startAddrs[pid]);
      auto pos = partition2RowOffset_[pid];
      auto end = partition2RowOffset_[pid + 1];
      for (; pos < end; ++pos) {
        auto rowId = rowOffset2RowId_[pos];
        *dstPidBase++ = reinterpret_cast<const T*>(srcAddr)[rowId]; // copy
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status splitBinaryType(
      uint32_t binaryIdx,
      const facebook::velox::FlatVector<facebook::velox::StringView>& src,
      std::vector<BinaryBuf>& dst);

  arrow::Status evictPartitionsOnDemand(int64_t* size);

  std::shared_ptr<arrow::RecordBatch> makeRecordBatch(
      uint32_t numRows,
      const std::vector<std::shared_ptr<arrow::Buffer>>& buffers);

  std::shared_ptr<arrow::Buffer> generateComplexTypeBuffers(facebook::velox::RowVectorPtr vector);

  arrow::Result<int64_t> shrinkPartitionBuffers();

  arrow::Status resetPartitionBuffer(uint32_t partitionId);

  arrow::Status shrinkPartitionBuffer(uint32_t partitionId);

  arrow::Status resizePartitionBuffer(uint32_t partitionId, int64_t newSize);

  uint64_t calculateValueBufferSizeForBinaryArray(uint32_t binaryIdx, int64_t newSize);

  void calculateSimpleColumnBytes();

  void stat() const;

  bool shrinkBeforeSpill() const;

  bool shrinkAfterSpill() const;

  arrow::Result<uint32_t> sizeAfterShrink(uint32_t partitionId) const;

 protected:
  // Memory Pool used to track memory allocation of Arrow IPC payloads.
  // The actual allocation is delegated to options_.memory_pool.
  std::shared_ptr<ShuffleMemoryPool> payloadPool_;

  std::shared_ptr<PartitionWriter> partitionWriter_;

  SplitState splitState_{kInit};

  bool supportAvx512_ = false;

  // store arrow column types
  std::vector<std::shared_ptr<arrow::DataType>> arrowColumnTypes_;

  // store velox column types
  std::vector<std::shared_ptr<const facebook::velox::Type>> veloxColumnTypes_;

  // Row ID -> Partition ID
  // subscript: Row ID
  // value: Partition ID
  std::vector<uint16_t> row2Partition_;

  // Partition ID -> Row Count
  // subscript: Partition ID
  // value: how many rows does this partition have
  std::vector<uint32_t> partition2RowCount_;

  // Partition ID -> Buffer Size(unit is row)
  std::vector<uint32_t> partition2BufferSize_;

  // Partition ID -> Row offset, elements num: Partition num + 1
  // subscript: Partition ID
  // value: the row offset of this Partition
  std::vector<uint32_t> partition2RowOffset_;

  // Row offset -> Row ID, elements num: Row Num
  // subscript: Row offset
  // value: Row ID
  std::vector<uint32_t> rowOffset2RowId_;

  uint32_t fixedWidthColumnCount_ = 0;

  //  binary columns
  std::vector<uint32_t> binaryColumnIndices_;

  // fixed-width and binary columns
  std::vector<uint32_t> simpleColumnIndices_;

  // struct、map、list columns
  std::vector<uint32_t> complexColumnIndices_;

  // partid, value is reducer batch's offset, output rb rownum < 64k
  std::vector<uint32_t> partitionBufferIdxBase_;

  std::vector<std::vector<uint8_t*>> partitionValidityAddrs_;
  std::vector<std::vector<uint8_t*>> partitionFixedWidthValueAddrs_;

  uint64_t totalInputNumRows_ = 0;
  std::vector<uint64_t> binaryArrayTotalSizeBytes_;

  // used for calculating bufferSize, calculate once.
  uint32_t simpleColumnBytes_ = 0;

  std::vector<std::vector<BinaryBuf>> partitionBinaryAddrs_;

  // Input column has null, in the order of fixed-width columns + binary columns.
  std::vector<bool> inputHasNull_;

  // pid
  std::vector<std::unique_ptr<facebook::velox::VectorSerializer>> complexTypeData_;
  std::vector<std::shared_ptr<arrow::ResizableBuffer>> complexTypeFlushBuffer_;
  std::shared_ptr<const facebook::velox::RowType> complexWriteType_;

  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
  std::vector<std::unique_ptr<facebook::velox::StreamArena>> arenas_;

  facebook::velox::serializer::presto::PrestoVectorSerde serde_;

  // stat
  enum CpuWallTimingType {
    CpuWallTimingBegin = 0,
    CpuWallTimingCompute = CpuWallTimingBegin,
    CpuWallTimingBuildPartition,
    CpuWallTimingEvictPartition,
    CpuWallTimingHasNull,
    CpuWallTimingCalculateBufferSize,
    CpuWallTimingAllocateBuffer,
    CpuWallTimingCreateRbFromBuffer,
    CpuWallTimingMakeRB,
    CpuWallTimingCacheRB,
    CpuWallTimingFlattenRV,
    CpuWallTimingSplitRV,
    CpuWallTimingIteratePartitions,
    CpuWallTimingStop,
    CpuWallTimingEnd,
    CpuWallTimingNum = CpuWallTimingEnd - CpuWallTimingBegin
  };

  static std::string CpuWallTimingName(CpuWallTimingType type) {
    switch (type) {
      case CpuWallTimingCompute:
        return "CpuWallTimingCompute";
      case CpuWallTimingBuildPartition:
        return "CpuWallTimingBuildPartition";
      case CpuWallTimingEvictPartition:
        return "CpuWallTimingEvictPartition";
      case CpuWallTimingHasNull:
        return "CpuWallTimingHasNull";
      case CpuWallTimingCalculateBufferSize:
        return "CpuWallTimingCalculateBufferSize";
      case CpuWallTimingAllocateBuffer:
        return "CpuWallTimingAllocateBuffer";
      case CpuWallTimingCreateRbFromBuffer:
        return "CpuWallTimingCreateRbFromBuffer";
      case CpuWallTimingMakeRB:
        return "CpuWallTimingMakeRB";
      case CpuWallTimingCacheRB:
        return "CpuWallTimingCacheRB";
      case CpuWallTimingFlattenRV:
        return "CpuWallTimingFlattenRV";
      case CpuWallTimingSplitRV:
        return "CpuWallTimingSplitRV";
      case CpuWallTimingIteratePartitions:
        return "CpuWallTimingIteratePartitions";
      case CpuWallTimingStop:
        return "CpuWallTimingStop";
      default:
        return "CpuWallTimingUnknown";
    }
  }

  facebook::velox::CpuWallTiming cpuWallTimingList_[CpuWallTimingNum];
}; // class VeloxShuffleWriter

} // namespace gluten
