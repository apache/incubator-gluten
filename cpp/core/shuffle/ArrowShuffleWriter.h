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

#include <arrow/array/builder_base.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <arrow/type_traits.h>

#include <random>

#include "jni/JniCommon.h"
#include "shuffle/PartitionWriterCreator.h"
#include "shuffle/Partitioner.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/utils.h"
#include "substrait/algebra.pb.h"

namespace gluten {
class ArrowShuffleWriter final : public ShuffleWriter {
 protected:
  struct BinaryBuff {
    BinaryBuff(uint8_t* v, uint8_t* o, uint64_t c, uint64_t f)
        : valueptr(v), offsetptr(o), value_capacity(c), value_offset(f) {}
    BinaryBuff(uint8_t* v, uint8_t* o, uint64_t c) : valueptr(v), offsetptr(o), value_capacity(c), value_offset(0) {}
    BinaryBuff() : valueptr(nullptr), offsetptr(nullptr), value_capacity(0), value_offset(0) {}

    uint8_t* valueptr;
    uint8_t* offsetptr;
    uint64_t value_capacity;
    uint64_t value_offset;
  };

 public:
  static arrow::Result<std::shared_ptr<ArrowShuffleWriter>> create(
      uint32_t numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      ShuffleWriterOptions options);

  typedef uint32_t row_offset_type;

  /**
   * Split input record batch into partition buffers according to the computed
   * partition id. The largest partition buffer will be evicted if memory
   * allocation failure occurs.
   */
  arrow::Status split(ColumnarBatch* cb) override;

  /**
   * For each partition, merge evicted file into shuffle data file and write any
   * cached record batch to shuffle data file. Close all resources and collect
   * metrics.
   */
  arrow::Status stop() override;

  /**
   * Evict specified partition
   */
  arrow::Status evictPartition(int32_t partitionId);

  arrow::Status setCompressType(arrow::Compression::type compressedType);

  /**
   * Evict for fixed size of partition data from memory
   */
  arrow::Status evictFixedSize(int64_t size, int64_t* actual) override;

  arrow::Status createRecordBatchFromBuffer(uint32_t partitionId, bool resetBuffers) override;

  /**
   * Evict the largest partition buffer
   * @return partition id. If no partition to evict, return -1
   */
  arrow::Result<int32_t> evictLargestPartition(int64_t* size);

  int64_t rawPartitionBytes() const {
    return std::accumulate(rawPartitionLengths_.begin(), rawPartitionLengths_.end(), 0LL);
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> createArrowRecordBatchFromBuffer(
      uint32_t partitionId,
      bool resetBuffers) override;

  arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> createArrowIpcPayload(
      const arrow::RecordBatch& rb,
      bool reuseBuffers) override;

  // for testing
  const std::string& dataFile() const {
    return options_.data_file;
  }

 protected:
  ArrowShuffleWriter(
      int32_t numPartitions,
      std::shared_ptr<PartitionWriterCreator> partitionWriterCreator,
      ShuffleWriterOptions options)
      : ShuffleWriter(numPartitions, partitionWriterCreator, options) {}

  arrow::Status init();

  arrow::Status initColumnType();

  arrow::Status doSplit(const arrow::RecordBatch& rb);

  row_offset_type calculateSplitBatchSize(const arrow::RecordBatch& rb);

  template <typename T>
  arrow::Status splitFixedType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs);

  arrow::Status splitFixedWidthValueBuffer(const arrow::RecordBatch& rb);

#if defined(COLUMNAR_PLUGIN_USE_AVX512)
  arrow::Status SplitFixedWidthValueBufferAVX(const arrow::RecordBatch& rb);
#endif
  arrow::Status splitBoolType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs);

  arrow::Status splitValidityBuffer(const arrow::RecordBatch& rb);

  arrow::Status splitBinaryArray(const arrow::RecordBatch& rb);

  template <typename T>
  arrow::Status splitBinaryType(
      const uint8_t* srcAddr,
      const T* srcOffsetAddr,
      std::vector<BinaryBuff>& dstAddrs,
      const int binaryIdx);

  arrow::Status splitListArray(const arrow::RecordBatch& rb);

  arrow::Status allocateBufferFromPool(std::shared_ptr<arrow::Buffer>& buffer, uint32_t size);

  template <
      typename T,
      typename ArrayType = typename arrow::TypeTraits<T>::ArrayType,
      typename BuilderType = typename arrow::TypeTraits<T>::BuilderType>
  arrow::Status appendBinary(
      const std::shared_ptr<ArrayType>& srcArr,
      const std::vector<std::shared_ptr<BuilderType>>& dstBuilders,
      int64_t numRows);

  arrow::Status appendList(
      const std::shared_ptr<arrow::Array>& srcArr,
      const std::vector<std::shared_ptr<arrow::ArrayBuilder>>& dstBuilders,
      int64_t numRows);

  arrow::Result<const int32_t*> getFirstColumn(const arrow::RecordBatch& rb);

  arrow::Status cacheRecordBatch(int32_t partitionId, const arrow::RecordBatch& batch);

  // Allocate new partition buffer/builder.
  // If successful, will point partition buffer/builder to new ones, otherwise
  // will evict the largest partition and retry
  arrow::Status allocateNew(int32_t partitionId, int32_t newSize);

  // Allocate new partition buffer/builder. May return OOM status.
  arrow::Status allocatePartitionBuffers(int32_t partitionId, int32_t newSize);

  // Check whether support AVX512 instructions
  bool supportAvx512_;
  // partid
  std::vector<int32_t> partitionBufferSize_;
  // partid, value is reducer batch's offset, output rb rownum < 64k
  std::vector<row_offset_type> partitionBufferIdxBase_;
  // partid
  // temp array to hold the destination pointer
  std::vector<uint8_t*> partitionBufferIdxOffset_;

  // col partid
  std::vector<std::vector<uint8_t*>> partitionValidityAddrs_;

  // col partid
  std::vector<std::vector<uint8_t*>> partitionFixedWidthValueAddrs_;
  // col partid, 24 bytes each
  std::vector<std::vector<BinaryBuff>> partitionBinaryAddrs_;

  std::vector<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> partitionListBuilders_;

  // col fixed + binary
  std::vector<int32_t> arrayIdx_;
  uint16_t fixedWidthColCnt_;

  // col
  std::vector<int32_t> listArrayIdx_;
  // col

  bool empiricalSizeCalculated_ = false;
  // col
  std::vector<uint64_t> binaryArrayEmpiricalSize_;

  // col
  std::vector<bool> inputHasNull_;

  // updated for each input record batch
  // col; value is partition number, part_num < 64k
  std::vector<uint16_t> partitionId_;
  // [num_rows] ; value is offset in input record batch; input rb rownum < 64k
  std::vector<row_offset_type> reducerOffsets_;
  // [num_partitions]; value is offset of row in record batch; input rb rownum <
  // 64k
  std::vector<row_offset_type> reducerOffsetOffset_;
  // col  ; value is reducer's row number for each input record batch; output rb
  // rownum < 64k
  std::vector<row_offset_type> partitionIdCnt_;

  // write options for tiny batches
  arrow::ipc::IpcWriteOptions tinyBachWriteOptions_;

  std::vector<std::shared_ptr<arrow::DataType>> columnTypeId_;
};

} // namespace gluten
