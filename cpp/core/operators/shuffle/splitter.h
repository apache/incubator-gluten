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

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <arrow/type_traits.h>

#include <random>

#include "memory/ColumnarBatch.h"
#include "operators/shuffle/SplitterBase.h"
#include "operators/shuffle/type.h"
#include "operators/shuffle/utils.h"
#include "substrait/algebra.pb.h"

namespace gluten {

// for stat create splitter
enum SPLITTER_TYPE {
  SPLITTER_HASH,
  SPLITTER_ROUND_ROBIN,
  SPLITTER_RANGE,
  SPLITTER_SINGLE,

  VELOX_SPLITTER_HASH,
  VELOX_SPLITTER_ROUND_ROBIN,
  VELOX_SPLITTER_RANGE,
  VELOX_SPLITTER_SINGLE,

  SPLITTER_TOTAL
};

void StatCreateSplitter(SPLITTER_TYPE type);

class Splitter : public SplitterBase {
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
  static arrow::Result<std::shared_ptr<Splitter>>
  Make(const std::string& short_name, int num_partitions, SplitOptions options = SplitOptions::Defaults());

  typedef uint32_t row_offset_type;

  /**
   * Split input record batch into partition buffers according to the computed
   * partition id. The largest partition buffer will be spilled if memory
   * allocation failure occurs.
   */
  virtual arrow::Status Split(ColumnarBatch* cb);

  /**
   * For each partition, merge spilled file into shuffle data file and write any
   * cached record batch to shuffle data file. Close all resources and collect
   * metrics.
   */
  virtual arrow::Status Stop();

  /**
   * Spill specified partition
   */
  arrow::Status SpillPartition(int32_t partition_id);

  arrow::Status SetCompressType(arrow::Compression::type compressed_type);

  /**
   * Spill for fixed size of partition data
   */
  arrow::Status SpillFixedSize(int64_t size, int64_t* actual);

  /**
   * Spill the largest partition buffer
   * @return partition id. If no partition to spill, return -1
   */
  arrow::Result<int32_t> SpillLargestPartition(int64_t* size);

  int64_t RawPartitionBytes() const {
    return std::accumulate(raw_partition_lengths_.begin(), raw_partition_lengths_.end(), 0LL);
  }

  // for testing
  const std::string& DataFile() const {
    return options_.data_file;
  }

 protected:
  Splitter(int32_t num_partitions, SplitOptions options) : SplitterBase(num_partitions, options) {}

  virtual arrow::Status Init();

  arrow::Status InitColumnType();

  virtual arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) = 0;

  arrow::Status DoSplit(const arrow::RecordBatch& rb);

  row_offset_type CalculateSplitBatchSize(const arrow::RecordBatch& rb);

  template <typename T>
  arrow::Status SplitFixedType(const uint8_t* src_addr, const std::vector<uint8_t*>& dst_addrs);

  arrow::Status SplitFixedWidthValueBuffer(const arrow::RecordBatch& rb);

#if defined(COLUMNAR_PLUGIN_USE_AVX512)
  arrow::Status SplitFixedWidthValueBufferAVX(const arrow::RecordBatch& rb);
#endif
  arrow::Status SplitBoolType(const uint8_t* src_addr, const std::vector<uint8_t*>& dst_addrs);

  arrow::Status SplitValidityBuffer(const arrow::RecordBatch& rb);

  arrow::Status SplitBinaryArray(const arrow::RecordBatch& rb);

  template <typename T>
  arrow::Status SplitBinaryType(
      const uint8_t* src_addr,
      const T* src_offset_addr,
      std::vector<BinaryBuff>& dst_addrs,
      const int binary_idx);

  arrow::Status SplitListArray(const arrow::RecordBatch& rb);

  arrow::Status AllocateBufferFromPool(std::shared_ptr<arrow::Buffer>& buffer, uint32_t size);

  template <
      typename T,
      typename ArrayType = typename arrow::TypeTraits<T>::ArrayType,
      typename BuilderType = typename arrow::TypeTraits<T>::BuilderType>
  arrow::Status AppendBinary(
      const std::shared_ptr<ArrayType>& src_arr,
      const std::vector<std::shared_ptr<BuilderType>>& dst_builders,
      int64_t num_rows);

  arrow::Status AppendList(
      const std::shared_ptr<arrow::Array>& src_arr,
      const std::vector<std::shared_ptr<arrow::ArrayBuilder>>& dst_builders,
      int64_t num_rows);

  // Cache the partition buffer/builder as compressed record batch. If reset
  // buffers, the partition buffer/builder will be set to nullptr. Two cases for
  // caching the partition buffers as record batch:
  // 1. Split record batch. It first calculate whether the partition
  // buffer can hold all data according to partition id. If not, call this
  // method and allocate new buffers. Spill will happen if OOM.
  // 2. Stop the splitter. The record batch will be written to disk immediately.
  arrow::Status CreateRecordBatchFromBuffer(int32_t partition_id, bool reset_buffers);

  arrow::Status CacheRecordBatch(int32_t partition_id, const arrow::RecordBatch& batch);

  // Allocate new partition buffer/builder.
  // If successful, will point partition buffer/builder to new ones, otherwise
  // will spill the largest partition and retry
  arrow::Status AllocateNew(int32_t partition_id, int32_t new_size);

  // Allocate new partition buffer/builder. May return OOM status.
  arrow::Status AllocatePartitionBuffers(int32_t partition_id, int32_t new_size);

  std::string NextSpilledFileDir();

  arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> GetSchemaPayload();

  class PartitionWriter;

  // Check whether support AVX512 instructions
  bool support_avx512_;
  // partid
  std::vector<int32_t> partition_buffer_size_;
  // partid, value is reducer batch's offset, output rb rownum < 64k
  std::vector<row_offset_type> partition_buffer_idx_base_;
  // partid
  // temp array to hold the destination pointer
  std::vector<uint8_t*> partition_buffer_idx_offset_;
  // partid
  std::vector<std::shared_ptr<PartitionWriter>> partition_writer_;
  // col partid
  std::vector<std::vector<uint8_t*>> partition_validity_addrs_;

  // col partid
  std::vector<std::vector<uint8_t*>> partition_fixed_width_value_addrs_;
  // col partid, 24 bytes each
  std::vector<std::vector<BinaryBuff>> partition_binary_addrs_;

  // col partid
  std::vector<std::vector<std::vector<std::shared_ptr<arrow::Buffer>>>> partition_buffers_;
  std::vector<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> partition_list_builders_;
  // col partid

  // slice the buffer for each reducer's column, in this way we can combine into
  // large page
  std::shared_ptr<arrow::ResizableBuffer> combine_buffer_;

  // partid
  std::vector<std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>> partition_cached_recordbatch_;
  // partid
  std::vector<int64_t> partition_cached_recordbatch_size_; // in bytes

  // col fixed + binary
  std::vector<int32_t> array_idx_;
  uint16_t fixed_width_col_cnt_;

  // col
  std::vector<int32_t> list_array_idx_;
  // col

  bool empirical_size_calculated_ = false;
  // col
  std::vector<uint64_t> binary_array_empirical_size_;

  // col
  std::vector<bool> input_has_null_;

  // updated for each input record batch
  // col; value is partition number, part_num < 64k
  std::vector<uint16_t> partition_id_;
  // [num_rows] ; value is offset in input record batch; input rb rownum < 64k
  std::vector<row_offset_type> reducer_offsets_;
  // [num_partitions]; value is offset of row in record batch; input rb rownum <
  // 64k
  std::vector<row_offset_type> reducer_offset_offset_;
  // col  ; value is reducer's row number for each input record batch; output rb
  // rownum < 64k
  std::vector<row_offset_type> partition_id_cnt_;

  std::shared_ptr<arrow::Schema> schema_;

  // write options for tiny batches
  arrow::ipc::IpcWriteOptions tiny_bach_write_options_;

  std::vector<std::shared_ptr<arrow::DataType>> column_type_id_;

  // configured local dirs for spilled file
  int32_t dir_selection_ = 0;
  std::vector<int32_t> sub_dir_selection_;
  std::vector<std::string> configured_dirs_;

  std::shared_ptr<arrow::io::OutputStream> data_file_os_;

  // shared by all partition writers
  std::shared_ptr<arrow::ipc::IpcPayload> schema_payload_;
};

class RoundRobinSplitter final : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<RoundRobinSplitter>> Create(int32_t num_partitions, SplitOptions options);

 private:
  RoundRobinSplitter(int32_t num_partitions, SplitOptions options) : Splitter(num_partitions, std::move(options)) {}

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;

  int32_t pid_selection_ = 0;
};

class SinglePartSplitter final : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<SinglePartSplitter>> Create(int32_t num_partitions, SplitOptions options);

 private:
  SinglePartSplitter(int32_t num_partitions, SplitOptions options) : Splitter(num_partitions, std::move(options)) {}

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;

  arrow::Status Split(ColumnarBatch* cb) override;

  arrow::Status Init() override;

  arrow::Status Stop() override;
};

class HashSplitter final : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<HashSplitter>> Create(int32_t num_partitions, SplitOptions options);

 private:
  HashSplitter(int32_t num_partitions, SplitOptions options) : Splitter(num_partitions, std::move(options)) {}

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;

  arrow::Status Split(ColumnarBatch* cb) override;
};

class FallbackRangeSplitter final : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<FallbackRangeSplitter>> Create(int32_t num_partitions, SplitOptions options);

  arrow::Status Split(ColumnarBatch* cb) override;

 private:
  FallbackRangeSplitter(int32_t num_partitions, SplitOptions options) : Splitter(num_partitions, std::move(options)) {}

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;
};

} // namespace gluten
