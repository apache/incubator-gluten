#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

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

#include "operators/shuffle/type.h"
#include "operators/shuffle/utils.h"

namespace gluten {

class VeloxSplitter {
  enum { VALIDITY_BUFFER_INDEX = 0, OFFSET_BUFFER_INDEX = 1, VALUE_BUFFER_INEDX = 2 };

 public:
  struct BinaryBuff {
    BinaryBuff(uint8_t* value, uint8_t* offset, uint64_t value_capacity, uint64_t value_offset)
        : value_ptr(value), offset_ptr(offset), value_capacity(value_capacity), value_offset(value_offset) {}

    BinaryBuff(uint8_t* value, uint8_t* offset, uint64_t value_capacity)
        : BinaryBuff(value, offset, value_capacity, 0) {}

    BinaryBuff() : BinaryBuff(nullptr, nullptr, 0, 0) {}

    uint8_t* value_ptr;
    uint8_t* offset_ptr;
    uint64_t value_capacity;
    uint64_t value_offset;
  };

  template <typename SPLITTER>
  static std::shared_ptr<SPLITTER> Create(uint32_t num_partitions, const SplitOptions& options) {
    return std::make_shared<SPLITTER>(num_partitions, options);
  }

  static arrow::Result<std::shared_ptr<VeloxSplitter>>
  Make(const std::string& name, uint32_t num_partitions, SplitOptions options = SplitOptions::Defaults());

  virtual const std::shared_ptr<arrow::Schema>& input_schema() const {
    return schema_;
  }

  virtual arrow::Status Split(const facebook::velox::RowVector& rv);

  virtual arrow::Status Stop();

  arrow::Status SpillFixedSize(int64_t size, int64_t* actual);

  int64_t TotalBytesWritten() const {
    return total_bytes_written_;
  }

  int64_t TotalBytesSpilled() const {
    return total_bytes_spilled_;
  }

  int64_t TotalWriteTime() const {
    return total_write_time_;
  }

  int64_t TotalSpillTime() const {
    return total_spill_time_;
  }

  int64_t TotalCompressTime() const {
    return total_compress_time_;
  }

  const std::vector<int64_t>& PartitionLengths() const {
    return partition_lengths_;
  }

  const std::vector<int64_t>& RawPartitionLengths() const {
    return raw_partition_lengths_;
  }

  int64_t RawPartitionBytes() const {
    return std::accumulate(raw_partition_lengths_.begin(), raw_partition_lengths_.end(), 0LL);
  }

  // for testing
  const std::string& DataFile() const {
    return options_.data_file;
  }

  arrow::Status SetCompressType(arrow::Compression::type compressed_type);

 protected:
  VeloxSplitter(uint32_t num_partitions, const SplitOptions& options)
      : num_partitions_(num_partitions), options_(options) {}

  virtual arrow::Status Init();

  arrow::Status InitIpcWriteOptions();

  arrow::Status InitPartitions(const facebook::velox::RowVector& rv);

  virtual arrow::Status InitColumnTypes(const facebook::velox::RowVector& rv);

  virtual arrow::Status Partition(const facebook::velox::RowVector& rv) = 0;

  virtual arrow::Status TransferSchema(
      const facebook::velox::RowVector& rv,
      std::shared_ptr<arrow::Schema>& input_schema);

  facebook::velox::RowVector GetStrippedRowVector(const facebook::velox::RowVector& rv) const;

  arrow::Status SplitRowVector(const facebook::velox::RowVector& rv);

  arrow::Status InitFromRowVector(const facebook::velox::RowVector& rv);

  arrow::Status CreatePartition2Row(uint32_t row_num);

  arrow::Status UpdateInputHasNull(const facebook::velox::RowVector& rv);

  arrow::Status DoSplit(const facebook::velox::RowVector& rv);

  std::string NextSpilledFileDir();

  arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> GetSchemaPayload();

  uint32_t CalculatePartitionBufferSize(const facebook::velox::RowVector& rv);

  arrow::Status AllocatePartitionBuffers(uint32_t partition_id, uint32_t new_size);

  arrow::Status AllocateBufferFromPool(std::shared_ptr<arrow::Buffer>& buffer, uint32_t size);

  arrow::Status AllocateNew(uint32_t partition_id, uint32_t new_size);

  arrow::Status CreateRecordBatchFromBuffer(uint32_t partition_id, bool reset_buffers);

  arrow::Status CacheRecordBatch(uint32_t partition_id, const arrow::RecordBatch& rb);

  arrow::Status SplitFixedWidthValueBuffer(const facebook::velox::RowVector& rv);

  arrow::Status SplitBoolType(const uint8_t* src_addr, const std::vector<uint8_t*>& dst_addrs);

  arrow::Status SplitValidityBuffer(const facebook::velox::RowVector& rv);

  arrow::Status SplitBinaryArray(const facebook::velox::RowVector& rv);

  template <typename T>
  arrow::Status SplitFixedType(const uint8_t* src_addr, const std::vector<uint8_t*>& dst_addrs) {
    std::transform(
        dst_addrs.begin(),
        dst_addrs.end(),
        partition_buffer_idx_base_.begin(),
        partition_buffer_idx_offset_.begin(),
        [](uint8_t* x, uint32_t y) { return x + y * sizeof(T); });

    for (uint32_t pid = 0; pid < num_partitions_; ++pid) {
      auto dst_pid_base = reinterpret_cast<T*>(partition_buffer_idx_offset_[pid]);
      auto pos = partition_2_row_offset_[pid];
      auto end = partition_2_row_offset_[pid + 1];
      for (; pos < end; ++pos) {
        auto row_id = row_offset_2_row_id_[pos];
        *dst_pid_base++ = reinterpret_cast<const T*>(src_addr)[row_id]; // copy
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status SplitBinaryType(
      uint32_t binary_idx,
      const facebook::velox::FlatVector<facebook::velox::StringView>& src,
      std::vector<BinaryBuff>& dst);

  arrow::Status SplitListArray(const facebook::velox::RowVector& rv);

  arrow::Result<int32_t> SpillLargestPartition(int64_t* size);

  arrow::Status SpillPartition(uint32_t partition_id);

 protected:
  bool support_avx512_ = false;

  uint32_t num_partitions_ = 0;

  // options
  SplitOptions options_;

  // the first column may be stripped if hash split
  std::shared_ptr<arrow::Schema> schema_;

  // store arrow column types
  std::vector<std::shared_ptr<arrow::DataType>> arrow_column_types_; // column_type_id_

  // store velox column types
  std::vector<std::shared_ptr<const facebook::velox::Type>> velox_column_types_;

  // write options for tiny batches
  arrow::ipc::IpcWriteOptions tiny_batch_write_options_;

  // Row ID -> Partition ID
  // subscript: Row ID
  // value: Partition ID
  // TODO: rethink, is uint16_t better?
  std::vector<uint32_t> row_2_partition_; // note: partition_id_

  // Partition ID -> Row Count
  // subscript: Partition ID
  // value: how many rows does this partition have
  std::vector<uint32_t> partition_2_row_count_; // note: partition_id_cnt_

  // Partition ID -> Buffer Size(unit is row)
  std::vector<uint32_t> partition_2_buffer_size_;

  // Partition ID -> Row offset
  // elements num: Partition num + 1
  // subscript: Partition ID
  // value: the row offset of this Partition
  std::vector<uint32_t> partition_2_row_offset_; // note: reducer_offset_offset_

  // Row offset -> Row ID
  // elements num: Row Num
  // subscript: Row offset
  // value: Row ID
  std::vector<uint32_t> row_offset_2_row_id_; // note: reducer_offsets_

  // Partition ID -> length
  std::vector<int64_t> partition_lengths_;

  // Partition ID -> raw length
  std::vector<int64_t> raw_partition_lengths_;

  uint32_t fixed_width_column_count_ = 0;

  std::vector<uint32_t> binary_column_indices_;

  // fixed columns + binary columns
  std::vector<uint32_t> simple_column_indices_;

  // struct、map、list、large list columns
  std::vector<uint32_t> complex_column_indices_;

  // partid, value is reducer batch's offset, output rb rownum < 64k
  std::vector<uint32_t> partition_buffer_idx_base_;

  // temp array to hold the destination pointer
  std::vector<uint8_t*> partition_buffer_idx_offset_;

  typedef uint32_t row_offset_type;

  class PartitionWriter;

  std::vector<std::shared_ptr<PartitionWriter>> partition_writer_;

  std::vector<std::vector<uint8_t*>> partition_validity_addrs_;
  std::vector<std::vector<uint8_t*>> partition_fixed_width_value_addrs_;

  std::vector<std::vector<std::vector<std::shared_ptr<arrow::Buffer>>>> partition_buffers_;
  std::vector<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> partition_list_builders_;

  // slice the buffer for each reducer's column, in this way we can combine into large page
  std::shared_ptr<arrow::ResizableBuffer> combine_buffer_;

  // partid
  std::vector<std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>> partition_cached_recordbatch_;

  // partid
  std::vector<int64_t> partition_cached_recordbatch_size_; // in bytes

  std::vector<uint64_t> binary_array_empirical_size_;

  std::vector<std::vector<BinaryBuff>> partition_binary_addrs_;

  std::vector<bool> input_has_null_;

  int64_t total_bytes_written_ = 0;
  int64_t total_bytes_spilled_ = 0;
  int64_t total_write_time_ = 0;
  int64_t total_spill_time_ = 0;
  int64_t total_compress_time_ = 0;

  int32_t dir_selection_ = 0;
  std::vector<int32_t> sub_dir_selection_;

  std::vector<std::string> configured_dirs_;

  std::shared_ptr<arrow::io::OutputStream> data_file_os_;

  // shared by all partition writers
  std::shared_ptr<arrow::ipc::IpcPayload> schema_payload_;
}; // class VeloxSplitter

class VeloxRoundRobinSplitter final : public VeloxSplitter {
 public:
  VeloxRoundRobinSplitter(uint32_t num_partitions, const SplitOptions& options)
      : VeloxSplitter(num_partitions, std::move(options)) {}

  arrow::Status Partition(const facebook::velox::RowVector& rv) override;

  uint32_t pid_selection_ = 0;
}; // class VeloxRoundRobinSplitter

class VeloxSinglePartSplitter final : public VeloxSplitter {
 public:
  VeloxSinglePartSplitter(uint32_t num_partitions, const SplitOptions& options)
      : VeloxSplitter(num_partitions, options) {}

  arrow::Status Init() override;

  arrow::Status Partition(const facebook::velox::RowVector& rv) override;

  arrow::Status Split(const facebook::velox::RowVector& rv) override;

  arrow::Status Stop() override;
}; // class VeloxSinglePartSplitter

class VeloxHashSplitter final : public VeloxSplitter {
  // original schema
  std::shared_ptr<arrow::Schema> input_schema_;

 public:
  VeloxHashSplitter(uint32_t num_partitions, const SplitOptions& options) : VeloxSplitter(num_partitions, options) {}

  arrow::Status Init() override;

  arrow::Status InitColumnTypes(const facebook::velox::RowVector& rv) override;

  arrow::Status Split(const facebook::velox::RowVector& rv) override;

  arrow::Status Partition(const facebook::velox::RowVector& rv) override;

  const std::shared_ptr<arrow::Schema>& input_schema() const override {
    return input_schema_;
  }
}; // class VeloxHashSplitter

class VeloxFallbackRangeSplitter final : public VeloxSplitter {
  std::shared_ptr<arrow::Schema> input_schema_;

 public:
  VeloxFallbackRangeSplitter(uint32_t num_partitions, const SplitOptions& options)
      : VeloxSplitter(num_partitions, options) {}

  arrow::Status Init() override;

  arrow::Status InitColumnTypes(const facebook::velox::RowVector& rv) override;

  arrow::Status Split(const facebook::velox::RowVector& rv) override;

  const std::shared_ptr<arrow::Schema>& input_schema() const override {
    return input_schema_;
  }

  arrow::Status Partition(const facebook::velox::RowVector& rv) override;
}; // class VeloxFallbackRangeSplitter

} // namespace gluten
