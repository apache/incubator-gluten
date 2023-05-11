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

#include "shuffle/PartitionWriterCreator.h"
#include "shuffle/Partitioner.h"
#include "shuffle/ShuffleWriter.h"
#include "shuffle/utils.h"

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

class VeloxShuffleWriter final : public ShuffleWriter {
  enum { kValidityBufferIndex = 0, kOffsetBufferIndex = 1, kValueBufferInedx = 2 };

 public:
  struct BinaryBuff {
    BinaryBuff(uint8_t* value, uint8_t* offset, uint64_t valueCapacity, uint64_t valueOffset)
        : value_ptr(value), offset_ptr(offset), value_capacity(valueCapacity), value_offset(valueOffset) {}

    BinaryBuff(uint8_t* value, uint8_t* offset, uint64_t valueCapacity) : BinaryBuff(value, offset, valueCapacity, 0) {}

    BinaryBuff() : BinaryBuff(nullptr, nullptr, 0, 0) {}

    uint8_t* value_ptr;
    uint8_t* offset_ptr;
    uint64_t value_capacity;
    uint64_t value_offset;
  };

  static arrow::Result<std::shared_ptr<VeloxShuffleWriter>> create(
      uint32_t numPartitions,
      std::shared_ptr<PartitionWriterCreator> partitionWriterCreator,
      ShuffleWriterOptions options);

  arrow::Status split(ColumnarBatch* cb) override;

  arrow::Status stop() override;

  arrow::Status evictFixedSize(int64_t size, int64_t* actual) override;

  arrow::Status createRecordBatchFromBuffer(uint32_t partitionId, bool resetBuffers) override;

  int64_t rawPartitionBytes() const {
    return std::accumulate(rawPartitionLengths_.begin(), rawPartitionLengths_.end(), 0LL);
  }

  // for testing
  const std::string& dataFile() const {
    return options_.data_file;
  }

  arrow::Status setCompressType(arrow::Compression::type compressedType);

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

 protected:
  VeloxShuffleWriter(
      uint32_t numPartitions,
      std::shared_ptr<PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options)
      : ShuffleWriter(numPartitions, partitionWriterCreator, options) {}

  arrow::Status init();

  arrow::Status initIpcWriteOptions();

  arrow::Status initPartitions(const facebook::velox::RowVector& rv);

  arrow::Status initColumnTypes(const facebook::velox::RowVector& rv);

  arrow::Status veloxType2ArrowSchema(const facebook::velox::TypePtr& type);

  facebook::velox::RowVector getStrippedRowVector(const facebook::velox::RowVector& rv) const;

  arrow::Status splitRowVector(const facebook::velox::RowVector& rv);

  arrow::Status initFromRowVector(const facebook::velox::RowVector& rv);

  arrow::Status createPartition2Row(uint32_t rowNum);

  arrow::Status updateInputHasNull(const facebook::velox::RowVector& rv);

  arrow::Status doSplit(const facebook::velox::RowVector& rv);

  uint32_t calculatePartitionBufferSize(const facebook::velox::RowVector& rv);

  arrow::Status allocatePartitionBuffers(uint32_t partitionId, uint32_t newSize);

  arrow::Status allocateBufferFromPool(std::shared_ptr<arrow::Buffer>& buffer, uint32_t size);

  arrow::Status allocateNew(uint32_t partitionId, uint32_t newSize);

  arrow::Status cacheRecordBatch(uint32_t partitionId, const arrow::RecordBatch& rb);

  arrow::Status splitFixedWidthValueBuffer(const facebook::velox::RowVector& rv);

  arrow::Status splitBoolType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs);

  arrow::Status splitValidityBuffer(const facebook::velox::RowVector& rv);

  arrow::Status splitBinaryArray(const facebook::velox::RowVector& rv);

  template <typename T>
  arrow::Status splitFixedType(const uint8_t* srcAddr, const std::vector<uint8_t*>& dstAddrs) {
    std::transform(
        dstAddrs.begin(),
        dstAddrs.end(),
        partitionBufferIdxBase_.begin(),
        partitionBufferIdxOffset_.begin(),
        [](uint8_t* x, uint32_t y) { return x + y * sizeof(T); });

    for (uint32_t pid = 0; pid < numPartitions_; ++pid) {
      auto dstPidBase = reinterpret_cast<T*>(partitionBufferIdxOffset_[pid]);
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
      std::vector<BinaryBuff>& dst);

  arrow::Status splitListArray(const facebook::velox::RowVector& rv);

  arrow::Result<int32_t> evictLargestPartition(int64_t* size);

  arrow::Status evictPartition(uint32_t partitionId);

  arrow::Result<const int32_t*> getFirstColumn(const facebook::velox::RowVector& rv);

 protected:
  bool supportAvx512_ = false;

  // store arrow column types
  std::vector<std::shared_ptr<arrow::DataType>> arrowColumnTypes_; // column_type_id_

  // store velox column types
  std::vector<std::shared_ptr<const facebook::velox::Type>> veloxColumnTypes_;

  // write options for tiny batches
  arrow::ipc::IpcWriteOptions tinyBatchWriteOptions_;

  // Row ID -> Partition ID
  // subscript: Row ID
  // value: Partition ID
  // TODO: rethink, is uint16_t better?
  std::vector<uint16_t> row2Partition_; // note: partition_id_

  // Partition ID -> Row Count
  // subscript: Partition ID
  // value: how many rows does this partition have
  std::vector<uint32_t> partition2RowCount_; // note: partition_id_cnt_

  // Partition ID -> Buffer Size(unit is row)
  std::vector<uint32_t> partition2BufferSize_;

  // Partition ID -> Row offset
  // elements num: Partition num + 1
  // subscript: Partition ID
  // value: the row offset of this Partition
  std::vector<uint32_t> partition2RowOffset_; // note: reducerOffsetOffset_

  // Row offset -> Row ID
  // elements num: Row Num
  // subscript: Row offset
  // value: Row ID
  std::vector<uint32_t> rowOffset2RowId_; // note: reducerOffsets_

  uint32_t fixedWidthColumnCount_ = 0;

  std::vector<uint32_t> binaryColumnIndices_;

  // fixed columns + binary columns
  std::vector<uint32_t> simpleColumnIndices_;

  // struct、map、list、large list columns
  std::vector<uint32_t> complexColumnIndices_;

  // partid, value is reducer batch's offset, output rb rownum < 64k
  std::vector<uint32_t> partitionBufferIdxBase_;

  // temp array to hold the destination pointer
  std::vector<uint8_t*> partitionBufferIdxOffset_;

  typedef uint32_t row_offset_type;

  std::vector<std::vector<uint8_t*>> partitionValidityAddrs_;
  std::vector<std::vector<uint8_t*>> partitionFixedWidthValueAddrs_;

  std::vector<std::vector<std::shared_ptr<arrow::ArrayBuilder>>> partitionListBuilders_;

  std::vector<uint64_t> binaryArrayEmpiricalSize_;

  std::vector<std::vector<BinaryBuff>> partitionBinaryAddrs_;

  std::vector<bool> inputHasNull_;
}; // class VeloxShuffleWriter

} // namespace gluten
