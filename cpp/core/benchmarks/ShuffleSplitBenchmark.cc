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

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/io_util.h>
#include <benchmark/benchmark.h>
#include <execinfo.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <sched.h>
#include <sys/mman.h>

#include <chrono>

#include "memory/ColumnarBatch.h"
#include "shuffle/ArrowShuffleWriter.h"
#include "shuffle/LocalPartitionWriter.h"
#include "utils/macros.h"

void printTrace(void) {
  char** strings;
  size_t i, size;
  enum Constexpr { kMaxSize = 1024 };
  void* array[kMaxSize];
  size = backtrace(array, kMaxSize);
  strings = backtrace_symbols(array, size);
  for (i = 0; i < size; i++)
    printf("    %s\n", strings[i]);
  puts("");
  free(strings);
}

using arrow::RecordBatchReader;
using arrow::Status;

using gluten::ArrowShuffleWriter;
using gluten::GlutenException;
using gluten::ShuffleWriterOptions;

namespace gluten {

std::shared_ptr<ColumnarBatch> recordBatchToColumnarBatch(std::shared_ptr<arrow::RecordBatch> rb) {
  std::unique_ptr<ArrowSchema> cSchema = std::make_unique<ArrowSchema>();
  std::unique_ptr<ArrowArray> cArray = std::make_unique<ArrowArray>();
  GLUTEN_THROW_NOT_OK(arrow::ExportRecordBatch(*rb, cArray.get(), cSchema.get()));
  return std::make_shared<ArrowCStructColumnarBatch>(std::move(cSchema), std::move(cArray));
}

#define ALIGNMENT 2 * 1024 * 1024

const int kBatchBufferSize = 32768;
const int kSplitBufferSize = 8192;

class MyMemoryPool final : public arrow::MemoryPool {
 public:
  explicit MyMemoryPool() {}

  Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
    RETURN_NOT_OK(pool_->Allocate(size, out));
    stats_.UpdateAllocatedBytes(size);
    // std::cout << "Allocate: size = " << size << " addr = " << std::hex <<
    // (uint64_t)*out << std::dec << std::endl; print_trace();
    return arrow::Status::OK();
  }

  Status Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) override {
    // auto old_ptr = *ptr;
    RETURN_NOT_OK(pool_->Reallocate(oldSize, newSize, ptr));
    stats_.UpdateAllocatedBytes(newSize - oldSize);
    // std::cout << "Reallocate: old_size = " << old_size << " old_ptr = " <<
    // std::hex << (uint64_t)old_ptr << std::dec << " new_size = " << new_size
    // << " addr = " << std::hex << (uint64_t)*ptr << std::dec << std::endl;
    // print_trace();
    return arrow::Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
    pool_->Free(buffer, size);
    stats_.UpdateAllocatedBytes(-size);
    // std::cout << "Free: size = " << size << " addr = " << std::hex <<
    // (uint64_t)buffer
    // << std::dec << std::endl; print_trace();
  }

  int64_t bytes_allocated() const override {
    return stats_.bytes_allocated();
  }

  int64_t max_memory() const override {
    return pool_->max_memory();
  }

  std::string backend_name() const override {
    return pool_->backend_name();
  }

 private:
  arrow::MemoryPool* pool_ = arrow::default_memory_pool();
  arrow::internal::MemoryPoolStats stats_;
};

// #define ENABLELARGEPAGE

class LargePageMemoryPool : public arrow::MemoryPool {
 public:
  explicit LargePageMemoryPool() {}

  ~LargePageMemoryPool() override = default;

  Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
#ifdef ENABLELARGEPAGE
    if (size < 2 * 1024 * 1024) {
      return pool_->Allocate(size, out);
    } else {
      Status st = pool_->AlignAllocate(size, out, ALIGNMENT);
      madvise(*out, size, /*MADV_HUGEPAGE */ 14);
      //std::cout << "Allocate: size = " << size << " addr = "  \
          << std::hex << (uint64_t)*out  << " end = " << std::hex << (uint64_t)(*out+size) << std::dec << std::endl;
      return st;
    }
#else
    return pool_->Allocate(size, out);
#endif
  }

  Status Reallocate(int64_t oldSize, int64_t newSize, int64_t alignment, uint8_t** ptr) override {
    return pool_->Reallocate(oldSize, newSize, ptr);
#ifdef ENABLELARGEPAGE
    if (new_size < 2 * 1024 * 1024) {
      return pool_->Reallocate(old_size, new_size, ptr);
    } else {
      Status st = pool_->AlignReallocate(old_size, new_size, ptr, ALIGNMENT);
      madvise(*ptr, new_size, /*MADV_HUGEPAGE */ 14);
      return st;
    }
#else
    return pool_->Reallocate(oldSize, newSize, ptr);
#endif
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
#ifdef ENABLELARGEPAGE
    if (size < 2 * 1024 * 1024) {
      pool_->Free(buffer, size);
    } else {
      pool_->Free(buffer, size, ALIGNMENT);
    }
#else
    pool_->Free(buffer, size);
#endif
  }

  int64_t bytes_allocated() const override {
    return pool_->bytes_allocated();
  }

  int64_t max_memory() const override {
    return pool_->max_memory();
  }

  std::string backend_name() const override {
    return "LargePageMemoryPool";
  }

 private:
  MemoryPool* pool_ = arrow::default_memory_pool();
};

class BenchmarkShuffleSplit {
 public:
  BenchmarkShuffleSplit(std::string fileName) {
    getRecordBatchReader(fileName);
  }

  void getRecordBatchReader(const std::string& inputFile) {
    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;

    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::string fileName;
    GLUTEN_ASSIGN_OR_THROW(fs, arrow::fs::FileSystemFromUriOrPath(inputFile, &fileName))

    GLUTEN_ASSIGN_OR_THROW(file_, fs->OpenInputFile(fileName));

    properties_.set_batch_size(kBatchBufferSize);
    properties_.set_pre_buffer(false);
    properties_.set_use_threads(false);

    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file_), properties_, &parquetReader));

    GLUTEN_THROW_NOT_OK(parquetReader->GetSchema(&schema_));

    auto numRowgroups = parquetReader->num_row_groups();

    for (int i = 0; i < numRowgroups; ++i) {
      rowGroupIndices_.push_back(i);
    }

    auto numColumns = schema_->num_fields();
    for (int i = 0; i < numColumns; ++i) {
      columnIndices_.push_back(i);
    }
  }

  void operator()(benchmark::State& state) {
    // SetCPU(state.thread_index());
    arrow::Compression::type compressionType = (arrow::Compression::type)state.range(1);

    std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<LargePageMemoryPool>();

    const int numPartitions = state.range(0);

    std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator =
        std::make_shared<LocalPartitionWriterCreator>();

    auto options = ShuffleWriterOptions::defaults();
    options.compression_type = compressionType;
    options.buffer_size = kSplitBufferSize;
    options.buffered_write = true;
    options.offheap_per_task = 128 * 1024 * 1024 * 1024L;
    options.prefer_evict = true;
    options.write_schema = false;
    options.memory_pool = pool;
    options.partitioning_name = "rr";

    std::shared_ptr<ArrowShuffleWriter> shuffleWriter;
    int64_t elapseRead = 0;
    int64_t numBatches = 0;
    int64_t numRows = 0;
    int64_t splitTime = 0;
    auto startTime = std::chrono::steady_clock::now();

    doSplit(
        shuffleWriter,
        elapseRead,
        numBatches,
        numRows,
        splitTime,
        numPartitions,
        partitionWriterCreator,
        options,
        state);
    auto endTime = std::chrono::steady_clock::now();
    auto totalTime = (endTime - startTime).count();

    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    GLUTEN_THROW_NOT_OK(fs->DeleteFile(shuffleWriter->dataFile()));

    state.SetBytesProcessed(int64_t(shuffleWriter->rawPartitionBytes()));

    state.counters["rowgroups"] =
        benchmark::Counter(rowGroupIndices_.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["columns"] =
        benchmark::Counter(columnIndices_.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batches"] =
        benchmark::Counter(numBatches, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["num_rows"] =
        benchmark::Counter(numRows, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["num_partitions"] =
        benchmark::Counter(numPartitions, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batch_buffer_size"] =
        benchmark::Counter(kBatchBufferSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["split_buffer_size"] =
        benchmark::Counter(kSplitBufferSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["bytes_spilled"] = benchmark::Counter(
        shuffleWriter->totalBytesEvicted(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_written"] = benchmark::Counter(
        shuffleWriter->totalBytesWritten(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_raw"] = benchmark::Counter(
        shuffleWriter->rawPartitionBytes(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_spilled"] = benchmark::Counter(
        shuffleWriter->totalBytesEvicted(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["parquet_parse"] =
        benchmark::Counter(elapseRead, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["write_time"] = benchmark::Counter(
        shuffleWriter->totalWriteTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["spill_time"] = benchmark::Counter(
        shuffleWriter->totalEvictTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["compress_time"] = benchmark::Counter(
        shuffleWriter->totalCompressTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    splitTime = splitTime - shuffleWriter->totalEvictTime() - shuffleWriter->totalCompressTime() -
        shuffleWriter->totalWriteTime();

    state.counters["split_time"] =
        benchmark::Counter(splitTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["total_time"] =
        benchmark::Counter(totalTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    shuffleWriter.reset();
  }

 protected:
  long setCpu(uint32_t cpuindex) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpuindex, &cs);
    return sched_setaffinity(0, sizeof(cs), &cs);
  }

  virtual void doSplit(
      std::shared_ptr<ArrowShuffleWriter>& shuffleWriter,
      int64_t& elapseRead,
      int64_t& numBatches,
      int64_t& numRows,
      int64_t& splitTime,
      const int numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      ShuffleWriterOptions options,
      benchmark::State& state) {}

 protected:
  std::string fileName_;
  std::shared_ptr<arrow::io::RandomAccessFile> file_;
  std::vector<int> rowGroupIndices_;
  std::vector<int> columnIndices_;
  std::shared_ptr<arrow::Schema> schema_;
  parquet::ArrowReaderProperties properties_;
};

class BenchmarkShuffleSplitCacheScanBenchmark : public BenchmarkShuffleSplit {
 public:
  BenchmarkShuffleSplitCacheScanBenchmark(std::string filename) : BenchmarkShuffleSplit(filename) {}

 protected:
  void doSplit(
      std::shared_ptr<ArrowShuffleWriter>& shuffleWriter,
      int64_t& elapseRead,
      int64_t& numBatches,
      int64_t& numRows,
      int64_t& splitTime,
      const int numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      ShuffleWriterOptions options,
      benchmark::State& state) {
    std::vector<int> localColumnIndices;
    // local_column_indices.push_back(0);
    /*    local_column_indices.push_back(0);
        local_column_indices.push_back(1);
        local_column_indices.push_back(2);
        local_column_indices.push_back(4);
        local_column_indices.push_back(5);
        local_column_indices.push_back(6);
        local_column_indices.push_back(7);
*/
    localColumnIndices.push_back(8);
    localColumnIndices.push_back(9);
    localColumnIndices.push_back(13);
    localColumnIndices.push_back(14);
    localColumnIndices.push_back(15);

    std::shared_ptr<arrow::Schema> localSchema;
    arrow::FieldVector fields;
    fields.push_back(schema_->field(8));
    fields.push_back(schema_->field(9));
    fields.push_back(schema_->field(13));
    fields.push_back(schema_->field(14));
    fields.push_back(schema_->field(15));
    localSchema = std::make_shared<arrow::Schema>(fields);

    if (state.thread_index() == 0)
      std::cout << localSchema->ToString() << std::endl;

    GLUTEN_ASSIGN_OR_THROW(shuffleWriter, ArrowShuffleWriter::create(numPartitions, partitionWriterCreator, options));

    std::shared_ptr<arrow::RecordBatch> recordBatch;

    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file_), properties_, &parquetReader));

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    GLUTEN_THROW_NOT_OK(parquetReader->GetRecordBatchReader(rowGroupIndices_, localColumnIndices, &recordBatchReader));
    do {
      TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));

      if (recordBatch) {
        batches.push_back(recordBatch);
        numBatches += 1;
        numRows += recordBatch->num_rows();
      }
    } while (recordBatch);
    std::cout << "parquet parse done elapsed time " << elapseRead / 1000000 << " ms " << std::endl;
    std::cout << "batches = " << numBatches << " rows = " << numRows << std::endl;

    for (auto _ : state) {
      for_each(
          batches.begin(),
          batches.end(),
          [&shuffleWriter, &splitTime](std::shared_ptr<arrow::RecordBatch>& recordBatch) {
            TIME_NANO_OR_THROW(splitTime, shuffleWriter->split(recordBatchToColumnarBatch(recordBatch).get()));
          });
      // std::cout << " split done memory allocated = " <<
      // options.memory_pool->bytes_allocated() << std::endl;
    }

    TIME_NANO_OR_THROW(splitTime, shuffleWriter->stop());
  }
};

class BenchmarkShuffleSplitIterateScanBenchmark : public BenchmarkShuffleSplit {
 public:
  BenchmarkShuffleSplitIterateScanBenchmark(std::string filename) : BenchmarkShuffleSplit(filename) {}

 protected:
  void doSplit(
      std::shared_ptr<ArrowShuffleWriter>& shuffleWriter,
      int64_t& elapseRead,
      int64_t& numBatches,
      int64_t& numRows,
      int64_t& splitTime,
      const int numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      ShuffleWriterOptions options,
      benchmark::State& state) {
    if (state.thread_index() == 0)
      std::cout << schema_->ToString() << std::endl;

    GLUTEN_ASSIGN_OR_THROW(
        shuffleWriter,
        ArrowShuffleWriter::create(numPartitions, std::move(partitionWriterCreator), std::move(options)));

    std::shared_ptr<arrow::RecordBatch> recordBatch;

    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file_), properties_, &parquetReader));

    for (auto _ : state) {
      std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
      GLUTEN_THROW_NOT_OK(parquetReader->GetRecordBatchReader(rowGroupIndices_, columnIndices_, &recordBatchReader));
      TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
      while (recordBatch) {
        numBatches += 1;
        numRows += recordBatch->num_rows();
        TIME_NANO_OR_THROW(splitTime, shuffleWriter->split(recordBatchToColumnarBatch(recordBatch).get()));
        TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
      }
    }
    TIME_NANO_OR_THROW(splitTime, shuffleWriter->stop());
  }
};

} // namespace gluten

int main(int argc, char** argv) {
  uint32_t iterations = 1;
  uint32_t partitions = 192;
  uint32_t threads = 1;
  std::string datafile;
  auto compressionCodec = arrow::Compression::LZ4_FRAME;

  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--iterations") == 0) {
      iterations = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--partitions") == 0) {
      partitions = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--threads") == 0) {
      threads = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--file") == 0) {
      datafile = argv[i + 1];
    } else if (strcmp(argv[i], "--qat") == 0) {
      compressionCodec = arrow::Compression::GZIP;
    }
  }
  std::cout << "iterations = " << iterations << std::endl;
  std::cout << "partitions = " << partitions << std::endl;
  std::cout << "threads = " << threads << std::endl;
  std::cout << "datafile = " << datafile << std::endl;

  /*
    sparkcolumnarplugin::shuffle::BenchmarkShuffleSplit_CacheScan_Benchmark
    bck(datafile);

    benchmark::RegisterBenchmark("BenchmarkShuffleSplit::CacheScan", bck)
        ->Iterations(iterations)
        ->Args({partitions, arrow::Compression::GZIP})
        ->Threads(threads)
        ->ReportAggregatesOnly(false)
        ->MeasureProcessCPUTime()
        ->Unit(benchmark::kSecond);

  */

  gluten::BenchmarkShuffleSplitIterateScanBenchmark bck(datafile);

  benchmark::RegisterBenchmark("BenchmarkShuffleSplit::IterateScan", bck)
      ->Iterations(iterations)
      ->Args({
          partitions,
          compressionCodec,
      })
      ->Threads(threads)
      ->ReportAggregatesOnly(false)
      ->MeasureProcessCPUTime()
      ->Unit(benchmark::kSecond);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}
