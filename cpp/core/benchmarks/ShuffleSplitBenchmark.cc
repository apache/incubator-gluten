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
#include "utils/macros.h"

void print_trace(void) {
  char** strings;
  size_t i, size;
  enum Constexpr { MAX_SIZE = 1024 };
  void* array[MAX_SIZE];
  size = backtrace(array, MAX_SIZE);
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
using gluten::SplitOptions;

namespace gluten {

std::shared_ptr<ColumnarBatch> RecordBatchToColumnarBatch(std::shared_ptr<arrow::RecordBatch> rb) {
  std::unique_ptr<ArrowSchema> cSchema = std::make_unique<ArrowSchema>();
  std::unique_ptr<ArrowArray> cArray = std::make_unique<ArrowArray>();
  GLUTEN_THROW_NOT_OK(arrow::ExportRecordBatch(*rb, cArray.get(), cSchema.get()));
  return std::make_shared<ArrowCStructColumnarBatch>(std::move(cSchema), std::move(cArray));
}

#define ALIGNMENT 2 * 1024 * 1024

const int batch_buffer_size = 32768;
const int split_buffer_size = 8192;

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

  Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
    // auto old_ptr = *ptr;
    RETURN_NOT_OK(pool_->Reallocate(old_size, new_size, ptr));
    stats_.UpdateAllocatedBytes(new_size - old_size);
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

  Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
    return pool_->Reallocate(old_size, new_size, ptr);
#ifdef ENABLELARGEPAGE
    if (new_size < 2 * 1024 * 1024) {
      return pool_->Reallocate(old_size, new_size, ptr);
    } else {
      Status st = pool_->AlignReallocate(old_size, new_size, ptr, ALIGNMENT);
      madvise(*ptr, new_size, /*MADV_HUGEPAGE */ 14);
      return st;
    }
#else
    return pool_->Reallocate(old_size, new_size, ptr);
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
  BenchmarkShuffleSplit(std::string file_name) {
    GetRecordBatchReader(file_name);
  }

  void GetRecordBatchReader(const std::string& input_file) {
    std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
    std::shared_ptr<RecordBatchReader> record_batch_reader;

    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::string file_name;
    GLUTEN_ASSIGN_OR_THROW(fs, arrow::fs::FileSystemFromUriOrPath(input_file, &file_name))

    GLUTEN_ASSIGN_OR_THROW(file, fs->OpenInputFile(file_name));

    properties.set_batch_size(batch_buffer_size);
    properties.set_pre_buffer(false);
    properties.set_use_threads(false);

    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file), properties, &parquet_reader));

    GLUTEN_THROW_NOT_OK(parquet_reader->GetSchema(&schema));

    auto num_rowgroups = parquet_reader->num_row_groups();

    for (int i = 0; i < num_rowgroups; ++i) {
      row_group_indices.push_back(i);
    }

    auto num_columns = schema->num_fields();
    for (int i = 0; i < num_columns; ++i) {
      column_indices.push_back(i);
    }
  }

  void operator()(benchmark::State& state) {
    // SetCPU(state.thread_index());
    arrow::Compression::type compression_type = (arrow::Compression::type)state.range(1);

    std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<LargePageMemoryPool>();

    const int num_partitions = state.range(0);

    auto options = SplitOptions::Defaults();
    options.compression_type = compression_type;
    options.buffer_size = split_buffer_size;
    options.buffered_write = true;
    options.offheap_per_task = 128 * 1024 * 1024 * 1024L;
    options.prefer_evict = true;
    options.write_schema = false;
    options.memory_pool = pool;
    options.partitioning_name = "rr";

    std::shared_ptr<ArrowShuffleWriter> shuffle_writer;
    int64_t elapse_read = 0;
    int64_t num_batches = 0;
    int64_t num_rows = 0;
    int64_t split_time = 0;
    auto start_time = std::chrono::steady_clock::now();

    Do_Split(shuffle_writer, elapse_read, num_batches, num_rows, split_time, num_partitions, options, state);
    auto end_time = std::chrono::steady_clock::now();
    auto total_time = (end_time - start_time).count();

    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    GLUTEN_THROW_NOT_OK(fs->DeleteFile(shuffle_writer->DataFile()));

    state.SetBytesProcessed(int64_t(shuffle_writer->RawPartitionBytes()));

    state.counters["rowgroups"] = benchmark::Counter(
        row_group_indices.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["columns"] =
        benchmark::Counter(column_indices.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batches"] =
        benchmark::Counter(num_batches, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["num_rows"] =
        benchmark::Counter(num_rows, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["num_partitions"] =
        benchmark::Counter(num_partitions, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batch_buffer_size"] =
        benchmark::Counter(batch_buffer_size, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["split_buffer_size"] =
        benchmark::Counter(split_buffer_size, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["bytes_spilled"] = benchmark::Counter(
        shuffle_writer->TotalBytesEvicted(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_written"] = benchmark::Counter(
        shuffle_writer->TotalBytesWritten(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_raw"] = benchmark::Counter(
        shuffle_writer->RawPartitionBytes(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_spilled"] = benchmark::Counter(
        shuffle_writer->TotalBytesEvicted(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["parquet_parse"] =
        benchmark::Counter(elapse_read, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["write_time"] = benchmark::Counter(
        shuffle_writer->TotalWriteTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["spill_time"] = benchmark::Counter(
        shuffle_writer->TotalEvictTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["compress_time"] = benchmark::Counter(
        shuffle_writer->TotalCompressTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    split_time = split_time - shuffle_writer->TotalEvictTime() - shuffle_writer->TotalCompressTime() -
        shuffle_writer->TotalWriteTime();

    state.counters["split_time"] =
        benchmark::Counter(split_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["total_time"] =
        benchmark::Counter(total_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    shuffle_writer.reset();
  }

 protected:
  long SetCPU(uint32_t cpuindex) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpuindex, &cs);
    return sched_setaffinity(0, sizeof(cs), &cs);
  }

  virtual void Do_Split(
      std::shared_ptr<ArrowShuffleWriter>& shuffle_writer,
      int64_t& elapse_read,
      int64_t& num_batches,
      int64_t& num_rows,
      int64_t& split_time,
      const int num_partitions,
      SplitOptions options,
      benchmark::State& state) {}

 protected:
  std::string file_name;
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::vector<int> row_group_indices;
  std::vector<int> column_indices;
  std::shared_ptr<arrow::Schema> schema;
  parquet::ArrowReaderProperties properties;
};

class BenchmarkShuffleSplit_CacheScan_Benchmark : public BenchmarkShuffleSplit {
 public:
  BenchmarkShuffleSplit_CacheScan_Benchmark(std::string filename) : BenchmarkShuffleSplit(filename) {}

 protected:
  void Do_Split(
      std::shared_ptr<ArrowShuffleWriter>& shuffle_writer,
      int64_t& elapse_read,
      int64_t& num_batches,
      int64_t& num_rows,
      int64_t& split_time,
      const int num_partitions,
      SplitOptions options,
      benchmark::State& state) {
    std::vector<int> local_column_indices;
    // local_column_indices.push_back(0);
    /*    local_column_indices.push_back(0);
        local_column_indices.push_back(1);
        local_column_indices.push_back(2);
        local_column_indices.push_back(4);
        local_column_indices.push_back(5);
        local_column_indices.push_back(6);
        local_column_indices.push_back(7);
*/
    local_column_indices.push_back(8);
    local_column_indices.push_back(9);
    local_column_indices.push_back(13);
    local_column_indices.push_back(14);
    local_column_indices.push_back(15);

    std::shared_ptr<arrow::Schema> local_schema;
    arrow::FieldVector fields;
    fields.push_back(schema->field(8));
    fields.push_back(schema->field(9));
    fields.push_back(schema->field(13));
    fields.push_back(schema->field(14));
    fields.push_back(schema->field(15));
    local_schema = std::make_shared<arrow::Schema>(fields);

    if (state.thread_index() == 0)
      std::cout << local_schema->ToString() << std::endl;

    GLUTEN_ASSIGN_OR_THROW(shuffle_writer, ArrowShuffleWriter::Create(num_partitions, options));

    std::shared_ptr<arrow::RecordBatch> record_batch;

    std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
    std::shared_ptr<RecordBatchReader> record_batch_reader;
    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file), properties, &parquet_reader));

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    GLUTEN_THROW_NOT_OK(
        parquet_reader->GetRecordBatchReader(row_group_indices, local_column_indices, &record_batch_reader));
    do {
      TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));

      if (record_batch) {
        batches.push_back(record_batch);
        num_batches += 1;
        num_rows += record_batch->num_rows();
      }
    } while (record_batch);
    std::cout << "parquet parse done elapsed time " << elapse_read / 1000000 << " ms " << std::endl;
    std::cout << "batches = " << num_batches << " rows = " << num_rows << std::endl;

    for (auto _ : state) {
      for_each(
          batches.begin(),
          batches.end(),
          [&shuffle_writer, &split_time, &options](std::shared_ptr<arrow::RecordBatch>& record_batch) {
            TIME_NANO_OR_THROW(split_time, shuffle_writer->Split(RecordBatchToColumnarBatch(record_batch).get()));
          });
      // std::cout << " split done memory allocated = " <<
      // options.memory_pool->bytes_allocated() << std::endl;
    }

    TIME_NANO_OR_THROW(split_time, shuffle_writer->Stop());
  }
};

class BenchmarkShuffleSplit_IterateScan_Benchmark : public BenchmarkShuffleSplit {
 public:
  BenchmarkShuffleSplit_IterateScan_Benchmark(std::string filename) : BenchmarkShuffleSplit(filename) {}

 protected:
  void Do_Split(
      std::shared_ptr<ArrowShuffleWriter>& shuffle_writer,
      int64_t& elapse_read,
      int64_t& num_batches,
      int64_t& num_rows,
      int64_t& split_time,
      const int num_partitions,
      SplitOptions options,
      benchmark::State& state) {
    if (state.thread_index() == 0)
      std::cout << schema->ToString() << std::endl;

    GLUTEN_ASSIGN_OR_THROW(shuffle_writer, ArrowShuffleWriter::Create(num_partitions, std::move(options)));

    std::shared_ptr<arrow::RecordBatch> record_batch;

    std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
    std::shared_ptr<RecordBatchReader> record_batch_reader;
    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file), properties, &parquet_reader));

    for (auto _ : state) {
      std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
      GLUTEN_THROW_NOT_OK(
          parquet_reader->GetRecordBatchReader(row_group_indices, column_indices, &record_batch_reader));
      TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      while (record_batch) {
        num_batches += 1;
        num_rows += record_batch->num_rows();
        TIME_NANO_OR_THROW(split_time, shuffle_writer->Split(RecordBatchToColumnarBatch(record_batch).get()));
        TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      }
    }
    TIME_NANO_OR_THROW(split_time, shuffle_writer->Stop());
  }
};

} // namespace gluten

int main(int argc, char** argv) {
  uint32_t iterations = 1;
  uint32_t partitions = 192;
  uint32_t threads = 1;
  std::string datafile;
  auto compression_codec = arrow::Compression::LZ4_FRAME;

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
      compression_codec = arrow::Compression::GZIP;
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

  gluten::BenchmarkShuffleSplit_IterateScan_Benchmark bck(datafile);

  benchmark::RegisterBenchmark("BenchmarkShuffleSplit::IterateScan", bck)
      ->Iterations(iterations)
      ->Args({
          partitions,
          compression_codec,
      })
      ->Threads(threads)
      ->ReportAggregatesOnly(false)
      ->MeasureProcessCPUTime()
      ->Unit(benchmark::kSecond);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}
