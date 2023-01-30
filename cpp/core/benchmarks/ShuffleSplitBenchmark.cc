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

#include "operators/shuffle/splitter.h"
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

using gluten::GlutenException;
using gluten::SplitOptions;
using gluten::Splitter;

namespace gluten {

#define ALIGNMENT 2 * 1024 * 1024
#define LARGE_BUFFER_SIZE 16 * 1024 * 1024

const int batch_buffer_size = 32768;
const int split_buffer_size = 32768;

class LargeMemoryPool : public arrow::MemoryPool {
 public:
  constexpr static uint64_t huge_page_size = 1 << 21;

  explicit LargeMemoryPool() {}

  ~LargeMemoryPool() override = default;

  Status Allocate(int64_t size, uint8_t** out) override {
    
    uint64_t alloc_size = size>LARGE_BUFFER_SIZE?size:LARGE_BUFFER_SIZE;
    alloc_size=round_to_huge_page_size(alloc_size);

    auto its = std::find_if(buffers_.begin(),buffers_.end(),[size](BufferAllocated& buf){
      return buf.allocated+size<buf.alloc_size;
    });

    BufferAllocated lastalloc;

    if(its==buffers_.end())
    {
      uint8_t* alloc_addr;
      RETURN_NOT_OK(do_alloc(alloc_size,&alloc_addr));
      lastalloc={alloc_addr,0,0,alloc_size};
      buffers_.push_back(lastalloc);
      std::cout << "alloc " << std::hex << alloc_addr << std::dec << " buffer size = " << buffers_.size() << std::endl;
    }else
    {
      lastalloc=*its;
    }

    *out=lastalloc.start_addr+lastalloc.allocated;
    lastalloc.allocated+=size;
    return arrow::Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    return arrow::Status::Invalid("Realloc in large page memory pool is not allowed");
  }

  void Free(uint8_t* buffer, int64_t size) override {

    auto its = std::find_if(buffers_.begin(),buffers_.end(),[buffer](BufferAllocated& buf){
      return buffer>=buf.start_addr && buffer <buf.start_addr+buf.alloc_size;
    });
    ARROW_CHECK_NE(its, buffers_.end());
    BufferAllocated &to_free = *its;

    to_free.freed+=size;
    if(to_free.freed && to_free.freed==to_free.allocated)
    {
      do_free(to_free.start_addr, to_free.alloc_size);
      buffers_.erase(its);
      std::cout << "free " << std::hex << to_free.start_addr << std::dec << " buffer size = " << buffers_.size() << std::endl;
    }
  }

  int64_t bytes_allocated() const override {
    return std::accumulate(buffers_.begin(),buffers_.end(), 0LL, [](uint64_t a, const BufferAllocated& buf){
      return a+buf.alloc_size;
    });
  }

  int64_t max_memory() const override {
    return pool_->max_memory();
  }

  std::string backend_name() const override {
    return "LargeMemoryPool";
  }

 protected:

  virtual Status do_alloc(int64_t size, uint8_t** out)
  {
    return pool_->Allocate(size, out);
  }
  virtual void do_free(uint8_t* buffer, int64_t size)
  {
    pool_->Free(buffer, size);
  }

  uint64_t round_to_huge_page_size(uint64_t n) {
    return n+= huge_page_size - (n & (huge_page_size-1));    
  }


  struct BufferAllocated{
    uint8_t* start_addr;
    uint64_t allocated;
    uint64_t freed;
    uint64_t alloc_size;
  };

  std::vector<BufferAllocated> buffers_;
  MemoryPool* pool_ = arrow::default_memory_pool();
};

class LargePageMemoryPool : public LargeMemoryPool
{
 protected:

  virtual Status do_alloc(int64_t size, uint8_t** out)
  {
    int rst = posix_memalign((void**)out, 1 << 21, size);
    madvise(*out, size, MADV_HUGEPAGE);
    madvise(*out, size, MADV_WILLNEED);
    if (rst!=0 || *out == nullptr) {
      return arrow::Status::OutOfMemory(" posix_memalign error ");
    }else
    {
      return arrow::Status::OK();
    }
  }
  virtual void do_free(uint8_t* buffer, int64_t size)
  {
    std::free((void*)(buffer));
  }
};

class MMapMemoryPool : public LargeMemoryPool
{
 protected:
  virtual Status do_alloc(int64_t size, uint8_t** out)
  {
    *out = static_cast<uint8_t *>(mmap(
        nullptr, size, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_POPULATE, -1, 0));
    if (*out == MAP_FAILED) {
      return arrow::Status::OutOfMemory(" posix_memalign error ");
    }
    else
    {
      //madvise(*out, size, MADV_WILLNEED);
      return arrow::Status::OK();
    }
  }
  virtual void do_free(uint8_t* buffer, int64_t size)
  {
    munmap((void*)(buffer),size);
  }
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

    std::shared_ptr<arrow::MemoryPool> pool;
    if(state.range(2)==0)
    {
      pool = GetDefaultWrappedArrowMemoryPool();
    }else if (state.range(2)==1)
    {
      pool = std::make_shared<LargeMemoryPool>();
    }else if (state.range(2)==1)
    {
      pool = std::make_shared<LargePageMemoryPool>();
    }else if (state.range(2)==1)
    {
      pool = std::make_shared<MMapMemoryPool>();
    }
    const int num_partitions = state.range(0);

    auto options = SplitOptions::Defaults();
    options.compression_type = compression_type;
    options.buffer_size = split_buffer_size;
    options.buffered_write = true;
    options.offheap_per_task = 128 * 1024 * 1024 * 1024L;
    options.prefer_spill = state.range(3);
    options.write_schema = false;
    options.large_memory_pool = pool;

    std::shared_ptr<Splitter> splitter;
    int64_t elapse_read = 0;
    int64_t num_batches = 0;
    int64_t num_rows = 0;
    int64_t split_time = 0;
    auto start_time = std::chrono::steady_clock::now();

    Do_Split(splitter, elapse_read, num_batches, num_rows, split_time, num_partitions, options, state);
    auto end_time = std::chrono::steady_clock::now();
    auto total_time = (end_time - start_time).count();

    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    fs->DeleteFile(splitter->DataFile());

    state.SetBytesProcessed(int64_t(splitter->RawPartitionBytes()));

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
        splitter->TotalBytesSpilled(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_written"] = benchmark::Counter(
        splitter->TotalBytesWritten(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_raw"] = benchmark::Counter(
        splitter->RawPartitionBytes(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_spilled"] = benchmark::Counter(
        splitter->TotalBytesSpilled(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["parquet_parse"] =
        benchmark::Counter(elapse_read, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["write_time"] = benchmark::Counter(
        splitter->TotalWriteTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["spill_time"] = benchmark::Counter(
        splitter->TotalSpillTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["compress_time"] = benchmark::Counter(
        splitter->TotalCompressTime(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    split_time = split_time - splitter->TotalSpillTime() - splitter->TotalCompressTime() - splitter->TotalWriteTime();

    state.counters["split_time"] =
        benchmark::Counter(split_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["total_time"] =
        benchmark::Counter(total_time, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    std::cout << "release splitter " << std::endl;
    splitter.reset();
    std::cout << "release splitter done" << std::endl;


  }

 protected:
  long SetCPU(uint32_t cpuindex) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpuindex, &cs);
    return sched_setaffinity(0, sizeof(cs), &cs);
  }

  virtual void Do_Split(
      std::shared_ptr<Splitter>& splitter,
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
      std::shared_ptr<Splitter>& splitter,
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

    GLUTEN_ASSIGN_OR_THROW(splitter, Splitter::Make("rr", local_schema, num_partitions, options));

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
          [&splitter, &split_time, &options](std::shared_ptr<arrow::RecordBatch>& record_batch) {
            TIME_NANO_OR_THROW(split_time, splitter->Split(*record_batch));
          });
      // std::cout << " split done memory allocated = " <<
      // options.memory_pool->bytes_allocated() << std::endl;
    }

    TIME_NANO_OR_THROW(split_time, splitter->Stop());
  }
};

class BenchmarkShuffleSplit_IterateScan_Benchmark : public BenchmarkShuffleSplit {
 public:
  BenchmarkShuffleSplit_IterateScan_Benchmark(std::string filename) : BenchmarkShuffleSplit(filename) {}

 protected:
  void Do_Split(
      std::shared_ptr<Splitter>& splitter,
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
    local_column_indices.push_back(8);
    local_column_indices.push_back(9);
    local_column_indices.push_back(13);
    local_column_indices.push_back(14);
*/
    local_column_indices.push_back(1);

    std::shared_ptr<arrow::Schema> local_schema;
    arrow::FieldVector fields;
/*    fields.push_back(schema->field(8));
    fields.push_back(schema->field(9));
    fields.push_back(schema->field(13));
    fields.push_back(schema->field(14));*/
    fields.push_back(schema->field(1));
    local_schema = std::make_shared<arrow::Schema>(fields);


    if (state.thread_index() == 0)
      std::cout << local_schema->ToString() << std::endl;

    GLUTEN_ASSIGN_OR_THROW(splitter, Splitter::Make("rr", local_schema, num_partitions, std::move(options)));

    std::shared_ptr<arrow::RecordBatch> record_batch;

    std::unique_ptr<::parquet::arrow::FileReader> parquet_reader;
    std::shared_ptr<RecordBatchReader> record_batch_reader;
    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file), properties, &parquet_reader));

    for (auto _ : state) {
      std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
      GLUTEN_THROW_NOT_OK(
          parquet_reader->GetRecordBatchReader(row_group_indices, local_column_indices, &record_batch_reader));
      TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      while (record_batch) {
        num_batches += 1;
        num_rows += record_batch->num_rows();
        TIME_NANO_OR_THROW(split_time, splitter->Split(*record_batch));
        TIME_NANO_OR_THROW(elapse_read, record_batch_reader->ReadNext(&record_batch));
      }
    }
    TIME_NANO_OR_THROW(split_time, splitter->Stop());
  }
};

} // namespace gluten

int main(int argc, char** argv) {
  uint32_t iterations = 1;
  uint32_t partitions = 192;
  uint32_t threads = 1;
  std::string datafile;
  uint32_t memory_pool=0;
  uint32_t prefer_spill=1;

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
    } else if (strcmp(argv[i], "--pool") == 0) {
      memory_pool = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--spill") == 0) {
      prefer_spill = atol(argv[i + 1]);
    }
  }
  std::cout << "iterations = " << iterations << std::endl;
  std::cout << "partitions = " << partitions << std::endl;
  std::cout << "threads = " << threads << std::endl;
  std::cout << "datafile = " << datafile << std::endl;
  std::cout << "memory pool = " << memory_pool << std::endl;
  std::cout << "prefer_spill = " << prefer_spill << std::endl;

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
          memory_pool,
          prefer_spill,
      })
      ->Threads(threads)
      ->ReportAggregatesOnly(false)
      ->MeasureProcessCPUTime()
      ->Unit(benchmark::kSecond);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}
