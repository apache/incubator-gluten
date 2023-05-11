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
#include <benchmark/benchmark.h>
#include <execinfo.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <sched.h>
#include <sys/mman.h>

#include <chrono>
#include <utility>

#include "shuffle/ArrowShuffleWriter.h"
#include "utils/compression.h"
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

#define ALIGNMENT 2 * 1024 * 1024

const int32_t kQatGzip = 0;
const int32_t kQplGzip = 1;
const int32_t kLZ4 = 2;

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

class LargePageMemoryPool final : public arrow::MemoryPool {
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

class BenchmarkCompression {
 public:
  explicit BenchmarkCompression(const std::string& fileName, uint32_t splitBufferSize) {
    getRecordBatchReader(fileName, splitBufferSize);
  }

  virtual std::string name() const = 0;

  void getRecordBatchReader(const std::string& inputFile, uint32_t splitBufferSize) {
    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;

    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::string fileName;
    GLUTEN_ASSIGN_OR_THROW(fs, arrow::fs::FileSystemFromUriOrPath(inputFile, &fileName))

    GLUTEN_ASSIGN_OR_THROW(file_, fs->OpenInputFile(fileName));

    properties_.set_batch_size(splitBufferSize);
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
    setCpu(state.range(2) + state.thread_index());
    auto ipcWriteOptions = arrow::ipc::IpcWriteOptions::Defaults();
    ipcWriteOptions.use_threads = false;
    auto splitBufferSize = (uint32_t)state.range(1);
    auto compressionType = state.range(0);
    switch (compressionType) {
      case gluten::kLZ4: {
        GLUTEN_ASSIGN_OR_THROW(ipcWriteOptions.codec, createArrowIpcCodec(arrow::Compression::LZ4_FRAME));
        break;
      }
#ifdef GLUTEN_ENABLE_QAT
      case gluten::QAT_GZIP: {
        qat::EnsureQatCodecRegistered("gzip");
        GLUTEN_ASSIGN_OR_THROW(ipcWriteOptions.codec, createArrowIpcCodec(arrow::Compression::CUSTOM));
        break;
      }
#endif
#ifdef GLUTEN_ENABLE_IAA
      case gluten::QPL_GZIP: {
        qpl::EnsureQplCodecRegistered("gzip");
        GLUTEN_ASSIGN_OR_THROW(ipcWriteOptions.codec, createArrowIpcCodec(arrow::Compression::CUSTOM));
        break;
      }
#endif
      default:
        throw GlutenException("Codec not supported. Only support LZ4 or QATGzip");
    }
    std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<LargePageMemoryPool>();
    ipcWriteOptions.memory_pool = pool.get();

    int64_t elapseRead = 0;
    int64_t numBatches = 0;
    int64_t numRows = 0;
    int64_t compressTime = 0;
    int64_t uncompressedSize = 0;
    int64_t compressedSize = 0;

    auto startTime = std::chrono::steady_clock::now();

    doCompress(elapseRead, numBatches, numRows, compressTime, uncompressedSize, compressedSize, ipcWriteOptions, state);
    auto endTime = std::chrono::steady_clock::now();
    auto totalTime = (endTime - startTime).count();
    std::cout << "Thread " << state.thread_index() << " took " << (1.0 * totalTime / 1e9) << "s" << std::endl;

    state.counters["rowgroups"] =
        benchmark::Counter(rowGroupIndices_.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["columns"] =
        benchmark::Counter(columnIndices_.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["batches"] =
        benchmark::Counter(numBatches, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["num_rows"] =
        benchmark::Counter(numRows, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["batch_buffer_size"] =
        benchmark::Counter(splitBufferSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["parquet_parse"] =
        benchmark::Counter(elapseRead, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["compress_time"] =
        benchmark::Counter(compressTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["total_time"] =
        benchmark::Counter(totalTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["uncompressed_size"] =
        benchmark::Counter(uncompressedSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["compressed_size"] =
        benchmark::Counter(compressedSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    auto compressionRatio = 1.0 * compressedSize / uncompressedSize;
    state.counters["compression_ratio"] =
        benchmark::Counter(compressionRatio, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    // compress_time is in nanosecond, zoom out to second.
    auto throughput = 1.0 * uncompressedSize / compressTime * 1e9 * 8;
    state.counters["throughput_total"] =
        benchmark::Counter(throughput, benchmark::Counter::kDefaults, benchmark::Counter::OneK::kIs1024);

    state.counters["throughput_per_thread"] =
        benchmark::Counter(throughput, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
  }

 protected:
  long setCpu(uint32_t cpuindex) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpuindex, &cs);
    return sched_setaffinity(0, sizeof(cs), &cs);
  }

  virtual void doCompress(
      int64_t& elapseRead,
      int64_t& numBatches,
      int64_t& numRows,
      int64_t& compressTime,
      int64_t& uncompressedSize,
      int64_t& compressedSize,
      arrow::ipc::IpcWriteOptions& ipcWriteOptions,
      benchmark::State& state) {}

 protected:
  std::shared_ptr<arrow::io::RandomAccessFile> file_;
  std::vector<int> rowGroupIndices_;
  std::vector<int> columnIndices_;
  std::shared_ptr<arrow::Schema> schema_;
  parquet::ArrowReaderProperties properties_;
};

class BenchmarkCompressionCacheScanBenchmark final : public BenchmarkCompression {
 public:
  explicit BenchmarkCompressionCacheScanBenchmark(const std::string& filename, uint32_t splitBufferSize)
      : BenchmarkCompression(filename, splitBufferSize) {}

  std::string name() const override {
    return "CacheScan";
  }

 protected:
  void doCompress(
      int64_t& elapseRead,
      int64_t& numBatches,
      int64_t& numRows,
      int64_t& compressTime,
      int64_t& uncompressedSize,
      int64_t& compressedSize,
      arrow::ipc::IpcWriteOptions& ipcWriteOptions,
      benchmark::State& state) override {
    std::shared_ptr<arrow::RecordBatch> recordBatch;

    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file_), properties_, &parquetReader));

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    GLUTEN_THROW_NOT_OK(parquetReader->GetRecordBatchReader(rowGroupIndices_, columnIndices_, &recordBatchReader));
    do {
      TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));

      if (recordBatch) {
        batches.push_back(recordBatch);
        numBatches += 1;
        numRows += recordBatch->num_rows();
      }
    } while (recordBatch);

    std::cout << "parquet parse done elapsed time " << elapseRead / 1e6 << " ms " << std::endl;
    std::cout << "batches = " << numBatches << " rows = " << numRows << std::endl;
    for (auto _ : state) {
      auto it = batches.begin();
      auto processedBatches = 0;
      while (it != batches.end()) {
        recordBatch = *it++;
        for (auto i = 0; i < recordBatch->num_columns(); ++i) {
          recordBatch->column(i)->data()->buffers[0] = nullptr;
        }
        auto payload = std::make_shared<arrow::ipc::IpcPayload>();

        TIME_NANO_OR_THROW(
            compressTime, arrow::ipc::GetRecordBatchPayload(*recordBatch, ipcWriteOptions, payload.get()));
        uncompressedSize += payload->raw_body_length;
        compressedSize += payload->body_length;
        std::cout << "Compressed " << processedBatches++ << " batches" << std::endl;
        TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
      }
    }
  }
};

class BenchmarkCompressionIterateScanBenchmark final : public BenchmarkCompression {
 public:
  explicit BenchmarkCompressionIterateScanBenchmark(const std::string& filename, uint32_t splitBufferSize)
      : BenchmarkCompression(filename, splitBufferSize) {}

  std::string name() const override {
    return "IterateScan";
  }

 protected:
  void doCompress(
      int64_t& elapseRead,
      int64_t& numBatches,
      int64_t& numRows,
      int64_t& compressTime,
      int64_t& uncompressedSize,
      int64_t& compressedSize,
      arrow::ipc::IpcWriteOptions& ipcWriteOptions,
      benchmark::State& state) override {
    std::shared_ptr<arrow::RecordBatch> recordBatch;

    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file_), properties_, &parquetReader));

    for (auto _ : state) {
      GLUTEN_THROW_NOT_OK(parquetReader->GetRecordBatchReader(rowGroupIndices_, columnIndices_, &recordBatchReader));
      TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
      while (recordBatch) {
        numBatches += 1;
        numRows += recordBatch->num_rows();
        for (auto i = 0; i < recordBatch->num_columns(); ++i) {
          recordBatch->column(i)->data()->buffers[0] = nullptr;
        }
        auto payload = std::make_shared<arrow::ipc::IpcPayload>();

        TIME_NANO_OR_THROW(
            compressTime, arrow::ipc::GetRecordBatchPayload(*recordBatch, ipcWriteOptions, payload.get()));
        uncompressedSize += payload->raw_body_length;
        compressedSize += payload->body_length;
        //        std::cout << "Compressed " << num_batches << " batches" << std::endl;
        TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
      }
    }
  }
};

} // namespace gluten

int main(int argc, char** argv) {
  uint32_t iterations = 1;
  uint32_t threads = 1;
  uint32_t cpuOffset = 0;
  std::string datafile;
  auto codec = gluten::kLZ4;
  uint32_t splitBufferSize = 8192;

  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--iterations") == 0) {
      iterations = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--threads") == 0) {
      threads = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--file") == 0) {
      datafile = argv[i + 1];
    } else if (strcmp(argv[i], "--qat-gzip") == 0) {
      std::cout << "QAT gzip is used as codec" << std::endl;
      codec = gluten::kQatGzip;
    } else if (strcmp(argv[i], "--qpl-gzip") == 0) {
      std::cout << "QPL gzip is used as codec" << std::endl;
      codec = gluten::kQplGzip;
    } else if (strcmp(argv[i], "--busy") == 0) {
      GLUTEN_THROW_NOT_OK(arrow::internal::SetEnvVar("QZ_POLLING_MODE", "BUSY"));
    } else if (strcmp(argv[i], "--buffer-size") == 0) {
      splitBufferSize = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--cpu-offset") == 0) {
      cpuOffset = atol(argv[i + 1]);
    }
  }
  std::cout << "iterations = " << iterations << std::endl;
  std::cout << "threads = " << threads << std::endl;
  std::cout << "datafile = " << datafile << std::endl;

  gluten::BenchmarkCompressionIterateScanBenchmark bmIterateScan(datafile, splitBufferSize);
  gluten::BenchmarkCompressionCacheScanBenchmark bmCacheScan(datafile, splitBufferSize);

  benchmark::RegisterBenchmark(bmIterateScan.name().c_str(), bmIterateScan)
      ->Iterations(iterations)
      ->Args({
          codec,
          splitBufferSize,
          cpuOffset,
      })
      ->Threads(threads)
      ->ReportAggregatesOnly(false)
      ->MeasureProcessCPUTime()
      ->Unit(benchmark::kSecond);

  benchmark::RegisterBenchmark(bmCacheScan.name().c_str(), bmCacheScan)
      ->Iterations(iterations)
      ->Args({
          codec,
          splitBufferSize,
      })
      ->Threads(threads)
      ->ReportAggregatesOnly(false)
      ->MeasureProcessCPUTime()
      ->Unit(benchmark::kSecond);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}
