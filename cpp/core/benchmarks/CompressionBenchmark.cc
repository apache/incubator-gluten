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

#include <arrow/extension_type.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/ipc/options.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <benchmark/benchmark.h>
#include <execinfo.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <sched.h>

#include <chrono>
#include <iostream>
#include <utility>

#include "shuffle/ShuffleWriter.h"
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
using gluten::GlutenException;
using gluten::ShuffleWriterOptions;

namespace gluten {

#define ALIGNMENT 2 * 1024 * 1024

const int32_t kQatGzip = 0;
const int32_t kQatZstd = 1;
const int32_t kQplGzip = 2;
const int32_t kLZ4 = 3;
const int32_t kZstd = 4;

class MyMemoryPool final : public arrow::MemoryPool {
 public:
  explicit MyMemoryPool() {}

  Status Allocate(int64_t size, uint8_t** out) override {
    RETURN_NOT_OK(pool_->Allocate(size, out));
    stats_.UpdateAllocatedBytes(size);
    // std::cout << "Allocate: size = " << size << " addr = " << std::hex <<
    // (uint64_t)*out << std::dec << std::endl; print_trace();
    return arrow::Status::OK();
  }

  Status Reallocate(int64_t oldSize, int64_t newSize, uint8_t** ptr) override {
    // auto old_ptr = *ptr;
    RETURN_NOT_OK(pool_->Reallocate(oldSize, newSize, ptr));
    stats_.UpdateAllocatedBytes(newSize - oldSize);
    // std::cout << "Reallocate: old_size = " << old_size << " old_ptr = " <<
    // std::hex << (uint64_t)old_ptr << std::dec << " new_size = " << new_size
    // << " addr = " << std::hex << (uint64_t)*ptr << std::dec << std::endl;
    // print_trace();
    return arrow::Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) override {
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

class BenchmarkCompression {
 public:
  explicit BenchmarkCompression(const std::string& fileName, uint32_t compressBufferSize) {
    getRecordBatchReader(fileName, compressBufferSize);
  }

  virtual std::string name() const = 0;

  void getRecordBatchReader(const std::string& inputFile, uint32_t compressBufferSize) {
    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;

    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::string fileName;
    GLUTEN_ASSIGN_OR_THROW(fs, arrow::fs::FileSystemFromUriOrPath(inputFile, &fileName))

    GLUTEN_ASSIGN_OR_THROW(file_, fs->OpenInputFile(fileName));

    properties_.set_batch_size(compressBufferSize);
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
    auto compressBufferSize = (uint32_t)state.range(1);
    auto compressionType = state.range(0);
    switch (compressionType) {
      case gluten::kLZ4: {
        ipcWriteOptions.codec = createArrowIpcCodec(arrow::Compression::LZ4_FRAME, CodecBackend::NONE);
        break;
      }
      case gluten::kZstd: {
        ipcWriteOptions.codec = createArrowIpcCodec(arrow::Compression::ZSTD, CodecBackend::NONE);
        break;
      }
#ifdef GLUTEN_ENABLE_QAT
      case gluten::kQatGzip: {
        ipcWriteOptions.codec = createArrowIpcCodec(arrow::Compression::GZIP, CodecBackend::QAT);
        break;
      }
      case gluten::kQatZstd: {
        ipcWriteOptions.codec = createArrowIpcCodec(arrow::Compression::ZSTD, CodecBackend::QAT);
        std::cout << "load qatzstd" << std::endl;
        break;
      }
#endif
#ifdef GLUTEN_ENABLE_IAA
      case gluten::kQplGzip: {
        ipcWriteOptions.codec = createArrowIpcCodec(arrow::Compression::ZSTD, CodecBackend::IAA);
        break;
      }
#endif
      default:
        throw GlutenException("Codec not supported. Only support LZ4 or QATGzip");
    }
    std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<MyMemoryPool>();
    ipcWriteOptions.memory_pool = pool.get();

    int64_t elapseRead = 0;
    int64_t numBatches = 0;
    int64_t numRows = 0;
    int64_t compressTime = 0;
    int64_t decompressTime = 0;
    int64_t uncompressedSize = 0;
    int64_t compressedSize = 0;

    auto startTime = std::chrono::steady_clock::now();

    doCompress(
        elapseRead,
        numBatches,
        numRows,
        compressTime,
        decompressTime,
        uncompressedSize,
        compressedSize,
        ipcWriteOptions,
        state);
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
        benchmark::Counter(compressBufferSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["parquet_parse"] =
        benchmark::Counter(elapseRead, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["compress_time"] =
        benchmark::Counter(compressTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

    state.counters["decompress_time"] =
        benchmark::Counter(decompressTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);

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
      int64_t& decompressTime,
      int64_t& uncompressedSize,
      int64_t& compressedSize,
      arrow::ipc::IpcWriteOptions& ipcWriteOptions,
      benchmark::State& state) {}

  void decompress(
      const arrow::ipc::IpcWriteOptions& ipcWriteOptions,
      const std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>& payloads,
      const std::vector<std::vector<int64_t>>& uncompressedBufferSize,
      int64_t& decompressTime) {
    TIME_NANO_START(decompressTime);
    auto codec = ipcWriteOptions.codec;
    for (auto i = 0; i < payloads.size(); ++i) {
      auto& buffers = payloads[i]->body_buffers;
      for (auto j = 0; j < buffers.size(); ++j) {
        auto outputSize = uncompressedBufferSize[i][j];
        if (buffers[j] && outputSize >= 0) {
          GLUTEN_ASSIGN_OR_THROW(auto out, arrow::AllocateResizableBuffer(outputSize, ipcWriteOptions.memory_pool));
          // Exclude the first 8-byte buffer metadata.
          GLUTEN_ASSIGN_OR_THROW(
              auto len,
              codec->Decompress(buffers[j]->size() - 8, buffers[j]->data() + 8, outputSize, out->mutable_data()));
          (void)len;
        }
      }
    }
    TIME_NANO_END(decompressTime);
  }

 protected:
  std::shared_ptr<arrow::io::RandomAccessFile> file_;
  std::vector<int> rowGroupIndices_;
  std::vector<int> columnIndices_;
  std::shared_ptr<arrow::Schema> schema_;
  parquet::ArrowReaderProperties properties_;
};

class BenchmarkCompressionCacheScanBenchmark final : public BenchmarkCompression {
 public:
  explicit BenchmarkCompressionCacheScanBenchmark(const std::string& filename, uint32_t compressBufferSize)
      : BenchmarkCompression(filename, compressBufferSize) {}

  std::string name() const override {
    return "CacheScan";
  }

 protected:
  void doCompress(
      int64_t& elapseRead,
      int64_t& numBatches,
      int64_t& numRows,
      int64_t& compressTime,
      int64_t& decompressTime,
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

    std::vector<std::shared_ptr<arrow::ipc::IpcPayload>> payloads(batches.size());
    std::vector<std::vector<int64_t>> uncompressedBufferSize(batches.size());

    for (auto _ : state) {
      auto it = batches.begin();
      auto pos = 0;
      while (it != batches.end()) {
        recordBatch = *it++;
        for (auto i = 0; i < recordBatch->num_columns(); ++i) {
          recordBatch->column(i)->data()->buffers[0] = nullptr;
          for (auto& buffer : recordBatch->column(i)->data()->buffers) {
            if (buffer) {
              uncompressedBufferSize[pos].push_back(buffer->size());
            } else {
              uncompressedBufferSize[pos].push_back(-1);
            }
          }
        }
        auto payload = std::make_shared<arrow::ipc::IpcPayload>();

        TIME_NANO_OR_THROW(
            compressTime, arrow::ipc::GetRecordBatchPayload(*recordBatch, ipcWriteOptions, payload.get()));
        uncompressedSize += payload->raw_body_length;
        compressedSize += payload->body_length;
        TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
        payloads[pos] = std::move(payload);
        pos++;
      }

      decompress(ipcWriteOptions, payloads, uncompressedBufferSize, decompressTime);
    }
  }
};

class BenchmarkCompressionIterateScanBenchmark final : public BenchmarkCompression {
 public:
  explicit BenchmarkCompressionIterateScanBenchmark(const std::string& filename, uint32_t compressBufferSize)
      : BenchmarkCompression(filename, compressBufferSize) {}

  std::string name() const override {
    return "IterateScan";
  }

 protected:
  void doCompress(
      int64_t& elapseRead,
      int64_t& numBatches,
      int64_t& numRows,
      int64_t& compressTime,
      int64_t& decompressTime,
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
      std::vector<std::shared_ptr<arrow::ipc::IpcPayload>> payloads;
      std::vector<std::vector<int64_t>> uncompressedBufferSize;
      while (recordBatch) {
        numBatches += 1;
        uncompressedBufferSize.resize(numBatches);

        numRows += recordBatch->num_rows();
        for (auto i = 0; i < recordBatch->num_columns(); ++i) {
          recordBatch->column(i)->data()->buffers[0] = nullptr;
          for (auto& buffer : recordBatch->column(i)->data()->buffers) {
            if (buffer) {
              uncompressedBufferSize.back().push_back(buffer->size());
            } else {
              uncompressedBufferSize.back().push_back(-1);
            }
          }
        }
        auto payload = std::make_shared<arrow::ipc::IpcPayload>();

        TIME_NANO_OR_THROW(
            compressTime, arrow::ipc::GetRecordBatchPayload(*recordBatch, ipcWriteOptions, payload.get()));
        uncompressedSize += payload->raw_body_length;
        compressedSize += payload->body_length;
        TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
        payloads.push_back(std::move(payload));
      }

      decompress(ipcWriteOptions, payloads, uncompressedBufferSize, decompressTime);
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
  uint32_t compressBufferSize = 4096;

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
    } else if (strcmp(argv[i], "--qat-zstd") == 0) {
      std::cout << "QAT zstd is used as codec" << std::endl;
      codec = gluten::kQatZstd;
    } else if (strcmp(argv[i], "--qpl-gzip") == 0) {
      std::cout << "QPL gzip is used as codec" << std::endl;
      codec = gluten::kQplGzip;
    } else if (strcmp(argv[i], "--zstd") == 0) {
      std::cout << "CPU zstd is used as codec" << std::endl;
      codec = gluten::kZstd;
    } else if (strcmp(argv[i], "--buffer-size") == 0) {
      compressBufferSize = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--cpu-offset") == 0) {
      cpuOffset = atol(argv[i + 1]);
    }
  }
  std::cout << "iterations = " << iterations << std::endl;
  std::cout << "threads = " << threads << std::endl;
  std::cout << "datafile = " << datafile << std::endl;

  gluten::BenchmarkCompressionIterateScanBenchmark bmIterateScan(datafile, compressBufferSize);
  gluten::BenchmarkCompressionCacheScanBenchmark bmCacheScan(datafile, compressBufferSize);

  benchmark::RegisterBenchmark(bmIterateScan.name().c_str(), bmIterateScan)
      ->Iterations(iterations)
      ->Args({
          codec,
          compressBufferSize,
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
          compressBufferSize,
          cpuOffset,
      })
      ->Threads(threads)
      ->ReportAggregatesOnly(false)
      ->MeasureProcessCPUTime()
      ->Unit(benchmark::kSecond);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}
