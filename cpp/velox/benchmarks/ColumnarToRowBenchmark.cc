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

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/io_util.h>
#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <chrono>

#include "memory/ArrowMemoryPool.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "operators/serializer/VeloxColumnarToRowConverter.h"
#include "utils/TestUtils.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/macros.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;
using namespace arrow;
namespace gluten {

const int kBatchBufferSize = 32768;

class GoogleBenchmarkColumnarToRow {
 public:
  GoogleBenchmarkColumnarToRow(std::string fileName) {
    getRecordBatchReader(fileName);
  }

  void getRecordBatchReader(const std::string& inputFile) {
    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;

    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::string fileName;
    ARROW_ASSIGN_OR_THROW(fs, arrow::fs::FileSystemFromUriOrPath(inputFile, &fileName))

    ARROW_ASSIGN_OR_THROW(file_, fs->OpenInputFile(fileName));

    properties_.set_batch_size(kBatchBufferSize);
    properties_.set_pre_buffer(false);
    properties_.set_use_threads(false);

    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file_), properties_, &parquetReader));

    ASSERT_NOT_OK(parquetReader->GetSchema(&schema_));

    auto numRowgroups = parquetReader->num_row_groups();

    for (int i = 0; i < numRowgroups; ++i) {
      rowGroupIndices_.push_back(i);
    }

    auto numColumns = schema_->num_fields();
    for (int i = 0; i < numColumns; ++i) {
      columnIndices_.push_back(i);
    }
  }

  virtual void operator()(benchmark::State& state) {}

 protected:
  long setCpu(uint32_t cpuindex) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpuindex, &cs);
    return sched_setaffinity(0, sizeof(cs), &cs);
  }

  velox::VectorPtr recordBatch2RowVector(const arrow::RecordBatch& rb) {
    ArrowArray arrowArray;
    ArrowSchema arrowSchema;
    ASSERT_NOT_OK(arrow::ExportRecordBatch(rb, &arrowArray, &arrowSchema));
    return velox::importFromArrowAsOwner(arrowSchema, arrowArray, gluten::defaultLeafVeloxMemoryPool().get());
  }

 protected:
  std::string fileName_;
  std::shared_ptr<arrow::io::RandomAccessFile> file_;
  std::vector<int> rowGroupIndices_;
  std::vector<int> columnIndices_;
  std::shared_ptr<arrow::Schema> schema_;
  parquet::ArrowReaderProperties properties_;
};
class GoogleBenchmarkColumnarToRowCacheScanBenchmark : public GoogleBenchmarkColumnarToRow {
 public:
  GoogleBenchmarkColumnarToRowCacheScanBenchmark(std::string filename) : GoogleBenchmarkColumnarToRow(filename) {}
  void operator()(benchmark::State& state) {
    if (state.range(0) == 0xffffffff) {
      setCpu(state.thread_index());
    } else {
      setCpu(state.range(0));
    }

    std::shared_ptr<arrow::RecordBatch> recordBatch;
    int64_t elapseRead = 0;
    int64_t numBatches = 0;
    int64_t numRows = 0;
    int64_t initTime = 0;
    int64_t writeTime = 0;

    std::vector<int> localColumnIndices = columnIndices_;

    std::shared_ptr<arrow::Schema> localSchema;
    localSchema = std::make_shared<arrow::Schema>(*schema_.get());

    if (state.thread_index() == 0)
      LOG(INFO) << localSchema->ToString();

    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        ::arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file_), properties_, &parquetReader));

    std::vector<velox::VectorPtr> vectors;
    ASSERT_NOT_OK(parquetReader->GetRecordBatchReader(rowGroupIndices_, localColumnIndices, &recordBatchReader));
    do {
      TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));

      if (recordBatch) {
        vectors.push_back(recordBatch2RowVector(*recordBatch));
        numBatches += 1;
        numRows += recordBatch->num_rows();
      }
    } while (recordBatch);

    LOG(INFO) << " parquet parse done elapsed time = " << elapseRead / 1000000 << " rows = " << numRows;

    // reuse the columnarToRowConverter for batches caused system % increase a lot
    auto ctxPool = defaultLeafVeloxMemoryPool();
    for (auto _ : state) {
      for (const auto& vector : vectors) {
        auto row = std::dynamic_pointer_cast<velox::RowVector>(vector);
        auto columnarToRowConverter = std::make_shared<gluten::VeloxColumnarToRowConverter>(ctxPool);
        auto cb = std::make_shared<VeloxColumnarBatch>(row);
        TIME_NANO_START(writeTime);
        columnarToRowConverter->convert(cb);
        TIME_NANO_END(writeTime);
      }
    }

    state.counters["rowgroups"] =
        benchmark::Counter(rowGroupIndices_.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["columns"] =
        benchmark::Counter(columnIndices_.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batches"] =
        benchmark::Counter(numBatches, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["num_rows"] =
        benchmark::Counter(numRows, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batch_buffer_size"] =
        benchmark::Counter(kBatchBufferSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["parquet_parse"] =
        benchmark::Counter(elapseRead, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["init_time"] =
        benchmark::Counter(initTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["write_time"] =
        benchmark::Counter(writeTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
  }
};

class GoogleBenchmarkColumnarToRowIterateScanBenchmark : public GoogleBenchmarkColumnarToRow {
 public:
  GoogleBenchmarkColumnarToRowIterateScanBenchmark(std::string filename) : GoogleBenchmarkColumnarToRow(filename) {}
  void operator()(benchmark::State& state) {
    setCpu(state.thread_index());

    int64_t elapseRead = 0;
    int64_t numBatches = 0;
    int64_t numRows = 0;
    int64_t initTime = 0;
    int64_t writeTime = 0;

    std::shared_ptr<arrow::RecordBatch> recordBatch;

    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    ASSERT_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::Open(file_), properties_, &parquetReader));

    auto ctxPool = defaultLeafVeloxMemoryPool();
    for (auto _ : state) {
      ASSERT_NOT_OK(parquetReader->GetRecordBatchReader(rowGroupIndices_, columnIndices_, &recordBatchReader));
      TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
      while (recordBatch) {
        numBatches += 1;
        numRows += recordBatch->num_rows();
        auto vector = recordBatch2RowVector(*recordBatch);
        auto columnarToRowConverter = std::make_shared<gluten::VeloxColumnarToRowConverter>(ctxPool);
        auto row = std::dynamic_pointer_cast<velox::RowVector>(vector);
        auto cb = std::make_shared<VeloxColumnarBatch>(row);
        TIME_NANO_START(writeTime);
        columnarToRowConverter->convert(cb);
        TIME_NANO_END(writeTime);
        TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
      }
    }

    state.counters["rowgroups"] =
        benchmark::Counter(rowGroupIndices_.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["columns"] =
        benchmark::Counter(columnIndices_.size(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batches"] =
        benchmark::Counter(numBatches, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["num_rows"] =
        benchmark::Counter(numRows, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batch_buffer_size"] =
        benchmark::Counter(kBatchBufferSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["parquet_parse"] =
        benchmark::Counter(elapseRead, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["init_time"] =
        benchmark::Counter(initTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["write_time"] =
        benchmark::Counter(writeTime, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
  }
};

} // namespace gluten

// usage
// ./columnar_to_row_benchmark --threads=1 --file /mnt/DP_disk1/int.parquet
int main(int argc, char** argv) {
  uint32_t iterations = 1;
  uint32_t threads = 1;
  std::string datafile;
  uint32_t cpu = 0xffffffff;

  for (int i = 0; i < argc; i++) {
    if (strcmp(argv[i], "--iterations") == 0) {
      iterations = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--threads") == 0) {
      threads = atol(argv[i + 1]);
    } else if (strcmp(argv[i], "--file") == 0) {
      datafile = argv[i + 1];
    } else if (strcmp(argv[i], "--cpu") == 0) {
      cpu = atol(argv[i + 1]);
    }
  }
  LOG(INFO) << "iterations = " << iterations;
  LOG(INFO) << "threads = " << threads;
  LOG(INFO) << "datafile = " << datafile;
  LOG(INFO) << "cpu = " << cpu;

  gluten::GoogleBenchmarkColumnarToRowCacheScanBenchmark bck(datafile);

  benchmark::RegisterBenchmark("GoogleBenchmarkColumnarToRow::CacheScan", bck)
      ->Args({
          cpu,
      })
      ->Iterations(iterations)
      ->Threads(threads)
      ->ReportAggregatesOnly(false)
      ->MeasureProcessCPUTime()
      ->Unit(benchmark::kSecond);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}
