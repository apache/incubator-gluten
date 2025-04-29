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

#include <benchmark/benchmark.h>

#include "benchmarks/common/BenchmarkUtils.h"
#include "memory/VeloxMemoryManager.h"
#include "operators/reader/FileReaderIterator.h"
#include "operators/serializer/VeloxColumnarToRowConverter.h"

namespace gluten {

const int kBatchBufferSize = 4096;

class GoogleBenchmarkColumnarToRowCacheScanBenchmark {
 public:
  GoogleBenchmarkColumnarToRowCacheScanBenchmark(const std::string& fileName) : fileName_(fileName) {}

  void operator()(benchmark::State& state) {
    if (state.range(0) == 0xffffffff) {
      setCpu(state.thread_index());
    } else {
      setCpu(state.range(0));
    }

    int64_t elapseRead = 0;
    int64_t numBatches = 0;
    int64_t numRows = 0;
    int64_t numColumns = 0;
    int64_t initTime = 0;
    int64_t writeTime = 0;

    auto inputIter = [&] {
      ScopedTimer timer(&elapseRead);
      return FileReaderIterator::getInputIteratorFromFileReader(
          FileReaderType::kBuffered, fileName_, kBatchBufferSize, defaultLeafVeloxMemoryPool().get());
    }();

    while (auto batch = inputIter->next()) {
      numBatches += 1;
      numRows += batch->numRows();
      numColumns = batch->numColumns();

      ScopedTimer timer(&writeTime);

      auto converter = std::make_shared<VeloxColumnarToRowConverter>(defaultLeafVeloxMemoryPool(), 64 << 20);
      converter->convert(batch);
    }

    state.counters["columns"] =
        benchmark::Counter(numColumns, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
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

 private:
  std::string fileName_;
};

class GoogleBenchmarkColumnarToRowIterateScanBenchmark {
 public:
  GoogleBenchmarkColumnarToRowIterateScanBenchmark(const std::string& fileName) : fileName_(fileName) {}

  void operator()(benchmark::State& state) {
    setCpu(state.thread_index());

    int64_t elapseRead = 0;
    int64_t numBatches = 0;
    int64_t numRows = 0;
    int64_t numColumns = 0;
    int64_t initTime = 0;
    int64_t writeTime = 0;

    for (auto _ : state) {
      auto inputIter = FileReaderIterator::getInputIteratorFromFileReader(
          FileReaderType::kStream, fileName_, kBatchBufferSize, defaultLeafVeloxMemoryPool().get());

      auto nextBatch = [&] {
        ScopedTimer timer(&elapseRead);
        return inputIter->next();
      };

      while (auto batch = nextBatch()) {
        numBatches += 1;
        numRows += batch->numRows();
        numColumns = batch->numColumns();

        ScopedTimer timer(&writeTime);

        auto converter = std::make_shared<VeloxColumnarToRowConverter>(defaultLeafVeloxMemoryPool(), 64 << 20);
        converter->convert(batch);
      }
    }

    state.counters["columns"] =
        benchmark::Counter(numColumns, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
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

 private:
  std::string fileName_;
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

  gluten::initVeloxBackend();
  memory::MemoryManager::testingSetInstance({});

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
