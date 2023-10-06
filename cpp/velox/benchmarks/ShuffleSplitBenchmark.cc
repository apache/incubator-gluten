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

#include <chrono>

#include "benchmarks/BenchmarkUtils.h"
#include "memory/ColumnarBatch.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "utils/TestUtils.h"
#include "utils/VeloxArrowUtils.h"
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
using gluten::VeloxShuffleWriter;

DEFINE_int32(partitions, -1, "Shuffle partitions");
DEFINE_string(file, "", "Input file to split");

namespace gluten {

const int kBatchBufferSize = 4096;
const int kPartitionBufferSize = 4096;

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
    if (FLAGS_cpu != -1) {
      setCpu(FLAGS_cpu + state.thread_index());
    } else {
      setCpu(state.thread_index());
    }

    std::shared_ptr<arrow::MemoryPool> pool = defaultArrowMemoryPool();

    std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator =
        std::make_shared<LocalPartitionWriterCreator>();

    auto options = ShuffleWriterOptions::defaults();
    options.buffer_size = kPartitionBufferSize;
    options.buffered_write = true;
    options.memory_pool = pool.get();
    options.partitioning_name = "rr";

    std::shared_ptr<VeloxShuffleWriter> shuffleWriter;
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
        FLAGS_partitions,
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
        benchmark::Counter(FLAGS_partitions, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1000);
    state.counters["batch_buffer_size"] =
        benchmark::Counter(kBatchBufferSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["split_buffer_size"] =
        benchmark::Counter(kPartitionBufferSize, benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

    state.counters["bytes_spilled"] = benchmark::Counter(
        shuffleWriter->totalBytesEvicted(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_written"] = benchmark::Counter(
        shuffleWriter->totalBytesWritten(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);
    state.counters["bytes_raw"] = benchmark::Counter(
        shuffleWriter->rawPartitionBytes(), benchmark::Counter::kAvgThreads, benchmark::Counter::OneK::kIs1024);

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
      std::shared_ptr<VeloxShuffleWriter>& shuffleWriter,
      int64_t& elapseRead,
      int64_t& numBatches,
      int64_t& numRows,
      int64_t& splitTime,
      const int numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      ShuffleWriterOptions options,
      benchmark::State& state) {}

 protected:
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
      std::shared_ptr<VeloxShuffleWriter>& shuffleWriter,
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

    auto pool = options.memory_pool;
    GLUTEN_ASSIGN_OR_THROW(
        shuffleWriter,
        VeloxShuffleWriter::create(numPartitions, partitionWriterCreator, options, defaultLeafVeloxMemoryPool()));

    std::shared_ptr<arrow::RecordBatch> recordBatch;

    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        pool, ::parquet::ParquetFileReader::Open(file_), properties_, &parquetReader));

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
          batches.cbegin(),
          batches.cend(),
          [&shuffleWriter, &splitTime](const std::shared_ptr<arrow::RecordBatch>& recordBatch) {
            std::shared_ptr<ColumnarBatch> cb;
            ARROW_ASSIGN_OR_THROW(cb, recordBatch2VeloxColumnarBatch(*recordBatch));
            TIME_NANO_OR_THROW(splitTime, shuffleWriter->split(cb, ShuffleWriter::kMinMemLimit));
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
      std::shared_ptr<VeloxShuffleWriter>& shuffleWriter,
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
        VeloxShuffleWriter::create(
            numPartitions, std::move(partitionWriterCreator), std::move(options), defaultLeafVeloxMemoryPool()));

    std::shared_ptr<arrow::RecordBatch> recordBatch;

    std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
    std::shared_ptr<RecordBatchReader> recordBatchReader;
    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        options.memory_pool, ::parquet::ParquetFileReader::Open(file_), properties_, &parquetReader));

    for (auto _ : state) {
      std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
      GLUTEN_THROW_NOT_OK(parquetReader->GetRecordBatchReader(rowGroupIndices_, columnIndices_, &recordBatchReader));
      TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
      while (recordBatch) {
        numBatches += 1;
        numRows += recordBatch->num_rows();
        std::shared_ptr<ColumnarBatch> cb;
        ARROW_ASSIGN_OR_THROW(cb, recordBatch2VeloxColumnarBatch(*recordBatch));
        TIME_NANO_OR_THROW(splitTime, shuffleWriter->split(cb, ShuffleWriter::kMinMemLimit));
        TIME_NANO_OR_THROW(elapseRead, recordBatchReader->ReadNext(&recordBatch));
      }
    }
    TIME_NANO_OR_THROW(splitTime, shuffleWriter->stop());
  }
};

} // namespace gluten

int main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_file.size() == 0) {
    std::cerr << "No input data file. Please specify via argument --file" << std::endl;
  }

  if (FLAGS_partitions == -1) {
    FLAGS_partitions = std::thread::hardware_concurrency();
  }

  gluten::BenchmarkShuffleSplitIterateScanBenchmark iterateScanBenchmark(FLAGS_file);

  auto bm = benchmark::RegisterBenchmark("BenchmarkShuffleSplit::IterateScan", iterateScanBenchmark)
                ->ReportAggregatesOnly(false)
                ->MeasureProcessCPUTime()
                ->Unit(benchmark::kSecond);

  if (FLAGS_threads > 0) {
    bm->Threads(FLAGS_threads);
  } else {
    bm->ThreadRange(1, std::thread::hardware_concurrency());
  }
  if (FLAGS_iterations > 0) {
    bm->Iterations(FLAGS_iterations);
  }

  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
}
