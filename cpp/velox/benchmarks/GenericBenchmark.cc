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

#include <arrow/c/bridge.h>
#include <arrow/util/range.h>
#include <benchmark/benchmark.h>
#include <gflags/gflags.h>
#include <operators/writer/ArrowWriter.h>
#include <velox/exec/PlanNodeStats.h>
#include <velox/exec/Task.h>

#include <chrono>

#include "BatchStreamIterator.h"
#include "BatchVectorIterator.h"
#include "BenchmarkUtils.h"
#include "compute/VeloxBackend.h"
#include "utils/exception.h"

#include <thread>

using namespace gluten;

auto BM_Generic = [](::benchmark::State& state,
                     const std::string& substraitJsonFile,
                     const std::vector<std::string>& input_files,
                     GetInputFunc* getInputIterator) {
  if (FLAGS_cpu != -1) {
    setCpu(FLAGS_cpu);
  } else {
    setCpu(state.thread_index());
  }
  const auto& filePath = getExampleFilePath(substraitJsonFile);
  auto maybePlan = getPlanFromFile(filePath);
  if (!maybePlan.ok()) {
    state.SkipWithError(maybePlan.status().message().c_str());
    return;
  }
  auto plan = std::move(maybePlan).ValueOrDie();

  auto startTime = std::chrono::steady_clock::now();
  int64_t collectBatchTime = 0;

  for (auto _ : state) {
    auto backend = gluten::CreateBackend();
    std::vector<std::shared_ptr<gluten::ResultIterator>> inputIters;
    std::transform(input_files.cbegin(), input_files.cend(), std::back_inserter(inputIters), getInputIterator);
    std::vector<BatchIterator*> inputItersRaw;
    std::transform(
        inputIters.begin(),
        inputIters.end(),
        std::back_inserter(inputItersRaw),
        [](std::shared_ptr<gluten::ResultIterator> iter) { return static_cast<BatchIterator*>(iter->GetRaw()); });

    backend->ParsePlan(plan->data(), plan->size());
    auto resultIter =
        backend->GetResultIterator(gluten::DefaultMemoryAllocator().get(), "/tmp", std::move(inputIters), {});
    auto outputSchema = backend->GetOutputSchema();
    ArrowWriter writer{FLAGS_write_file};
    state.PauseTiming();
    if (!FLAGS_write_file.empty()) {
      GLUTEN_THROW_NOT_OK(writer.initWriter(*(outputSchema.get())));
    }
    state.ResumeTiming();
    while (resultIter->HasNext()) {
      auto array = resultIter->Next()->exportArrowArray();
      state.PauseTiming();
      auto maybeBatch = arrow::ImportRecordBatch(array.get(), outputSchema);
      if (!maybeBatch.ok()) {
        state.SkipWithError(maybeBatch.status().message().c_str());
        return;
      }
      if (FLAGS_print_result) {
        std::cout << maybeBatch.ValueOrDie()->ToString() << std::endl;
      }
      if (!FLAGS_write_file.empty()) {
        GLUTEN_THROW_NOT_OK(writer.writeInBatches(maybeBatch.ValueOrDie()));
      }
      state.ResumeTiming();
    }
    state.PauseTiming();
    if (!FLAGS_write_file.empty()) {
      GLUTEN_THROW_NOT_OK(writer.closeWriter());
    }
    state.ResumeTiming();

    collectBatchTime +=
        std::accumulate(inputItersRaw.begin(), inputItersRaw.end(), 0, [](int64_t sum, BatchIterator* iter) {
          return sum + iter->GetCollectBatchTime();
        });

    auto* rawIter = static_cast<gluten::WholeStageResultIterator*>(resultIter->GetRaw());
    const auto& task = rawIter->task_;
    const auto& planNode = rawIter->veloxPlan_;
    auto statsStr = facebook::velox::exec::printPlanWithStats(*planNode, task->taskStats(), true);
    std::cout << statsStr << std::endl;
  }

  auto endTime = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();

  state.counters["collect_batch_time"] =
      benchmark::Counter(collectBatchTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
  state.counters["elapsed_time"] =
      benchmark::Counter(duration, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
};

arrow::Status CreateOrcFile(const std::string& parquetFile, const std::string& orcFile) {
  ParquetBatchStreamIterator parquetIterator(parquetFile);

  std::shared_ptr<arrow::io::FileOutputStream> outputStream;

  ARROW_ASSIGN_OR_RAISE(outputStream, arrow::io::FileOutputStream::Open(orcFile));

  auto writerOptions = arrow::adapters::orc::WriteOptions();
  auto maybeWriter = arrow::adapters::orc::ORCFileWriter::Open(outputStream.get(), writerOptions);
  GLUTEN_THROW_NOT_OK(maybeWriter);
  auto& writer = *maybeWriter;

  while (true) {
    // 1. read from Parquet
    auto columnarBatch = parquetIterator.Next();
    GLUTEN_THROW_NOT_OK(columnarBatch);

    auto& cb = *columnarBatch;
    if (!cb) {
      break;
    }

    auto arrowColumnarBatch = std::dynamic_pointer_cast<gluten::ArrowColumnarBatch>(cb);
    auto recordBatch = arrowColumnarBatch->GetRecordBatch();

    // 2. write to Orc
    if (!(writer->Write(*recordBatch)).ok()) {
      return arrow::Status::IOError("Write failed");
    }
  }

  if (!(writer->Close()).ok()) {
    return arrow::Status::IOError("Close failed");
  }

  return arrow::Status::OK();
}

std::vector<std::string> outputFiles = {"example_orders.orc", "example_lineitem.orc"};

void OrcTestBegin() {
  std::vector<std::string> inputFiles(2);
  GLUTEN_ASSIGN_OR_THROW(inputFiles[0], getGeneratedFilePath("example_orders"));
  GLUTEN_ASSIGN_OR_THROW(inputFiles[1], getGeneratedFilePath("example_lineitem"));

  for (auto i = 0; i != inputFiles.size(); ++i) {
    GLUTEN_THROW_NOT_OK(CreateOrcFile(inputFiles[i], outputFiles[i]));
  }
}

void OrcTestEnd() {
  for (auto& x : outputFiles) {
    std::filesystem::remove(x);
  }
}

int main(int argc, char** argv) {
  InitVeloxBackend();
  ::benchmark::Initialize(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::string substraitJsonFile;
  std::vector<std::string> inputFiles;
  try {
    if (argc < 2) {
      std::cout << "No input args. Usage: " << std::endl
                << "./generic_benchmark /path/to/substrait_json_file /path/to/data_file_1 /path/to/data_file_2 ..."
                << std::endl;
      std::cout << "Running example..." << std::endl;
      inputFiles.resize(2);
      GLUTEN_ASSIGN_OR_THROW(substraitJsonFile, getGeneratedFilePath("example.json"));
      GLUTEN_ASSIGN_OR_THROW(inputFiles[0], getGeneratedFilePath("example_orders"));
      GLUTEN_ASSIGN_OR_THROW(inputFiles[1], getGeneratedFilePath("example_lineitem"));
    } else {
      substraitJsonFile = argv[1];
      AbortIfFileNotExists(substraitJsonFile);
      std::cout << "Using substrait json file: " << std::endl << substraitJsonFile << std::endl;
      std::cout << "Using " << argc - 2 << " input data file(s): " << std::endl;
      for (auto i = 2; i < argc; ++i) {
        inputFiles.emplace_back(argv[i]);
        AbortIfFileNotExists(inputFiles.back());
        std::cout << inputFiles.back() << std::endl;
      }
    }
  } catch (const std::exception& e) {
    std::cout << "Failed to run benchmark: " << e.what() << std::endl;
    ::benchmark::Shutdown();
    std::exit(EXIT_FAILURE);
  }

#define GENERIC_BENCHMARK(NAME, FUNC)                                                                \
  do {                                                                                               \
    auto* bm = ::benchmark::RegisterBenchmark(NAME, BM_Generic, substraitJsonFile, inputFiles, FUNC) \
                   ->MeasureProcessCPUTime()                                                         \
                   ->UseRealTime();                                                                  \
    if (FLAGS_threads > 0) {                                                                         \
      bm->Threads(FLAGS_threads);                                                                    \
    } else {                                                                                         \
      bm->ThreadRange(1, std::thread::hardware_concurrency());                                       \
    }                                                                                                \
    if (FLAGS_iterations > 0) {                                                                      \
      bm->Iterations(FLAGS_iterations);                                                              \
    }                                                                                                \
  } while (0)

#if 0
  std::cout << "FLAGS_threads:" << FLAGS_threads << std::endl;
  std::cout << "FLAGS_iterations:" << FLAGS_iterations << std::endl;
  std::cout << "FLAGS_cpu:" << FLAGS_cpu << std::endl;
  std::cout << "FLAGS_print_result:" << FLAGS_print_result << std::endl;
  std::cout << "FLAGS_write_file:" << FLAGS_write_file << std::endl;
#endif

  GENERIC_BENCHMARK("ParquetInputFromBatchVector", getParquetInputFromBatchVector);
  GENERIC_BENCHMARK("ParquetInputFromBatchStream", getParquetInputFromBatchStream);

  OrcTestBegin();

  inputFiles = outputFiles;

  GENERIC_BENCHMARK("OrcInputFromBatchVector", getOrcInputFromBatchVector);
  GENERIC_BENCHMARK("OrcInputFromBatchStream", getOrcInputFromBatchStream);

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  OrcTestEnd();

  return 0;
}
