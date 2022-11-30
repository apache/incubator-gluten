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
#include <velox/exec/PlanNodeStats.h>

#include <chrono>

#include "BenchmarkUtils.h"
#include "compute/VeloxPlanConverter.h"
#include "compute/exec_backend.h"
#include "utils/exception.h"

#include <thread>

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
    std::vector<std::shared_ptr<gluten::GlutenResultIterator>> inputIters;
    std::transform(input_files.cbegin(), input_files.cend(), std::back_inserter(inputIters), getInputIterator);
    std::vector<BatchIteratorWrapper*> inputItersRaw;
    std::transform(
        inputIters.begin(),
        inputIters.end(),
        std::back_inserter(inputItersRaw),
        [](std::shared_ptr<gluten::GlutenResultIterator> iter) {
          return static_cast<BatchIteratorWrapper*>(iter->GetRaw());
        });

    backend->ParsePlan(plan->data(), plan->size());
    auto resultIter = backend->GetResultIterator(gluten::DefaultMemoryAllocator().get(), std::move(inputIters));
    auto outputSchema = backend->GetOutputSchema();

    while (resultIter->HasNext()) {
      auto array = resultIter->Next()->exportArrowArray();
      if (FLAGS_print_result) {
        state.PauseTiming();
        auto maybeBatch = arrow::ImportRecordBatch(array.get(), outputSchema);
        if (!maybeBatch.ok()) {
          state.SkipWithError(maybeBatch.status().message().c_str());
          return;
        }
        std::cout << maybeBatch.ValueOrDie()->ToString() << std::endl;
        state.ResumeTiming();
      }
    }

    collectBatchTime +=
        std::accumulate(inputItersRaw.begin(), inputItersRaw.end(), 0, [](int64_t sum, BatchIteratorWrapper* iter) {
          return sum + iter->GetCollectBatchTime();
        });

    auto* rawIter = static_cast<gluten::WholeStageResIter*>(resultIter->GetRaw());
    const auto& task = rawIter->task_;
    const auto& planNode = rawIter->planNode_;
    auto statsStr = ::facebook::velox::exec::printPlanWithStats(*planNode, task->taskStats(), true);
    std::cout << statsStr << std::endl;
  }

  auto endTime = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();

  state.counters["collect_batch_time"] =
      benchmark::Counter(collectBatchTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
  state.counters["elapsed_time"] =
      benchmark::Counter(duration, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
};

int main(int argc, char** argv) {
  InitVeloxBackend();
  ::benchmark::Initialize(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::string substraitJsonFile;
  std::vector<std::string> inputFiles;
  if (argc < 2) {
    std::cout << "No input args. Running example..." << std::endl;
    inputFiles.resize(2);
    try {
      GLUTEN_ASSIGN_OR_THROW(substraitJsonFile, getGeneratedFilePath("example.json"));
      GLUTEN_ASSIGN_OR_THROW(inputFiles[0], getGeneratedFilePath("example_lineitem"));
      GLUTEN_ASSIGN_OR_THROW(inputFiles[1], getGeneratedFilePath("example_orders"));
    } catch (const std::exception& e) {
      std::cout << "Failed to run example: " << e.what() << std::endl;
      ::benchmark::Shutdown();
      std::exit(EXIT_FAILURE);
    }
  } else {
    substraitJsonFile = argv[1];
    for (auto i = 2; i < argc; ++i) {
      inputFiles.emplace_back(argv[i]);
    }
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

  GENERIC_BENCHMARK("InputFromBatchVector", getInputFromBatchVector);
  GENERIC_BENCHMARK("InputFromBatchStream", getInputFromBatchStream);

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}
