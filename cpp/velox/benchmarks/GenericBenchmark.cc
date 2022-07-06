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

#include <arrow/util/range.h>
#include <benchmark/benchmark.h>
#include <gflags/gflags.h>

#include "BenchmarkUtils.h"
#include "compute/VeloxPlanConverter.h"
#include "jni/exec_backend.h"
#include "utils/exception.h"

DEFINE_bool(print_result, true, "Print result for execution");
DEFINE_int32(cpu, -1, "Run benchmark on specific CPU");
DEFINE_int32(threads, 1, "The number of threads to run this benchmark");

using GetInputFunc =
    std::shared_ptr<gluten::RecordBatchResultIterator>(const std::string&);

auto BM_Generic = [](::benchmark::State& state, const std::string& substraitJsonFile,
                     const std::vector<std::string>& input_files,
                     GetInputFunc* getInputIterator) {
  if (FLAGS_cpu != -1) {
    setCpu(FLAGS_cpu);
  }
  const auto& filePath = getExampleFilePath(substraitJsonFile);
  auto maybePlan = getPlanFromFile(filePath);
  if (!maybePlan.ok()) {
    state.SkipWithError(maybePlan.status().message().c_str());
    return;
  }
  auto plan = std::move(maybePlan).ValueOrDie();

  for (auto _ : state) {
    state.PauseTiming();
    auto backend = gluten::CreateBackend();
    std::vector<std::shared_ptr<gluten::RecordBatchResultIterator>> inputIters;
    std::transform(input_files.cbegin(), input_files.cend(),
                   std::back_inserter(inputIters), getInputIterator);

    state.ResumeTiming();
    backend->ParsePlan(plan->data(), plan->size());
    auto resultIter = backend->GetResultIterator(std::move(inputIters));

    while (resultIter->HasNext()) {
      auto batch = resultIter->Next();
      if (FLAGS_print_result) {
        state.PauseTiming();
        std::cout << batch->ToString() << std::endl;
        state.ResumeTiming();
      }
    }

    auto* rawIter = static_cast<velox::compute::WholeStageResIter*>(resultIter->GetRaw());
    const auto& task = rawIter->task_;
    auto taskStats = task->taskStats();
    for (const auto& pStat : taskStats.pipelineStats) {
      for (const auto& opStat : pStat.operatorStats) {
        if (opStat.operatorType != "N/A") {
          // ${pipelineId}_${operatorId}_${planNodeId}_${operatorType}_${metric}
          // Different operators may have same planNodeId, e.g. HashBuild and HashProbe
          // from same HashNode.
          const auto& opId = std::to_string(opStat.pipelineId) + "_" +
                             std::to_string(opStat.operatorId) + "_" + opStat.planNodeId +
                             "_" + opStat.operatorType;
          state.counters[opId + "_addInputTiming"] = benchmark::Counter(
              opStat.addInputTiming.cpuNanos, benchmark::Counter::Flags::kAvgIterations);
          state.counters[opId + "_getOutputTiming"] = benchmark::Counter(
              opStat.getOutputTiming.cpuNanos, benchmark::Counter::Flags::kAvgIterations);
          state.counters[opId + "_finishTiming"] = benchmark::Counter(
              opStat.finishTiming.cpuNanos, benchmark::Counter::Flags::kAvgIterations);
        }
      }
    }
  }
};

int main(int argc, char** argv) {
  std::unique_ptr<facebook::velox::memory::MemoryPool> veloxPool =
      facebook::velox::memory::getDefaultScopedMemoryPool();
  InitVeloxBackend(veloxPool.get());
  ::benchmark::Initialize(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::string substraitJsonFile = argv[1];
  std::vector<std::string> inputFiles;
  for (auto i = 2; i < argc; ++i) {
    inputFiles.emplace_back(argv[i]);
  }

#define GENERIC_BENCHMARK(NAME, FUNC)                                                   \
  ::benchmark::RegisterBenchmark(NAME, BM_Generic, substraitJsonFile, inputFiles, FUNC) \
      ->Threads(FLAGS_threads)                                                          \
      ->MeasureProcessCPUTime()                                                         \
      ->UseRealTime();

  GENERIC_BENCHMARK("InputFromBatchVector", getInputFromBatchVector);
  GENERIC_BENCHMARK("InputFromBatchStream", getInputFromBatchStream);

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}
