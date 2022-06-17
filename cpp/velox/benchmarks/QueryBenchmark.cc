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

#include "BenchmarkUtils.h"
#include "compute/VeloxPlanConverter.h"
#include "jni/exec_backend.h"

auto BM = [](::benchmark::State& state, const std::string& datasetPath,
             const std::string& jsonFile, const std::string& fileFormat) {
  const auto& filePath = getExampleFilePath(jsonFile);
  auto maybePlan = readFromFile(filePath);
  if (!maybePlan.ok()) {
    state.SkipWithError(maybePlan.status().message().c_str());
  }
  auto plan = std::move(maybePlan).ValueOrDie();

  std::vector<std::string> paths;
  std::vector<u_int64_t> starts;
  std::vector<u_int64_t> lengths;
  getFileInfos(datasetPath, fileFormat, paths, starts, lengths);

  for (auto _ : state) {
    state.PauseTiming();
    auto backend = std::dynamic_pointer_cast<velox::compute::VeloxPlanConverter>(
        gluten::CreateBackend());
    state.ResumeTiming();
    backend->ParsePlan(plan->data(), plan->size());
    auto resultIter = backend->GetResultIterator(paths, starts, lengths, fileFormat);

    while (resultIter->HasNext()) {
      std::cout << resultIter->Next()->ToString() << std::endl;
    }
  }
};

int main(int argc, char** argv) {
  std::unique_ptr<memory::MemoryPool> veloxPool = memory::getDefaultScopedMemoryPool();
  InitVeloxBackend(veloxPool.get());
  ::benchmark::Initialize(&argc, argv);
  // Threads cannot work well, use ThreadRange instead.
  // The multi-thread performance is not correct.
  // BENCHMARK(BM)->ThreadRange(36, 36);

  // Register for TPC-H Q1 ORC tests.
  std::string lineitemOrcPath = getExampleFilePath("orc/bm_lineitem/");
  if (argc < 2) {
    ::benchmark::RegisterBenchmark("q1_first_stage_orc", BM, lineitemOrcPath,
                                   "q1_first_stage_orc.json", "orc");
  } else {
    ::benchmark::RegisterBenchmark("q1_first_stage_orc", BM, std::string(argv[1]) + "/",
                                   "q1_first_stage_orc.json", "orc");
  }

  // Register for TPC-H Q6 ORC tests.
  if (argc < 2) {
    ::benchmark::RegisterBenchmark("q6_first_stage_orc", BM, lineitemOrcPath,
                                   "q6_first_stage_orc.json", "orc");
  } else {
    ::benchmark::RegisterBenchmark("q6_first_stage_orc", BM, std::string(argv[1]) + "/",
                                   "q6_first_stage_orc.json", "orc");
  }

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}
