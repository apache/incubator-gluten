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

#include "BenchmarkUtils.h"
#include "compute/VeloxPlanConverter.h"
#include "jni/exec_backend.h"
#include "utils/exception.h"

using GetInputFunc =
    std::shared_ptr<gluten::RecordBatchResultIterator>(const std::string&);

auto BM_Generic = [](::benchmark::State& state, const std::string& substraitJsonFile,
                     const std::vector<std::string>& input_files,
                     GetInputFunc* getInputIterator) {
  SetCPU(2);

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
      std::cout << resultIter->Next()->ToString() << std::endl;
    }
  }
};

int main(int argc, char** argv) {
  std::unique_ptr<facebook::velox::memory::MemoryPool> veloxPool =
      facebook::velox::memory::getDefaultScopedMemoryPool();
  InitVeloxBackend(veloxPool.get());
  ::benchmark::Initialize(&argc, argv);

  std::string substraitJsonFile = argv[1];
  std::vector<std::string> inputFiles;
  for (auto i = 2; i < argc; ++i) {
    inputFiles.emplace_back(argv[i]);
  }

  ::benchmark::RegisterBenchmark("InputFromBatchVector", BM_Generic, substraitJsonFile,
                                 inputFiles, getInputFromBatchVector);

  ::benchmark::RegisterBenchmark("InputFromBatchStream", BM_Generic, substraitJsonFile,
                                 inputFiles, getInputFromBatchStream);

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}
