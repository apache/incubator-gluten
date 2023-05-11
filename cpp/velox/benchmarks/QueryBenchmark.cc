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
#include "compute/ArrowTypeUtils.h"
#include "compute/VeloxBackend.h"
#include "compute/VeloxPlanConverter.h"

using namespace facebook;
using namespace gluten;

const std::string getFilePath(const std::string& fileName) {
  const std::string currentPath = std::filesystem::current_path().c_str();
  const std::string filePath = currentPath + "/../../../velox/benchmarks/data/" + fileName;
  return filePath;
}

// Used by unit test and benchmark.
std::shared_ptr<ResultIterator> getResultIterator(
    MemoryAllocator* allocator,
    std::shared_ptr<Backend> backend,
    const std::vector<std::shared_ptr<velox::substrait::SplitInfo>>& setScanInfos) {
  auto ctxPool = getDefaultVeloxLeafMemoryPool()->addAggregateChild("query_benchmark_result_iterator");
  auto resultPool = ctxPool->addLeafChild("query_benchmark_result_vector");
  std::vector<std::shared_ptr<ResultIterator>> inputIter;
  auto veloxPlanConverter = std::make_unique<VeloxPlanConverter>(inputIter, ctxPool);
  auto veloxPlan = veloxPlanConverter->toVeloxPlan(backend->getPlan());

  // In test, use setScanInfos to replace the one got from Substrait.
  std::vector<std::shared_ptr<velox::substrait::SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  VeloxBackend::getInfoAndIds(
      veloxPlanConverter->splitInfos(), veloxPlan->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
      ctxPool, resultPool, veloxPlan, scanIds, setScanInfos, streamIds, "/tmp/test-spill", backend->getConfMap());
  return std::make_shared<ResultIterator>(std::move(wholestageIter), backend);
}

auto bm = [](::benchmark::State& state,
             const std::vector<std::string>& datasetPaths,
             const std::string& jsonFile,
             const std::string& fileFormat) {
  const auto& filePath = getFilePath("plan/" + jsonFile);
  auto plan = getPlanFromFile(filePath);

  std::vector<std::shared_ptr<velox::substrait::SplitInfo>> scanInfos;
  scanInfos.reserve(datasetPaths.size());
  for (const auto& datasetPath : datasetPaths) {
    scanInfos.emplace_back(getSplitInfos(datasetPath, fileFormat));
  }

  for (auto _ : state) {
    state.PauseTiming();
    auto backend = std::dynamic_pointer_cast<gluten::VeloxBackend>(gluten::createBackend());
    state.ResumeTiming();

    backend->parsePlan(reinterpret_cast<uint8_t*>(plan.data()), plan.size());
    auto resultIter = getResultIterator(gluten::defaultMemoryAllocator().get(), backend, scanInfos);
    auto veloxPlan = std::dynamic_pointer_cast<gluten::VeloxBackend>(backend)->getVeloxPlan();
    auto outputSchema = toArrowSchema(veloxPlan->outputType());
    while (resultIter->hasNext()) {
      auto array = resultIter->next()->exportArrowArray();
      auto maybeBatch = arrow::ImportRecordBatch(array.get(), outputSchema);
      if (!maybeBatch.ok()) {
        state.SkipWithError(maybeBatch.status().message().c_str());
        return;
      }
      std::cout << maybeBatch.ValueOrDie()->ToString() << std::endl;
    }
  }
};

int main(int argc, char** argv) {
  initVeloxBackend();
  ::benchmark::Initialize(&argc, argv);
  // Threads cannot work well, use ThreadRange instead.
  // The multi-thread performance is not correct.
  // BENCHMARK(BM)->ThreadRange(36, 36);

#if 0
  const auto& lineitemParquetPath = getFilePath("bm_lineitem/parquet/");
  if (argc < 2) {
    ::benchmark::RegisterBenchmark(
        "select", BM, std::vector<std::string>{lineitemParquetPath}, "select.json", "parquet");
  } else {
    ::benchmark::RegisterBenchmark(
        "select", BM, std::vector<std::string>{std::string(argv[1]) + "/"}, "select.json", "parquet");
  }
#else
  // For ORC debug.
  const auto& lineitemOrcPath = getFilePath("bm_lineitem/orc/");
  if (argc < 2) {
    ::benchmark::RegisterBenchmark("select", bm, std::vector<std::string>{lineitemOrcPath}, "select.json", "orc");
  } else {
    ::benchmark::RegisterBenchmark(
        "select", bm, std::vector<std::string>{std::string(argv[1]) + "/"}, "select.json", "orc");
  }
#endif

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}
