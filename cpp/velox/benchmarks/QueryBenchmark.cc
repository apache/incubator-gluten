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
#include <compute/VeloxExecutionCtx.h>

#include "BenchmarkUtils.h"
#include "compute/VeloxPlanConverter.h"
#include "memory/VeloxMemoryManager.h"
#include "utils/VeloxArrowUtils.h"

using namespace facebook;
using namespace gluten;

const std::string getFilePath(const std::string& fileName) {
  const std::string currentPath = std::filesystem::current_path().c_str();
  const std::string filePath = currentPath + "/../../../velox/benchmarks/data/" + fileName;
  return filePath;
}

// Used by unit test and benchmark.
std::shared_ptr<ResultIterator> getResultIterator(
    std::shared_ptr<velox::memory::MemoryPool> veloxPool,
    ExecutionCtx* executionCtx,
    const std::vector<std::shared_ptr<SplitInfo>>& setScanInfos,
    std::shared_ptr<const facebook::velox::core::PlanNode>& veloxPlan) {
  auto ctxPool = veloxPool->addAggregateChild(
      "query_benchmark_result_iterator", facebook::velox::memory::MemoryReclaimer::create());

  std::vector<std::shared_ptr<ResultIterator>> inputIter;
  std::unordered_map<std::string, std::string> sessionConf = {};
  auto veloxPlanConverter =
      std::make_unique<VeloxPlanConverter>(inputIter, defaultLeafVeloxMemoryPool().get(), sessionConf);
  veloxPlan = veloxPlanConverter->toVeloxPlan(executionCtx->getPlan());

  // In test, use setScanInfos to replace the one got from Substrait.
  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  std::vector<velox::core::PlanNodeId> scanIds;
  std::vector<velox::core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  VeloxExecutionCtx::getInfoAndIds(
      veloxPlanConverter->splitInfos(), veloxPlan->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
      ctxPool,
      veloxPlan,
      scanIds,
      setScanInfos,
      streamIds,
      "/tmp/test-spill",
      executionCtx->getConfMap(),
      executionCtx->getSparkTaskInfo());
  auto iter = std::make_shared<ResultIterator>(std::move(wholestageIter), executionCtx);
  auto handle = executionCtx->addResultIterator(iter);
  return executionCtx->getResultIterator(handle);
}

auto BM = [](::benchmark::State& state,
             const std::vector<std::string>& datasetPaths,
             const std::string& jsonFile,
             const std::string& fileFormat) {
  const auto& filePath = getFilePath("plan/" + jsonFile);
  auto plan = getPlanFromFile(filePath);

  auto memoryManager = getDefaultMemoryManager();
  auto executionCtx = gluten::createExecutionCtx();
  auto veloxPool = memoryManager->getAggregateMemoryPool();

  std::vector<std::shared_ptr<SplitInfo>> scanInfos;
  scanInfos.reserve(datasetPaths.size());
  for (const auto& datasetPath : datasetPaths) {
    if (std::filesystem::is_directory(datasetPath)) {
      scanInfos.emplace_back(getSplitInfos(datasetPath, fileFormat));
    } else {
      scanInfos.emplace_back(getSplitInfosFromFile(datasetPath, fileFormat));
    }
  }

  for (auto _ : state) {
    state.PauseTiming();
    state.ResumeTiming();

    executionCtx->parsePlan(reinterpret_cast<uint8_t*>(plan.data()), plan.size());
    std::shared_ptr<const facebook::velox::core::PlanNode> veloxPlan;
    auto resultIter = getResultIterator(veloxPool, executionCtx, scanInfos, veloxPlan);
    auto outputSchema = toArrowSchema(veloxPlan->outputType(), defaultLeafVeloxMemoryPool().get());
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
  gluten::releaseExecutionCtx(executionCtx);
};

#define orc_reader_decimal 1

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
  if (argc < 2) {
    auto lineitemOrcPath = getFilePath("bm_lineitem/orc/");
#if orc_reader_decimal == 0
    ::benchmark::RegisterBenchmark("select", BM, std::vector<std::string>{lineitemOrcPath}, "select.json", "orc");
#else
    auto fileName1 = lineitemOrcPath + "short_decimal_nonull.orc";
    ::benchmark::RegisterBenchmark(
        "select", BM, std::vector<std::string>{fileName1}, "select_short_decimal.json", "orc");
    auto fileName2 = lineitemOrcPath + "long_decimal_nonull.orc";
    ::benchmark::RegisterBenchmark(
        "select", BM, std::vector<std::string>{fileName2}, "select_long_decimal.json", "orc");
#endif
  } else {
    ::benchmark::RegisterBenchmark(
        "select", BM, std::vector<std::string>{std::string(argv[1]) + "/"}, "select.json", "orc");
  }
#endif

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}
