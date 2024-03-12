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

#include <chrono>
#include <thread>

#include <arrow/c/bridge.h>
#include <arrow/util/range.h>
#include <benchmark/benchmark.h>
#include <gflags/gflags.h>
#include <operators/writer/ArrowWriter.h>

#include "benchmarks/common/BenchmarkUtils.h"
#include "benchmarks/common/FileReaderIterator.h"
#include "compute/VeloxPlanConverter.h"
#include "compute/VeloxRuntime.h"
#include "config/GlutenConfig.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "shuffle/rss/CelebornPartitionWriter.h"
#include "utils/StringUtil.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/exception.h"
#include "utils/tests/LocalRssClient.h"
#include "velox/exec/PlanNodeStats.h"

using namespace gluten;

namespace {
DEFINE_bool(with_shuffle, false, "Add shuffle split at end.");
DEFINE_string(partitioning, "rr", "Short partitioning name. Valid options are rr, hash, range, single");
DEFINE_bool(celeborn, false, "Mocking celeborn shuffle.");
DEFINE_bool(zstd, false, "Use ZSTD as shuffle compression codec");
DEFINE_bool(qat_gzip, false, "Use QAT GZIP as shuffle compression codec");
DEFINE_bool(qat_zstd, false, "Use QAT ZSTD as shuffle compression codec");
DEFINE_bool(iaa_gzip, false, "Use IAA GZIP as shuffle compression codec");
DEFINE_int32(shuffle_partitions, 200, "Number of shuffle split (reducer) partitions");
DEFINE_bool(run_example, false, "Run the example and exit.");

DEFINE_string(plan, "", "Path to input json file of the substrait plan.");
DEFINE_string(
    split,
    "",
    "Path to input json file of the splits. Only valid for simulating the first stage. Use comma-separated list for multiple splits.");
DEFINE_string(data, "", "Path to input data files in parquet format. Only valid for simulating the middle stage.");
DEFINE_string(conf, "", "Path to the configuration file.");
DEFINE_string(write_path, "/tmp", "Path for simulate write task.");

struct WriterMetrics {
  int64_t splitTime;
  int64_t evictTime;
  int64_t writeTime;
  int64_t compressTime;
};

std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(
    VeloxMemoryManager* memoryManager,
    const std::string& dataFile,
    const std::vector<std::string>& localDirs) {
  PartitionWriterOptions partitionWriterOptions{};
  if (FLAGS_zstd) {
    partitionWriterOptions.codecBackend = CodecBackend::NONE;
    partitionWriterOptions.compressionType = arrow::Compression::ZSTD;
  } else if (FLAGS_qat_gzip) {
    partitionWriterOptions.codecBackend = CodecBackend::QAT;
    partitionWriterOptions.compressionType = arrow::Compression::GZIP;
  } else if (FLAGS_qat_zstd) {
    partitionWriterOptions.codecBackend = CodecBackend::QAT;
    partitionWriterOptions.compressionType = arrow::Compression::ZSTD;
  } else if (FLAGS_iaa_gzip) {
    partitionWriterOptions.codecBackend = CodecBackend::IAA;
    partitionWriterOptions.compressionType = arrow::Compression::GZIP;
  }

  std::unique_ptr<PartitionWriter> partitionWriter;
  if (FLAGS_celeborn) {
    auto rssClient = std::make_unique<LocalRssClient>(dataFile);
    partitionWriter = std::make_unique<CelebornPartitionWriter>(
        FLAGS_shuffle_partitions,
        std::move(partitionWriterOptions),
        memoryManager->getArrowMemoryPool(),
        std::move(rssClient));
  } else {
    partitionWriter = std::make_unique<LocalPartitionWriter>(
        FLAGS_shuffle_partitions,
        std::move(partitionWriterOptions),
        memoryManager->getArrowMemoryPool(),
        dataFile,
        localDirs);
  }

  auto options = ShuffleWriterOptions{};
  options.partitioning = gluten::toPartitioning(FLAGS_partitioning);
  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter,
      VeloxShuffleWriter::create(
          FLAGS_shuffle_partitions,
          std::move(partitionWriter),
          std::move(options),
          memoryManager->getLeafMemoryPool(),
          memoryManager->getArrowMemoryPool()));

  return shuffleWriter;
}

void populateWriterMetrics(
    const std::shared_ptr<VeloxShuffleWriter>& shuffleWriter,
    int64_t shuffleWriteTime,
    WriterMetrics& metrics) {
  metrics.compressTime += shuffleWriter->totalCompressTime();
  metrics.evictTime += shuffleWriter->totalEvictTime();
  metrics.writeTime += shuffleWriter->totalWriteTime();
  metrics.evictTime +=
      (shuffleWriteTime - shuffleWriter->totalCompressTime() - shuffleWriter->totalEvictTime() -
       shuffleWriter->totalWriteTime());
}

} // namespace

auto BM_Generic = [](::benchmark::State& state,
                     const std::string& planFile,
                     const std::vector<std::string>& splitFiles,
                     const std::vector<std::string>& dataFiles,
                     const std::unordered_map<std::string, std::string>& conf,
                     FileReaderType readerType) {
  // Pin each threads to different CPU# starting from 0 or --cpu.
  if (FLAGS_cpu != -1) {
    setCpu(FLAGS_cpu + state.thread_index());
  } else {
    setCpu(state.thread_index());
  }
  memory::MemoryManager::testingSetInstance({});
  auto memoryManager = getDefaultMemoryManager();
  auto runtime = Runtime::create(kVeloxRuntimeKind, conf);
  auto plan = getPlanFromFile("Plan", planFile);
  std::vector<std::string> splits{};
  for (const auto& splitFile : splitFiles) {
    splits.push_back(getPlanFromFile("ReadRel.LocalFiles", splitFile));
  }
  auto startTime = std::chrono::steady_clock::now();
  int64_t collectBatchTime = 0;
  WriterMetrics writerMetrics{};

  for (auto _ : state) {
    std::vector<std::shared_ptr<gluten::ResultIterator>> inputIters;
    std::vector<FileReaderIterator*> inputItersRaw;
    if (!dataFiles.empty()) {
      for (const auto& input : dataFiles) {
        inputIters.push_back(getInputIteratorFromFileReader(input, readerType));
      }
      std::transform(
          inputIters.begin(),
          inputIters.end(),
          std::back_inserter(inputItersRaw),
          [](std::shared_ptr<gluten::ResultIterator> iter) {
            return static_cast<FileReaderIterator*>(iter->getInputIter());
          });
    }
    runtime->injectWriteFilesTempPath(FLAGS_write_path);
    runtime->parsePlan(reinterpret_cast<uint8_t*>(plan.data()), plan.size(), {}, std::nullopt);
    for (auto& split : splits) {
      runtime->parseSplitInfo(reinterpret_cast<uint8_t*>(split.data()), split.size(), std::nullopt);
    }
    auto resultIter =
        runtime->createResultIterator(memoryManager.get(), "/tmp/test-spill", std::move(inputIters), conf);
    auto veloxPlan = dynamic_cast<gluten::VeloxRuntime*>(runtime)->getVeloxPlan();
    if (FLAGS_with_shuffle) {
      int64_t shuffleWriteTime;
      TIME_NANO_START(shuffleWriteTime);
      std::string dataFile;
      std::vector<std::string> localDirs;
      bool isFromEnv;
      GLUTEN_THROW_NOT_OK(setLocalDirsAndDataFileFromEnv(dataFile, localDirs, isFromEnv));
      const auto& shuffleWriter = createShuffleWriter(memoryManager.get(), dataFile, localDirs);
      while (resultIter->hasNext()) {
        GLUTEN_THROW_NOT_OK(shuffleWriter->split(resultIter->next(), ShuffleWriter::kMinMemLimit));
      }
      GLUTEN_THROW_NOT_OK(shuffleWriter->stop());
      TIME_NANO_END(shuffleWriteTime);
      populateWriterMetrics(shuffleWriter, shuffleWriteTime, writerMetrics);
      // Cleanup shuffle outputs
      cleanupShuffleOutput(dataFile, localDirs, isFromEnv);
    } else {
      // May write the output into file.
      ArrowSchema cSchema;
      toArrowSchema(veloxPlan->outputType(), memoryManager->getLeafMemoryPool().get(), &cSchema);
      GLUTEN_ASSIGN_OR_THROW(auto outputSchema, arrow::ImportSchema(&cSchema));
      ArrowWriter writer{FLAGS_write_file};
      state.PauseTiming();
      if (!FLAGS_write_file.empty()) {
        GLUTEN_THROW_NOT_OK(writer.initWriter(*(outputSchema.get())));
      }
      state.ResumeTiming();

      while (resultIter->hasNext()) {
        auto array = resultIter->next()->exportArrowArray();
        state.PauseTiming();
        auto maybeBatch = arrow::ImportRecordBatch(array.get(), outputSchema);
        if (!maybeBatch.ok()) {
          state.SkipWithError(maybeBatch.status().message().c_str());
          return;
        }
        if (FLAGS_print_result) {
          LOG(INFO) << maybeBatch.ValueOrDie()->ToString();
        }
        if (!FLAGS_write_file.empty()) {
          GLUTEN_THROW_NOT_OK(writer.writeInBatches(maybeBatch.ValueOrDie()));
        }
      }

      state.PauseTiming();
      if (!FLAGS_write_file.empty()) {
        GLUTEN_THROW_NOT_OK(writer.closeWriter());
      }
      state.ResumeTiming();
    }

    collectBatchTime +=
        std::accumulate(inputItersRaw.begin(), inputItersRaw.end(), 0, [](int64_t sum, FileReaderIterator* iter) {
          return sum + iter->getCollectBatchTime();
        });

    auto* rawIter = static_cast<gluten::WholeStageResultIterator*>(resultIter->getInputIter());
    const auto* task = rawIter->task();
    const auto* planNode = rawIter->veloxPlan();
    auto statsStr = facebook::velox::exec::printPlanWithStats(*planNode, task->taskStats(), true);
    LOG(INFO) << statsStr;
  }
  Runtime::release(runtime);

  auto endTime = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();

  state.counters["collect_batch_time"] =
      benchmark::Counter(collectBatchTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
  state.counters["elapsed_time"] =
      benchmark::Counter(duration, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
  state.counters["shuffle_write_time"] = benchmark::Counter(
      writerMetrics.writeTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
  state.counters["shuffle_spill_time"] = benchmark::Counter(
      writerMetrics.evictTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
  state.counters["shuffle_split_time"] = benchmark::Counter(
      writerMetrics.splitTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
  state.counters["shuffle_compress_time"] = benchmark::Counter(
      writerMetrics.compressTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
};

int main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Init Velox backend.
  std::unordered_map<std::string, std::string> backendConf;
  std::unordered_map<std::string, std::string> sessionConf;
  backendConf.insert({gluten::kSparkBatchSize, std::to_string(FLAGS_batch_size)});
  backendConf.insert({kDebugModeEnabled, "true"});
  if (!FLAGS_conf.empty()) {
    abortIfFileNotExists(FLAGS_conf);
    std::ifstream file(FLAGS_conf);

    if (!file.is_open()) {
      LOG(ERROR) << "Unable to open configuration file.";
      ::benchmark::Shutdown();
      std::exit(EXIT_FAILURE);
    }

    // Parse the ini file.
    // Load all key-values under [Backend Conf] to backendConf, under [Session Conf] to sessionConf.
    // If no [Session Conf] section specified, all key-values are loaded for both backendConf and sessionConf.
    bool isBackendConf = true;
    std::string line;
    while (std::getline(file, line)) {
      if (line.empty() || line[0] == ';') {
        continue;
      }
      if (line[0] == '[') {
        if (line == "[Backend Conf]") {
          isBackendConf = true;
        } else if (line == "[Session Conf]") {
          isBackendConf = false;
        } else {
          LOG(ERROR) << "Invalid section: " << line;
          ::benchmark::Shutdown();
          std::exit(EXIT_FAILURE);
        }
        continue;
      }
      std::istringstream iss(line);
      std::string key, value;

      iss >> key;

      // std::ws is used to consume any leading whitespace.
      std::getline(iss >> std::ws, value);

      if (isBackendConf) {
        backendConf[key] = value;
      } else {
        sessionConf[key] = value;
      }
    }
  }
  if (sessionConf.empty()) {
    sessionConf = backendConf;
  }

  initVeloxBackend(backendConf);

  // Parse substrait plan, split file and data files.
  std::string substraitJsonFile = FLAGS_plan;
  std::vector<std::string> splitFiles{};
  std::vector<std::string> dataFiles{};

  if (FLAGS_run_example) {
    LOG(INFO) << "Running example...";
    dataFiles.resize(2);
    try {
      substraitJsonFile = getGeneratedFilePath("example.json");
      dataFiles[0] = getGeneratedFilePath("example_orders");
      dataFiles[1] = getGeneratedFilePath("example_lineitem");
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to run example. " << e.what();
      ::benchmark::Shutdown();
      std::exit(EXIT_FAILURE);
    }
  } else {
    // Validate input args.
    std::string errorMsg{};
    if (substraitJsonFile.empty()) {
      errorMsg = "Missing '--plan' option.";
    } else if (!checkPathExists(substraitJsonFile)) {
      errorMsg = "File path does not exist: " + substraitJsonFile;
    } else if (FLAGS_split.empty() && FLAGS_data.empty()) {
      errorMsg = "Missing '--split' or '--data' option.";
    } else if (!FLAGS_split.empty() && !FLAGS_data.empty()) {
      errorMsg = "Duplicated option '--split' and '--data'.";
    }

    try {
      if (!FLAGS_data.empty()) {
        dataFiles = gluten::splitPaths(FLAGS_data, true);
      } else {
        splitFiles = gluten::splitPaths(FLAGS_split, true);
      }
    } catch (const std::exception& e) {
      errorMsg = e.what();
    }

    if (!errorMsg.empty()) {
      LOG(ERROR) << "Incorrect usage: " << errorMsg << std::endl
                 << "If simulating a first stage, the usage is:" << std::endl
                 << "./generic_benchmark "
                 << "--plan /absolute-path/to/substrait_json_file "
                 << "--split /absolute-path/to/split_json_file_1,/abosolute-path/to/split_json_file_2,..." << std::endl
                 << "If simulating a middle stage, the usage is:" << std::endl
                 << "./generic_benchmark "
                 << "--plan /absolute-path/to/substrait_json_file "
                 << "--data /absolute-path/to/data_file_1,/absolute-path/to/data_file_2,...";
      LOG(ERROR) << "*** Please check docs/developers/MicroBenchmarks.md for the full usage. ***";
      ::benchmark::Shutdown();
      std::exit(EXIT_FAILURE);
    }
  }

  // Check whether input files exist.
  LOG(INFO) << "Using substrait json file: " << std::endl << substraitJsonFile;
  if (!splitFiles.empty()) {
    LOG(INFO) << "Using " << splitFiles.size() << " input split file(s): ";
    for (const auto& splitFile : splitFiles) {
      LOG(INFO) << splitFile;
    }
  } else {
    LOG(INFO) << "Using " << dataFiles.size() << " input data file(s): ";
    for (const auto& dataFile : dataFiles) {
      LOG(INFO) << dataFile;
    }
  }

#define GENERIC_BENCHMARK(NAME, READER_TYPE)                                                             \
  do {                                                                                                   \
    auto* bm = ::benchmark::RegisterBenchmark(                                                           \
                   NAME, BM_Generic, substraitJsonFile, splitFiles, dataFiles, sessionConf, READER_TYPE) \
                   ->MeasureProcessCPUTime()                                                             \
                   ->UseRealTime();                                                                      \
    if (FLAGS_threads > 0) {                                                                             \
      bm->Threads(FLAGS_threads);                                                                        \
    } else {                                                                                             \
      bm->ThreadRange(1, std::thread::hardware_concurrency());                                           \
    }                                                                                                    \
    if (FLAGS_iterations > 0) {                                                                          \
      bm->Iterations(FLAGS_iterations);                                                                  \
    }                                                                                                    \
  } while (0)

  LOG(INFO) << "Using options: ";
  LOG(INFO) << "threads: " << FLAGS_threads;
  LOG(INFO) << "iterations: " << FLAGS_iterations;
  LOG(INFO) << "cpu: " << FLAGS_cpu;
  LOG(INFO) << "print_result: " << FLAGS_print_result;
  LOG(INFO) << "write_file: " << FLAGS_write_file;
  LOG(INFO) << "batch_size: " << FLAGS_batch_size;
  LOG(INFO) << "write_path: " << FLAGS_write_path;

  if (dataFiles.empty()) {
    GENERIC_BENCHMARK("SkipInput", FileReaderType::kNone);
  } else {
    GENERIC_BENCHMARK("InputFromBatchVector", FileReaderType::kBuffered);
    GENERIC_BENCHMARK("InputFromBatchStream", FileReaderType::kStream);
  }

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}
