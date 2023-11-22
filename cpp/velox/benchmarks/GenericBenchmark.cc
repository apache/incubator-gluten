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
#include "utils/VeloxArrowUtils.h"
#include "utils/exception.h"
#include "velox/exec/PlanNodeStats.h"

using namespace gluten;

namespace {
DEFINE_bool(skip_input, false, "Skip specifying input files.");
DEFINE_bool(with_shuffle, false, "Add shuffle split at end.");
DEFINE_string(partitioning, "rr", "Short partitioning name. Valid options are rr, hash, range, single");
DEFINE_bool(zstd, false, "Use ZSTD as shuffle compression codec");
DEFINE_bool(qat_gzip, false, "Use QAT GZIP as shuffle compression codec");
DEFINE_bool(qat_zstd, false, "Use QAT ZSTD as shuffle compression codec");
DEFINE_bool(iaa_gzip, false, "Use IAA GZIP as shuffle compression codec");
DEFINE_int32(shuffle_partitions, 200, "Number of shuffle split (reducer) partitions");

struct WriterMetrics {
  int64_t splitTime;
  int64_t evictTime;
  int64_t writeTime;
  int64_t compressTime;
};

std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(VeloxMemoryManager* memoryManager) {
  std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator =
      std::make_shared<LocalPartitionWriterCreator>();

  auto options = ShuffleWriterOptions::defaults();
  options.memory_pool = memoryManager->getArrowMemoryPool();
  options.partitioning = gluten::toPartitioning(FLAGS_partitioning);
  if (FLAGS_zstd) {
    options.codec_backend = CodecBackend::NONE;
    options.compression_type = arrow::Compression::ZSTD;
  } else if (FLAGS_qat_gzip) {
    options.codec_backend = CodecBackend::QAT;
    options.compression_type = arrow::Compression::GZIP;
  } else if (FLAGS_qat_zstd) {
    options.codec_backend = CodecBackend::QAT;
    options.compression_type = arrow::Compression::ZSTD;
  } else if (FLAGS_iaa_gzip) {
    options.codec_backend = CodecBackend::IAA;
    options.compression_type = arrow::Compression::GZIP;
  }

  GLUTEN_THROW_NOT_OK(setLocalDirsAndDataFileFromEnv(options));

  GLUTEN_ASSIGN_OR_THROW(
      auto shuffleWriter,
      VeloxShuffleWriter::create(
          FLAGS_shuffle_partitions,
          std::move(partitionWriterCreator),
          std::move(options),
          memoryManager->getLeafMemoryPool()));

  return shuffleWriter;
}

void cleanup(const std::shared_ptr<VeloxShuffleWriter>& shuffleWriter) {
  auto dataFile = std::filesystem::path(shuffleWriter->dataFile());
  const auto& parentDir = dataFile.parent_path();
  std::filesystem::remove(dataFile);
  if (std::filesystem::is_empty(parentDir)) {
    std::filesystem::remove(parentDir);
  }
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
                     const std::string& substraitJsonFile,
                     const std::vector<std::string>& inputFiles,
                     const std::unordered_map<std::string, std::string>& conf,
                     FileReaderType readerType) {
  // Pin each threads to different CPU# starting from 0 or --cpu.
  if (FLAGS_cpu != -1) {
    setCpu(FLAGS_cpu + state.thread_index());
  } else {
    setCpu(state.thread_index());
  }
  auto memoryManager = getDefaultMemoryManager();
  auto runtime = Runtime::create(kVeloxRuntimeKind, conf);
  const auto& filePath = getExampleFilePath(substraitJsonFile);
  auto plan = getPlanFromFile(filePath);
  auto startTime = std::chrono::steady_clock::now();
  int64_t collectBatchTime = 0;
  WriterMetrics writerMetrics{};

  for (auto _ : state) {
    std::vector<std::shared_ptr<gluten::ResultIterator>> inputIters;
    std::vector<FileReaderIterator*> inputItersRaw;
    if (!inputFiles.empty()) {
      for (const auto& input : inputFiles) {
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

    runtime->parsePlan(reinterpret_cast<uint8_t*>(plan.data()), plan.size(), {});
    auto resultIter =
        runtime->createResultIterator(memoryManager.get(), "/tmp/test-spill", std::move(inputIters), conf);
    auto veloxPlan = dynamic_cast<gluten::VeloxRuntime*>(runtime)->getVeloxPlan();
    if (FLAGS_with_shuffle) {
      int64_t shuffleWriteTime;
      TIME_NANO_START(shuffleWriteTime);
      const auto& shuffleWriter = createShuffleWriter(memoryManager.get());
      while (resultIter->hasNext()) {
        GLUTEN_THROW_NOT_OK(shuffleWriter->split(resultIter->next(), ShuffleWriter::kMinMemLimit));
      }
      GLUTEN_THROW_NOT_OK(shuffleWriter->stop());
      TIME_NANO_END(shuffleWriteTime);
      populateWriterMetrics(shuffleWriter, shuffleWriteTime, writerMetrics);
      // Cleanup shuffle outputs
      cleanup(shuffleWriter);
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
          std::cout << maybeBatch.ValueOrDie()->ToString() << std::endl;
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
    const auto& task = rawIter->task_;
    const auto& planNode = rawIter->veloxPlan_;
    auto statsStr = facebook::velox::exec::printPlanWithStats(*planNode, task->taskStats(), true);
    std::cout << statsStr << std::endl;
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

  std::string substraitJsonFile;
  std::vector<std::string> inputFiles;
  std::unordered_map<std::string, std::string> conf;

  conf.insert({gluten::kSparkBatchSize, FLAGS_batch_size});
  conf.insert({kDebugModeEnabled, "true"});
  initVeloxBackend(conf);

  try {
    if (argc < 2) {
      std::cout << "No input args. Usage: " << std::endl
                << "./generic_benchmark /path/to/substrait_json_file /path/to/data_file_1 /path/to/data_file_2 ..."
                << std::endl;
      std::cout << "Running example..." << std::endl;
      inputFiles.resize(2);
      substraitJsonFile = getGeneratedFilePath("example.json");
      inputFiles[0] = getGeneratedFilePath("example_orders");
      inputFiles[1] = getGeneratedFilePath("example_lineitem");
    } else {
      substraitJsonFile = argv[1];
      abortIfFileNotExists(substraitJsonFile);
      std::cout << "Using substrait json file: " << std::endl << substraitJsonFile << std::endl;
      std::cout << "Using " << argc - 2 << " input data file(s): " << std::endl;
      for (auto i = 2; i < argc; ++i) {
        inputFiles.emplace_back(argv[i]);
        abortIfFileNotExists(inputFiles.back());
        std::cout << inputFiles.back() << std::endl;
      }
    }
  } catch (const std::exception& e) {
    std::cout << "Failed to run benchmark: " << e.what() << std::endl;
    ::benchmark::Shutdown();
    std::exit(EXIT_FAILURE);
  }

#define GENERIC_BENCHMARK(NAME, READER_TYPE)                                                                      \
  do {                                                                                                            \
    auto* bm = ::benchmark::RegisterBenchmark(NAME, BM_Generic, substraitJsonFile, inputFiles, conf, READER_TYPE) \
                   ->MeasureProcessCPUTime()                                                                      \
                   ->UseRealTime();                                                                               \
    if (FLAGS_threads > 0) {                                                                                      \
      bm->Threads(FLAGS_threads);                                                                                 \
    } else {                                                                                                      \
      bm->ThreadRange(1, std::thread::hardware_concurrency());                                                    \
    }                                                                                                             \
    if (FLAGS_iterations > 0) {                                                                                   \
      bm->Iterations(FLAGS_iterations);                                                                           \
    }                                                                                                             \
  } while (0)

#if 0
  std::cout << "FLAGS_threads:" << FLAGS_threads << std::endl;
  std::cout << "FLAGS_iterations:" << FLAGS_iterations << std::endl;
  std::cout << "FLAGS_cpu:" << FLAGS_cpu << std::endl;
  std::cout << "FLAGS_print_result:" << FLAGS_print_result << std::endl;
  std::cout << "FLAGS_write_file:" << FLAGS_write_file << std::endl;
  std::cout << "FLAGS_batch_size:" << FLAGS_batch_size << std::endl;
#endif

  if (FLAGS_skip_input) {
    GENERIC_BENCHMARK("SkipInput", FileReaderType::kNone);
  } else {
    GENERIC_BENCHMARK("InputFromBatchVector", FileReaderType::kBuffered);
    GENERIC_BENCHMARK("InputFromBatchStream", FileReaderType::kStream);
  }

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}
