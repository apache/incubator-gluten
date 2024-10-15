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
#include "compute/VeloxBackend.h"
#include "compute/VeloxPlanConverter.h"
#include "compute/VeloxRuntime.h"
#include "config/GlutenConfig.h"
#include "config/VeloxConfig.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/VeloxShuffleWriter.h"
#include "shuffle/rss/RssPartitionWriter.h"
#include "utils/Exception.h"
#include "utils/StringUtil.h"
#include "utils/Timer.h"
#include "utils/VeloxArrowUtils.h"
#include "utils/tests/LocalRssClient.h"
#include "velox/exec/PlanNodeStats.h"

using namespace gluten;

namespace {

DEFINE_bool(run_example, false, "Run the example and exit.");
DEFINE_bool(print_result, true, "Print result for execution");
DEFINE_string(save_output, "", "Path to parquet file for saving the task output iterator");
DEFINE_bool(with_shuffle, false, "Add shuffle split at end.");
DEFINE_bool(run_shuffle, false, "Only run shuffle write.");
DEFINE_bool(run_shuffle_read, false, "Whether to run shuffle read when run_shuffle is true.");
DEFINE_string(shuffle_writer, "hash", "Shuffle writer type. Can be hash or sort");
DEFINE_string(
    partitioning,
    "rr",
    "Short partitioning name. Valid options are rr, hash, range, single, random (only for test purpose)");
DEFINE_bool(rss, false, "Mocking rss.");
DEFINE_string(
    compression,
    "lz4",
    "Specify the compression codec. Valid options are lz4, zstd, qat_gzip, qat_zstd, iaa_gzip");
DEFINE_int32(shuffle_partitions, 200, "Number of shuffle split (reducer) partitions");

DEFINE_string(plan, "", "Path to input json file of the substrait plan.");
DEFINE_string(
    split,
    "",
    "Path to input json file of the splits. Only valid for simulating the first stage. Use comma-separated list for multiple splits.");
DEFINE_string(data, "", "Path to input data files in parquet format. Use comma-separated list for multiple files.");
DEFINE_string(conf, "", "Path to the configuration file.");
DEFINE_string(write_path, "/tmp", "Path to save the output from write tasks.");
DEFINE_int64(memory_limit, std::numeric_limits<int64_t>::max(), "Memory limit used to trigger spill.");
DEFINE_string(
    scan_mode,
    "stream",
    "Scan mode for reading parquet data."
    "'stream' mode: Input file scan happens inside of the pipeline."
    "'buffered' mode: First read all data into memory and feed the pipeline with it.");
DEFINE_bool(debug_mode, false, "Whether to enable debug mode. Same as setting `spark.gluten.sql.debug`");

struct WriterMetrics {
  int64_t splitTime{0};
  int64_t evictTime{0};
  int64_t writeTime{0};
  int64_t compressTime{0};

  int64_t bytesSpilled{0};
  int64_t bytesWritten{0};
};

struct ReaderMetrics {
  int64_t decompressTime{0};
  int64_t deserializeTime{0};
};

void setUpBenchmark(::benchmark::internal::Benchmark* bm) {
  if (FLAGS_threads > 0) {
    bm->Threads(FLAGS_threads);
  } else {
    bm->ThreadRange(1, std::thread::hardware_concurrency());
  }
  if (FLAGS_iterations > 0) {
    bm->Iterations(FLAGS_iterations);
  }
}

PartitionWriterOptions createPartitionWriterOptions() {
  PartitionWriterOptions partitionWriterOptions{};
  // Disable writer's merge.
  partitionWriterOptions.mergeThreshold = 0;

  // Configure compression.
  if (FLAGS_compression == "lz4") {
    partitionWriterOptions.codecBackend = CodecBackend::NONE;
    partitionWriterOptions.compressionType = arrow::Compression::LZ4_FRAME;
    partitionWriterOptions.compressionTypeStr = "lz4";
  } else if (FLAGS_compression == "zstd") {
    partitionWriterOptions.codecBackend = CodecBackend::NONE;
    partitionWriterOptions.compressionType = arrow::Compression::ZSTD;
    partitionWriterOptions.compressionTypeStr = "zstd";
  } else if (FLAGS_compression == "qat_gzip") {
    partitionWriterOptions.codecBackend = CodecBackend::QAT;
    partitionWriterOptions.compressionType = arrow::Compression::GZIP;
  } else if (FLAGS_compression == "qat_zstd") {
    partitionWriterOptions.codecBackend = CodecBackend::QAT;
    partitionWriterOptions.compressionType = arrow::Compression::ZSTD;
  } else if (FLAGS_compression == "iaa_gzip") {
    partitionWriterOptions.codecBackend = CodecBackend::IAA;
    partitionWriterOptions.compressionType = arrow::Compression::GZIP;
  }
  return partitionWriterOptions;
}

std::unique_ptr<PartitionWriter> createPartitionWriter(
    Runtime* runtime,
    PartitionWriterOptions options,
    const std::string& dataFile,
    const std::vector<std::string>& localDirs) {
  std::unique_ptr<PartitionWriter> partitionWriter;
  if (FLAGS_rss) {
    auto rssClient = std::make_unique<LocalRssClient>(dataFile);
    partitionWriter = std::make_unique<RssPartitionWriter>(
        FLAGS_shuffle_partitions,
        std::move(options),
        runtime->memoryManager()->getArrowMemoryPool(),
        std::move(rssClient));
  } else {
    partitionWriter = std::make_unique<LocalPartitionWriter>(
        FLAGS_shuffle_partitions,
        std::move(options),
        runtime->memoryManager()->getArrowMemoryPool(),
        dataFile,
        localDirs);
  }
  return partitionWriter;
}

std::shared_ptr<VeloxShuffleWriter> createShuffleWriter(
    Runtime* runtime,
    std::unique_ptr<PartitionWriter> partitionWriter) {
  auto options = ShuffleWriterOptions{};
  options.partitioning = gluten::toPartitioning(FLAGS_partitioning);
  if (FLAGS_rss || FLAGS_shuffle_writer == "rss_sort") {
    options.shuffleWriterType = gluten::kRssSortShuffle;
  } else if (FLAGS_shuffle_writer == "sort") {
    options.shuffleWriterType = gluten::kSortShuffle;
  }
  auto shuffleWriter =
      runtime->createShuffleWriter(FLAGS_shuffle_partitions, std::move(partitionWriter), std::move(options));

  return std::reinterpret_pointer_cast<VeloxShuffleWriter>(shuffleWriter);
}

void populateWriterMetrics(
    const std::shared_ptr<VeloxShuffleWriter>& shuffleWriter,
    int64_t totalTime,
    WriterMetrics& metrics) {
  metrics.compressTime += shuffleWriter->totalCompressTime();
  metrics.evictTime += shuffleWriter->totalEvictTime();
  metrics.writeTime += shuffleWriter->totalWriteTime();
  auto splitTime = totalTime - metrics.compressTime - metrics.evictTime - metrics.writeTime;
  if (splitTime > 0) {
    metrics.splitTime += splitTime;
  }
  metrics.bytesWritten += shuffleWriter->totalBytesWritten();
  metrics.bytesSpilled += shuffleWriter->totalBytesEvicted();
}

void setCpu(::benchmark::State& state) {
  // Pin each threads to different CPU# starting from 0 or --cpu.
  auto cpu = state.thread_index();
  if (FLAGS_cpu != -1) {
    cpu += FLAGS_cpu;
  }
  LOG(WARNING) << "Setting CPU for thread " << state.thread_index() << " to " << cpu;
  gluten::setCpu(cpu);
}

void runShuffle(
    Runtime* runtime,
    BenchmarkAllocationListener* listener,
    const std::shared_ptr<gluten::ResultIterator>& resultIter,
    WriterMetrics& writerMetrics,
    ReaderMetrics& readerMetrics,
    bool readAfterWrite) {
  std::string dataFile;
  std::vector<std::string> localDirs;
  bool isFromEnv;
  GLUTEN_THROW_NOT_OK(setLocalDirsAndDataFileFromEnv(dataFile, localDirs, isFromEnv));

  auto partitionWriterOptions = createPartitionWriterOptions();
  auto partitionWriter = createPartitionWriter(runtime, partitionWriterOptions, dataFile, localDirs);
  auto shuffleWriter = createShuffleWriter(runtime, std::move(partitionWriter));
  listener->setShuffleWriter(shuffleWriter.get());

  int64_t totalTime = 0;
  std::shared_ptr<ArrowSchema> cSchema;
  {
    gluten::ScopedTimer timer(&totalTime);
    while (resultIter->hasNext()) {
      auto cb = resultIter->next();
      if (!cSchema) {
        cSchema = cb->exportArrowSchema();
      }
      GLUTEN_THROW_NOT_OK(shuffleWriter->write(cb, ShuffleWriter::kMaxMemLimit - shuffleWriter->cachedPayloadSize()));
    }
    GLUTEN_THROW_NOT_OK(shuffleWriter->stop());
  }

  populateWriterMetrics(shuffleWriter, totalTime, writerMetrics);

  if (readAfterWrite && cSchema) {
    auto readerOptions = ShuffleReaderOptions{};
    readerOptions.shuffleWriterType = shuffleWriter->options().shuffleWriterType;
    readerOptions.compressionType = partitionWriterOptions.compressionType;
    readerOptions.codecBackend = partitionWriterOptions.codecBackend;
    readerOptions.compressionTypeStr = partitionWriterOptions.compressionTypeStr;

    std::shared_ptr<arrow::Schema> schema =
        gluten::arrowGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(cSchema.get())));
    auto reader = runtime->createShuffleReader(schema, readerOptions);

    GLUTEN_ASSIGN_OR_THROW(auto in, arrow::io::ReadableFile::Open(dataFile));
    // Read all partitions.
    auto iter = reader->readStream(in);
    while (iter->hasNext()) {
      // Read and discard.
      auto cb = iter->next();
    }
    readerMetrics.decompressTime = reader->getDecompressTime();
    readerMetrics.deserializeTime = reader->getDeserializeTime();
  }
  // Cleanup shuffle outputs
  cleanupShuffleOutput(dataFile, localDirs, isFromEnv);
}

void updateBenchmarkMetrics(
    ::benchmark::State& state,
    const int64_t& elapsedTime,
    const int64_t& readInputTime,
    const WriterMetrics& writerMetrics,
    const ReaderMetrics& readerMetrics) {
  state.counters["read_input_time"] =
      benchmark::Counter(readInputTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
  state.counters["elapsed_time"] =
      benchmark::Counter(elapsedTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);

  if (FLAGS_run_shuffle || FLAGS_with_shuffle) {
    state.counters["shuffle_write_time"] = benchmark::Counter(
        writerMetrics.writeTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
    state.counters["shuffle_spill_time"] = benchmark::Counter(
        writerMetrics.evictTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
    state.counters["shuffle_compress_time"] = benchmark::Counter(
        writerMetrics.compressTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
    state.counters["shuffle_decompress_time"] = benchmark::Counter(
        readerMetrics.decompressTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);
    state.counters["shuffle_deserialize_time"] = benchmark::Counter(
        readerMetrics.deserializeTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);

    auto splitTime = writerMetrics.splitTime;
    if (FLAGS_scan_mode == "stream") {
      splitTime -= readInputTime;
    }
    state.counters["shuffle_split_time"] =
        benchmark::Counter(splitTime, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1000);

    state.counters["shuffle_spilled_bytes"] = benchmark::Counter(
        writerMetrics.bytesSpilled, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1024);
    state.counters["shuffle_write_bytes"] = benchmark::Counter(
        writerMetrics.bytesWritten, benchmark::Counter::kAvgIterations, benchmark::Counter::OneK::kIs1024);
  }
}

} // namespace

using RuntimeFactory = std::function<VeloxRuntime*(MemoryManager* memoryManager)>;

auto BM_Generic = [](::benchmark::State& state,
                     const std::string& planFile,
                     const std::vector<std::string>& splitFiles,
                     const std::vector<std::string>& dataFiles,
                     RuntimeFactory runtimeFactory,
                     FileReaderType readerType) {
  setCpu(state);

  auto listener = std::make_unique<BenchmarkAllocationListener>(FLAGS_memory_limit);
  auto* listenerPtr = listener.get();
  auto* memoryManager = MemoryManager::create(kVeloxBackendKind, std::move(listener));
  auto runtime = runtimeFactory(memoryManager);

  auto plan = getPlanFromFile("Plan", planFile);
  std::vector<std::string> splits{};
  for (const auto& splitFile : splitFiles) {
    splits.push_back(getPlanFromFile("ReadRel.LocalFiles", splitFile));
  }

  WriterMetrics writerMetrics{};
  ReaderMetrics readerMetrics{};
  int64_t readInputTime = 0;
  int64_t elapsedTime = 0;

  {
    ScopedTimer timer(&elapsedTime);
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
      *Runtime::localWriteFilesTempPath() = FLAGS_write_path;
      runtime->parsePlan(reinterpret_cast<uint8_t*>(plan.data()), plan.size(), std::nullopt);
      for (auto& split : splits) {
        runtime->parseSplitInfo(reinterpret_cast<uint8_t*>(split.data()), split.size(), std::nullopt);
      }
      auto resultIter = runtime->createResultIterator("/tmp/test-spill", std::move(inputIters), runtime->getConfMap());
      listenerPtr->setIterator(resultIter.get());

      if (FLAGS_with_shuffle) {
        runShuffle(runtime, listenerPtr, resultIter, writerMetrics, readerMetrics, false);
      } else {
        // May write the output into file.
        auto veloxPlan = dynamic_cast<gluten::VeloxRuntime*>(runtime)->getVeloxPlan();

        ArrowSchema cSchema;
        toArrowSchema(veloxPlan->outputType(), runtime->memoryManager()->getLeafMemoryPool().get(), &cSchema);
        GLUTEN_ASSIGN_OR_THROW(auto outputSchema, arrow::ImportSchema(&cSchema));
        ArrowWriter writer{FLAGS_save_output};
        state.PauseTiming();
        if (!FLAGS_save_output.empty()) {
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
            LOG(WARNING) << maybeBatch.ValueOrDie()->ToString();
          }
          if (!FLAGS_save_output.empty()) {
            GLUTEN_THROW_NOT_OK(writer.writeInBatches(maybeBatch.ValueOrDie()));
          }
        }

        state.PauseTiming();
        if (!FLAGS_save_output.empty()) {
          GLUTEN_THROW_NOT_OK(writer.closeWriter());
        }
        state.ResumeTiming();
      }

      readInputTime +=
          std::accumulate(inputItersRaw.begin(), inputItersRaw.end(), 0, [](int64_t sum, FileReaderIterator* iter) {
            return sum + iter->getCollectBatchTime();
          });

      auto* rawIter = static_cast<gluten::WholeStageResultIterator*>(resultIter->getInputIter());
      const auto* task = rawIter->task();
      const auto* planNode = rawIter->veloxPlan();
      auto statsStr = facebook::velox::exec::printPlanWithStats(*planNode, task->taskStats(), true);
      LOG(WARNING) << statsStr;
    }
  }

  updateBenchmarkMetrics(state, elapsedTime, readInputTime, writerMetrics, readerMetrics);
  Runtime::release(runtime);
  MemoryManager::release(memoryManager);
};

auto BM_ShuffleWriteRead = [](::benchmark::State& state,
                              const std::string& inputFile,
                              RuntimeFactory runtimeFactory,
                              FileReaderType readerType) {
  setCpu(state);

  auto listener = std::make_unique<BenchmarkAllocationListener>(FLAGS_memory_limit);
  auto* listenerPtr = listener.get();
  auto* memoryManager = MemoryManager::create(kVeloxBackendKind, std::move(listener));
  auto runtime = runtimeFactory(memoryManager);

  WriterMetrics writerMetrics{};
  ReaderMetrics readerMetrics{};
  int64_t readInputTime = 0;
  int64_t elapsedTime = 0;
  {
    ScopedTimer timer(&elapsedTime);
    for (auto _ : state) {
      auto resultIter = getInputIteratorFromFileReader(inputFile, readerType);
      runShuffle(runtime, listenerPtr, resultIter, writerMetrics, readerMetrics, FLAGS_run_shuffle_read);

      auto reader = static_cast<FileReaderIterator*>(resultIter->getInputIter());
      readInputTime += reader->getCollectBatchTime();
    }
  }

  updateBenchmarkMetrics(state, elapsedTime, readInputTime, writerMetrics, readerMetrics);
  Runtime::release(runtime);
  MemoryManager::release(memoryManager);
};

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::ostringstream ss;
  ss << "Setting flags from command line args: " << std::endl;
  std::vector<google::CommandLineFlagInfo> flags;
  google::GetAllFlags(&flags);
  auto filename = std::filesystem::path(__FILE__).filename();
  for (const auto& flag : flags) {
    if (std::filesystem::path(flag.filename).filename() == filename) {
      ss << "    FLAGS_" << flag.name << ": default = " << flag.default_value << ", current = " << flag.current_value
         << std::endl;
    }
  }
  LOG(WARNING) << ss.str();

  ::benchmark::Initialize(&argc, argv);

  // Init Velox backend.
  auto backendConf = gluten::defaultConf();
  auto sessionConf = gluten::defaultConf();
  backendConf.insert({gluten::kDebugModeEnabled, std::to_string(FLAGS_debug_mode)});
  backendConf.insert({gluten::kGlogVerboseLevel, std::to_string(FLAGS_v)});
  backendConf.insert({gluten::kGlogSeverityLevel, std::to_string(FLAGS_minloglevel)});
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
  memory::MemoryManager::testingSetInstance({});

  // Parse substrait plan, split file and data files.
  std::string substraitJsonFile = FLAGS_plan;
  std::vector<std::string> splitFiles{};
  std::vector<std::string> dataFiles{};

  if (FLAGS_run_example) {
    LOG(WARNING) << "Running example...";
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
  } else if (FLAGS_run_shuffle) {
    std::string errorMsg{};
    if (FLAGS_data.empty()) {
      errorMsg = "Missing '--split' or '--data' option.";
    } else if (FLAGS_partitioning != "rr" && FLAGS_partitioning != "random") {
      errorMsg = "--run-shuffle only support round-robin partitioning and random partitioning.";
    }
    if (errorMsg.empty()) {
      try {
        dataFiles = gluten::splitPaths(FLAGS_data, true);
        if (dataFiles.size() > 1) {
          errorMsg = "Only one data file is allowed for shuffle write.";
        }
      } catch (const std::exception& e) {
        errorMsg = e.what();
      }
    }
    if (!errorMsg.empty()) {
      LOG(ERROR) << "Incorrect usage: " << errorMsg << std::endl;
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
    }

    if (errorMsg.empty()) {
      try {
        if (!FLAGS_data.empty()) {
          dataFiles = gluten::splitPaths(FLAGS_data, true);
        }
        if (!FLAGS_split.empty()) {
          splitFiles = gluten::splitPaths(FLAGS_split, true);
        }
      } catch (const std::exception& e) {
        errorMsg = e.what();
      }
    }

    if (!errorMsg.empty()) {
      LOG(ERROR) << "Incorrect usage: " << errorMsg << std::endl
                 << "*** Please check docs/developers/MicroBenchmarks.md for the full usage. ***";
      ::benchmark::Shutdown();
      std::exit(EXIT_FAILURE);
    }
  }

  LOG(WARNING) << "Using substrait json file: " << std::endl << substraitJsonFile;
  if (!splitFiles.empty()) {
    LOG(WARNING) << "Using " << splitFiles.size() << " input split file(s): ";
    for (const auto& splitFile : splitFiles) {
      LOG(WARNING) << splitFile;
    }
  }
  if (!dataFiles.empty()) {
    LOG(WARNING) << "Using " << dataFiles.size() << " input data file(s): ";
    for (const auto& dataFile : dataFiles) {
      LOG(WARNING) << dataFile;
    }
  }

  RuntimeFactory runtimeFactory = [=](MemoryManager* memoryManager) {
    return dynamic_cast<VeloxRuntime*>(Runtime::create(kVeloxBackendKind, memoryManager, sessionConf));
  };

#define GENERIC_BENCHMARK(READER_TYPE)                                                                             \
  do {                                                                                                             \
    auto* bm =                                                                                                     \
        ::benchmark::RegisterBenchmark(                                                                            \
            "GenericBenchmark", BM_Generic, substraitJsonFile, splitFiles, dataFiles, runtimeFactory, READER_TYPE) \
            ->MeasureProcessCPUTime()                                                                              \
            ->UseRealTime();                                                                                       \
    setUpBenchmark(bm);                                                                                            \
  } while (0)

#define SHUFFLE_WRITE_READ_BENCHMARK(READER_TYPE)                                                      \
  do {                                                                                                 \
    auto* bm = ::benchmark::RegisterBenchmark(                                                         \
                   "ShuffleWriteRead", BM_ShuffleWriteRead, dataFiles[0], runtimeFactory, READER_TYPE) \
                   ->MeasureProcessCPUTime()                                                           \
                   ->UseRealTime();                                                                    \
    setUpBenchmark(bm);                                                                                \
  } while (0)

  if (dataFiles.empty()) {
    GENERIC_BENCHMARK(FileReaderType::kNone);
  } else {
    FileReaderType readerType;
    if (FLAGS_scan_mode == "buffered") {
      readerType = FileReaderType::kBuffered;
      LOG(WARNING) << "Using buffered mode for reading parquet data.";
    } else {
      readerType = FileReaderType::kStream;
      LOG(WARNING) << "Using stream mode for reading parquet data.";
    }
    if (FLAGS_run_shuffle) {
      SHUFFLE_WRITE_READ_BENCHMARK(readerType);
    } else {
      GENERIC_BENCHMARK(readerType);
    }
  }

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  gluten::VeloxBackend::get()->tearDown();

  return 0;
}
