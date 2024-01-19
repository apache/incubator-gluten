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

#include "BenchmarkUtils.h"
#include "compute/VeloxBackend.h"
#include "compute/VeloxRuntime.h"
#include "config/GlutenConfig.h"
#include "shuffle/Utils.h"
#include "utils/StringUtil.h"
#include "velox/dwio/common/Options.h"

using namespace facebook;
namespace fs = std::filesystem;

DEFINE_bool(print_result, true, "Print result for execution");
DEFINE_string(write_file, "", "Write the output to parquet file, file absolute path");
DEFINE_int64(batch_size, 4096, "To set velox::core::QueryConfig::kPreferredOutputBatchSize.");
DEFINE_int32(cpu, -1, "Run benchmark on specific CPU");
DEFINE_int32(threads, 1, "The number of threads to run this benchmark");
DEFINE_int32(iterations, 1, "The number of iterations to run this benchmark");

namespace {

std::unordered_map<std::string, std::string> bmConfMap = {{gluten::kSparkBatchSize, std::to_string(FLAGS_batch_size)}};

} // namespace

void initVeloxBackend(std::unordered_map<std::string, std::string>& conf) {
  gluten::VeloxBackend::create(conf);
}

void initVeloxBackend() {
  initVeloxBackend(bmConfMap);
}

std::string getPlanFromFile(const std::string& type, const std::string& filePath) {
  // Read json file and resume the binary data.
  std::ifstream msgJson(filePath);
  std::stringstream buffer;
  buffer << msgJson.rdbuf();
  std::string msgData = buffer.str();

  return gluten::substraitFromJsonToPb(type, msgData);
}

velox::dwio::common::FileFormat getFileFormat(const std::string& fileFormat) {
  if (fileFormat.compare("orc") == 0) {
    return velox::dwio::common::FileFormat::ORC;
  } else if (fileFormat.compare("parquet") == 0) {
    return velox::dwio::common::FileFormat::PARQUET;
  } else {
    return velox::dwio::common::FileFormat::UNKNOWN;
  }
}

std::shared_ptr<gluten::SplitInfo> getSplitInfos(const std::string& datasetPath, const std::string& fileFormat) {
  auto scanInfo = std::make_shared<gluten::SplitInfo>();

  // Set format to scan info.
  scanInfo->format = getFileFormat(fileFormat);

  // Set split start, length, and path to scan info.
  std::filesystem::path fileDir(datasetPath);
  for (auto i = std::filesystem::directory_iterator(fileDir); i != std::filesystem::directory_iterator(); i++) {
    if (!is_directory(i->path())) {
      std::string singleFilePath = i->path().filename().string();
      if (endsWith(singleFilePath, "." + fileFormat)) {
        auto fileAbsolutePath = datasetPath + singleFilePath;
        scanInfo->starts.emplace_back(0);
        scanInfo->lengths.emplace_back(fs::file_size(fileAbsolutePath));
        scanInfo->paths.emplace_back("file://" + fileAbsolutePath);
      }
    } else {
      continue;
    }
  }
  return scanInfo;
}

std::shared_ptr<gluten::SplitInfo> getSplitInfosFromFile(const std::string& fileName, const std::string& fileFormat) {
  auto scanInfo = std::make_shared<gluten::SplitInfo>();

  // Set format to scan info.
  scanInfo->format = getFileFormat(fileFormat);

  // Set split start, length, and path to scan info.
  scanInfo->starts.emplace_back(0);
  scanInfo->lengths.emplace_back(fs::file_size(fileName));
  scanInfo->paths.emplace_back("file://" + fileName);

  return scanInfo;
}

bool checkPathExists(const std::string& filepath) {
  std::filesystem::path f{filepath};
  return std::filesystem::exists(f);
}

void abortIfFileNotExists(const std::string& filepath) {
  if (!checkPathExists(filepath)) {
    LOG(WARNING) << "File path does not exist: " << filepath;
    ::benchmark::Shutdown();
    std::exit(EXIT_FAILURE);
  }
}

bool endsWith(const std::string& data, const std::string& suffix) {
  return data.find(suffix, data.size() - suffix.size()) != std::string::npos;
}

#if 0
std::shared_ptr<arrow::RecordBatchReader> createReader(const std::string& path) {
  std::unique_ptr<parquet::arrow::FileReader> parquetReader;
  std::shared_ptr<arrow::RecordBatchReader> recordBatchReader;
  parquet::ArrowReaderProperties properties = parquet::default_arrow_reader_properties();

  GLUTEN_THROW_NOT_OK(parquet::arrow::FileReader::Make(
      arrow::default_memory_pool(), parquet::ParquetFileReader::OpenFile(path), properties, &parquetReader));
  GLUTEN_THROW_NOT_OK(
      parquetReader->GetRecordBatchReader(arrow::internal::Iota(parquetReader->num_row_groups()), &recordBatchReader));
  return recordBatchReader;
}
#endif

void setCpu(uint32_t cpuindex) {
  static const auto kTotalCores = std::thread::hardware_concurrency();
  cpuindex = cpuindex % kTotalCores;
  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(cpuindex, &cs);
  if (sched_setaffinity(0, sizeof(cs), &cs) == -1) {
    LOG(WARNING) << "Error binding CPU " << std::to_string(cpuindex);
    exit(EXIT_FAILURE);
  }
}

arrow::Status
setLocalDirsAndDataFileFromEnv(std::string& dataFile, std::vector<std::string>& localDirs, bool& isFromEnv) {
  auto joinedDirsC = std::getenv(gluten::kGlutenSparkLocalDirs.c_str());
  if (joinedDirsC != nullptr && strcmp(joinedDirsC, "") > 0) {
    isFromEnv = true;
    // Set local dirs.
    auto joinedDirs = std::string(joinedDirsC);
    // Split local dirs and use thread id to choose one directory for data file.
    localDirs = gluten::splitPaths(joinedDirs);
    size_t id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % localDirs.size();
    ARROW_ASSIGN_OR_RAISE(dataFile, gluten::createTempShuffleFile(localDirs[id]));
  } else {
    isFromEnv = false;
    // Otherwise create 1 temp dir and data file.
    static const std::string kBenchmarkDirsPrefix = "columnar-shuffle-benchmark-";
    {
      // Because tmpDir will be deleted in the dtor, allow it to be deleted upon exiting the block and then recreate it
      // in createTempShuffleFile.
      ARROW_ASSIGN_OR_RAISE(auto tmpDir, arrow::internal::TemporaryDir::Make(kBenchmarkDirsPrefix))
      localDirs.push_back(tmpDir->path().ToString());
    }
    ARROW_ASSIGN_OR_RAISE(dataFile, gluten::createTempShuffleFile(localDirs.back()));
  }
  return arrow::Status::OK();
}

void cleanupShuffleOutput(const std::string& dataFile, const std::vector<std::string>& localDirs, bool isFromEnv) {
  std::filesystem::remove(dataFile);
  for (auto& localDir : localDirs) {
    if (std::filesystem::is_empty(localDir)) {
      std::filesystem::remove(localDir);
    }
  }
}
