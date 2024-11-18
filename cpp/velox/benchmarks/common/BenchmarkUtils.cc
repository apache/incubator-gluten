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
#include "config/VeloxConfig.h"
#include "shuffle/Utils.h"
#include "utils/StringUtil.h"
#include "velox/dwio/common/Options.h"

DEFINE_int64(batch_size, 4096, "To set velox::core::QueryConfig::kPreferredOutputBatchSize.");
DEFINE_int32(cpu, -1, "Run benchmark on specific CPU");
DEFINE_int32(threads, 1, "The number of threads to run this benchmark");
DEFINE_int32(iterations, 1, "The number of iterations to run this benchmark");

namespace gluten {
namespace {
std::unordered_map<std::string, std::string> bmConfMap = defaultConf();
} // namespace

std::unordered_map<std::string, std::string> defaultConf() {
  return {
      {gluten::kSparkBatchSize, std::to_string(FLAGS_batch_size)},
  };
}

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

facebook::velox::dwio::common::FileFormat getFileFormat(const std::string& fileFormat) {
  if (fileFormat.compare("orc") == 0) {
    return facebook::velox::dwio::common::FileFormat::ORC;
  } else if (fileFormat.compare("parquet") == 0) {
    return facebook::velox::dwio::common::FileFormat::PARQUET;
  } else {
    return facebook::velox::dwio::common::FileFormat::UNKNOWN;
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
        scanInfo->lengths.emplace_back(std::filesystem::file_size(fileAbsolutePath));
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
  scanInfo->lengths.emplace_back(std::filesystem::file_size(fileName));
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

void setCpu(uint32_t cpuIndex) {
  static const auto kTotalCores = std::thread::hardware_concurrency();
  cpuIndex = cpuIndex % kTotalCores;
  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(cpuIndex, &cs);
  if (sched_setaffinity(0, sizeof(cs), &cs) == -1) {
    LOG(WARNING) << "Error binding CPU " << std::to_string(cpuIndex);
    std::exit(EXIT_FAILURE);
  }
}

void createDirOrAbort(const std::string& path) {
  std::error_code ec;
  if (!std::filesystem::create_directories(path)) {
    LOG(ERROR) << fmt::format("Failed to created spill directory: {}, error code: {}", path, ec.message());
    std::exit(EXIT_FAILURE);
  }
}

std::vector<std::string> createLocalDirs() {
  static const std::string kBenchmarkDirsPrefix = "generic-benchmark-";
  std::vector<std::string> localDirs;

  auto joinedDirsC = std::getenv(gluten::kGlutenSparkLocalDirs.c_str());
  // Check if local dirs are set from env.
  if (joinedDirsC != nullptr && strcmp(joinedDirsC, "") > 0) {
    auto joinedDirs = std::string(joinedDirsC);
    auto dirs = gluten::splitPaths(joinedDirs);
    for (const auto& dir : dirs) {
      auto path = std::filesystem::path(dir) / (kBenchmarkDirsPrefix + generateUuid());
      createDirOrAbort(path);
      localDirs.push_back(path.string());
    }
  } else {
    // Otherwise create 1 temp dir.
    auto tempPath = std::filesystem::temp_directory_path() / (kBenchmarkDirsPrefix + generateUuid());
    createDirOrAbort(tempPath);
    localDirs.push_back(tempPath.string());
  }
  return localDirs;
}

void cleanupLocalDirs(const std::vector<std::string>& localDirs) {
  for (const auto& localDir : localDirs) {
    std::error_code ec;
    std::filesystem::remove_all(localDir, ec);
    if (ec) {
      LOG(WARNING) << fmt::format("Failed to remove directory: {}, error message: {}", localDir, ec.message());
    } else {
      LOG(INFO) << "Removed local dir: " << localDir;
    }
  }
}

void BenchmarkAllocationListener::allocationChanged(int64_t diff) {
  if (diff > 0 && usedBytes_ + diff >= limit_) {
    LOG(INFO) << fmt::format(
        "reach hard limit {} when need {}, current used {}.",
        facebook::velox::succinctBytes(limit_),
        facebook::velox::succinctBytes(diff),
        facebook::velox::succinctBytes(usedBytes_));
    auto neededBytes = usedBytes_ + diff - limit_;
    int64_t spilledBytes = 0;
    if (iterator_) {
      spilledBytes += iterator_->spillFixedSize(neededBytes);
    }
    if (spilledBytes < neededBytes && shuffleWriter_) {
      int64_t reclaimed = 0;
      GLUTEN_THROW_NOT_OK(shuffleWriter_->reclaimFixedSize(neededBytes - spilledBytes, &reclaimed));
      spilledBytes += reclaimed;
    }
    LOG(INFO) << fmt::format("spill finish, got {}.", facebook::velox::succinctBytes(spilledBytes));
  } else {
    usedBytes_ += diff;
  }
}
} // namespace gluten
