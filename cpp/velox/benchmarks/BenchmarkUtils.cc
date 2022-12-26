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

#include <velox/dwio/common/Options.h>
#include <velox/dwio/common/tests/utils/DataFiles.h>

#include <boost/filesystem.hpp>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>

#include "compute/VeloxBackend.h"

DEFINE_bool(print_result, true, "Print result for execution");
DEFINE_string(write_file, "", "Write the output to parquet file, file absolute path");
DEFINE_int32(cpu, -1, "Run benchmark on specific CPU");
DEFINE_int32(threads, 0, "The number of threads to run this benchmark");
DEFINE_int32(iterations, 0, "The number of iterations to run this benchmark");

using namespace boost::filesystem;
namespace fs = std::filesystem;

std::unordered_map<std::string, std::string> confMap;

std::string getExampleFilePath(const std::string& fileName) {
  std::filesystem::path path(fileName);
  if (path.is_absolute()) {
    return fileName;
  }
  return ::facebook::velox::test::getDataFilePath("cpp/velox/benchmarks", "data/" + fileName);
}

arrow::Result<std::string> getGeneratedFilePath(const std::string& fileName) {
  std::string currentPath = fs::current_path().c_str();
  auto generatedFilePath = currentPath + "/../../../backends-velox/generated-native-benchmark/";
  fs::directory_entry filePath{generatedFilePath + fileName};
  if (filePath.exists()) {
    if (filePath.is_regular_file() && filePath.path().extension().native() == ".json") {
      // If fileName points to a regular file, it should be substrait json plan.
      return filePath.path().c_str();
    } else if (filePath.is_directory()) {
      // If fileName points to a directory, get the generated parquet data.
      auto dirItr = fs::directory_iterator(fs::path(filePath));
      for (auto& itr : dirItr) {
        if (itr.is_regular_file() && itr.path().extension().native() == ".parquet") {
          return itr.path().c_str();
        }
      }
    }
  }
  return arrow::Status::Invalid("Could not get generated file from given path: " + fileName);
}

void InitVeloxBackend() {
  gluten::SetBackendFactory([] { return std::make_shared<gluten::VeloxBackend>(confMap); });
  auto veloxInitializer = std::make_shared<gluten::VeloxInitializer>(confMap);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> getPlanFromFile(const std::string& filePath) {
  // Read json file and resume the binary data.
  std::ifstream msgJson(filePath);
  std::stringstream buffer;
  buffer << msgJson.rdbuf();
  std::string msgData = buffer.str();

  auto maybePlan = SubstraitFromJSON("Plan", msgData);
  return maybePlan;
}

std::shared_ptr<facebook::velox::substrait::SplitInfo> getFileInfos(
    const std::string& datasetPath,
    const std::string& fileFormat) {
  auto scanInfo = std::make_shared<facebook::velox::substrait::SplitInfo>();

  // Set format to scan info.
  auto format = facebook::velox::dwio::common::FileFormat::UNKNOWN;
  if (fileFormat.compare("orc") == 0) {
    format = facebook::velox::dwio::common::FileFormat::ORC;
  } else if (fileFormat.compare("parquet") == 0) {
    format = facebook::velox::dwio::common::FileFormat::PARQUET;
  }
  scanInfo->format = format;

  // Set split start, length, and path to scan info.
  path fileDir(datasetPath);
  for (auto i = directory_iterator(fileDir); i != directory_iterator(); i++) {
    if (!is_directory(i->path())) {
      std::string singleFilePath = i->path().filename().string();
      if (EndsWith(singleFilePath, "." + fileFormat)) {
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

bool CheckPathExists(const std::string& filepath) {
  std::filesystem::path f{filepath};
  return std::filesystem::exists(f);
}

void AbortIfFileNotExists(const std::string& filepath) {
  if (!CheckPathExists(filepath)) {
    std::cerr << "File path does not exist: " << filepath << std::endl;
    ::benchmark::Shutdown();
    std::exit(EXIT_FAILURE);
  }
}

bool EndsWith(const std::string& data, const std::string& suffix) {
  return data.find(suffix, data.size() - suffix.size()) != std::string::npos;
}

std::shared_ptr<arrow::RecordBatchReader> createReader(const std::string& path) {
  std::unique_ptr<::parquet::arrow::FileReader> parquetReader;
  std::shared_ptr<arrow::RecordBatchReader> recordBatchReader;
  ::parquet::ArrowReaderProperties properties = ::parquet::default_arrow_reader_properties();

  GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
      arrow::default_memory_pool(), ::parquet::ParquetFileReader::OpenFile(path), properties, &parquetReader));
  GLUTEN_THROW_NOT_OK(
      parquetReader->GetRecordBatchReader(arrow::internal::Iota(parquetReader->num_row_groups()), &recordBatchReader));
  return recordBatchReader;
}

std::shared_ptr<gluten::ResultIterator> getInputFromBatchVector(const std::string& path) {
  return std::make_shared<gluten::ResultIterator>(std::make_unique<BatchVectorIterator>(path));
}

std::shared_ptr<gluten::ResultIterator> getInputFromBatchStream(const std::string& path) {
  return std::make_shared<gluten::ResultIterator>(std::make_unique<BatchStreamIterator>(path));
}

void setCpu(uint32_t cpuindex) {
  static const auto total_cores = std::thread::hardware_concurrency();
  cpuindex = cpuindex % total_cores;
  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(cpuindex, &cs);
  if (sched_setaffinity(0, sizeof(cs), &cs) == -1) {
    std::cerr << "Error binding CPU " << std::to_string(cpuindex) << std::endl;
    exit(EXIT_FAILURE);
  }
}
