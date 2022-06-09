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

#include <velox/dwio/dwrf/test/utils/DataFiles.h>

#include <boost/filesystem.hpp>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "compute/VeloxPlanConverter.h"

using namespace boost::filesystem;
namespace fs = std::filesystem;

std::string getExampleFilePath(const std::string& fileName) {
  return ::facebook::velox::test::getDataFilePath("cpp/velox/benchmarks",
                                                  "data/" + fileName);
}

void InitVeloxBackend() {
  gluten::SetBackendFactory(
      [] { return std::make_shared<::velox::compute::VeloxPlanConverter>(); });
  auto veloxInitializer = std::make_shared<::velox::compute::VeloxInitializer>();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> readFromFile(const std::string& filePath) {
  // Read json file and resume the binary data.
  std::ifstream msgJson(filePath);
  std::stringstream buffer;
  buffer << msgJson.rdbuf();
  std::string msgData = buffer.str();

  auto maybePlan = SubstraitFromJSON("Plan", msgData);
  return maybePlan;
}

void getFileInfos(const std::string datasetPath, const std::string fileFormat,
                  std::vector<std::string>& paths, std::vector<u_int64_t>& starts,
                  std::vector<u_int64_t>& lengths) {
  path fileDir(datasetPath);
  for (auto i = directory_iterator(fileDir); i != directory_iterator(); i++) {
    if (!is_directory(i->path())) {
      std::string singleFilePath = i->path().filename().string();
      if (EndsWith(singleFilePath, "." + fileFormat)) {
        auto fileAbsolutePath = datasetPath + singleFilePath;
        starts.push_back(0);
        lengths.push_back(fs::file_size(fileAbsolutePath));
        paths.push_back("file://" + fileAbsolutePath);
      }
    } else {
      continue;
    }
  }
}

bool EndsWith(const std::string& data, const std::string& suffix) {
  return data.find(suffix, data.size() - suffix.size()) != std::string::npos;
}
