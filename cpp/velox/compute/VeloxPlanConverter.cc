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

#include "VeloxPlanConverter.h"
#include <filesystem>

#include "compute/ResultIterator.h"
#include "config/GlutenConfig.h"
#include "iceberg/IcebergPlanConverter.h"
#include "operators/plannodes/RowVectorStream.h"
#include "velox/common/file/FileSystems.h"

namespace gluten {

using namespace facebook;

VeloxPlanConverter::VeloxPlanConverter(
    const std::vector<std::shared_ptr<ResultIterator>>& inputIters,
    velox::memory::MemoryPool* veloxPool,
    const std::unordered_map<std::string, std::string>& confMap,
    const std::optional<std::string> writeFilesTempPath,
    bool validationMode)
    : validationMode_(validationMode),
      substraitVeloxPlanConverter_(veloxPool, confMap, writeFilesTempPath, validationMode) {
  substraitVeloxPlanConverter_.setInputIters(std::move(inputIters));
}

namespace {
std::shared_ptr<SplitInfo> parseScanSplitInfo(
    const google::protobuf::RepeatedPtrField<substrait::ReadRel_LocalFiles_FileOrFiles>& fileList) {
  using SubstraitFileFormatCase = ::substrait::ReadRel_LocalFiles_FileOrFiles::FileFormatCase;

  auto splitInfo = std::make_shared<SplitInfo>();
  splitInfo->paths.reserve(fileList.size());
  splitInfo->starts.reserve(fileList.size());
  splitInfo->lengths.reserve(fileList.size());
  splitInfo->partitionColumns.reserve(fileList.size());
  splitInfo->properties.reserve(fileList.size());
  splitInfo->metadataColumns.reserve(fileList.size());
  for (const auto& file : fileList) {
    // Expect all Partitions share the same index.
    splitInfo->partitionIndex = file.partition_index();

    std::unordered_map<std::string, std::string> partitionColumnMap;
    for (const auto& partitionColumn : file.partition_columns()) {
      partitionColumnMap[partitionColumn.key()] = partitionColumn.value();
    }
    splitInfo->partitionColumns.emplace_back(partitionColumnMap);

    std::unordered_map<std::string, std::string> metadataColumnMap;
    for (const auto& metadataColumn : file.metadata_columns()) {
      metadataColumnMap[metadataColumn.key()] = metadataColumn.value();
    }
    splitInfo->metadataColumns.emplace_back(metadataColumnMap);

    splitInfo->paths.emplace_back(file.uri_file());
    splitInfo->starts.emplace_back(file.start());
    splitInfo->lengths.emplace_back(file.length());

    facebook::velox::FileProperties fileProps;
    if (file.has_properties()) {
      fileProps.fileSize = file.properties().filesize();
      fileProps.modificationTime = file.properties().modificationtime();
    }
    splitInfo->properties.emplace_back(fileProps);
    switch (file.file_format_case()) {
      case SubstraitFileFormatCase::kOrc:
        splitInfo->format = dwio::common::FileFormat::ORC;
        break;
      case SubstraitFileFormatCase::kDwrf:
        splitInfo->format = dwio::common::FileFormat::DWRF;
        break;
      case SubstraitFileFormatCase::kParquet:
        splitInfo->format = dwio::common::FileFormat::PARQUET;
        break;
      case SubstraitFileFormatCase::kText:
        splitInfo->format = dwio::common::FileFormat::TEXT;
        break;
      case SubstraitFileFormatCase::kIceberg:
        splitInfo = IcebergPlanConverter::parseIcebergSplitInfo(file, std::move(splitInfo));
        break;
      default:
        splitInfo->format = dwio::common::FileFormat::UNKNOWN;
        break;
    }
  }
  return splitInfo;
}

void parseLocalFileNodes(
    SubstraitToVeloxPlanConverter* planConverter,
    std::vector<::substrait::ReadRel_LocalFiles>& localFiles) {
  std::vector<std::shared_ptr<SplitInfo>> splitInfos;
  splitInfos.reserve(localFiles.size());
  for (int32_t i = 0; i < localFiles.size(); i++) {
    const auto& localFile = localFiles[i];
    const auto& fileList = localFile.items();

    splitInfos.push_back(parseScanSplitInfo(fileList));
  }

  planConverter->setSplitInfos(std::move(splitInfos));
}
} // namespace

std::shared_ptr<const facebook::velox::core::PlanNode> VeloxPlanConverter::toVeloxPlan(
    const ::substrait::Plan& substraitPlan,
    std::vector<::substrait::ReadRel_LocalFiles> localFiles) {
  if (!validationMode_) {
    parseLocalFileNodes(&substraitVeloxPlanConverter_, localFiles);
  }

  return substraitVeloxPlanConverter_.toVeloxPlan(substraitPlan);
}

std::string VeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

} // namespace gluten
