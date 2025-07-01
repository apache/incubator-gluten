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

#include "IcebergWriter.h"

#include "IcebergPartitionSpec.pb.h"
#include "compute/ProtobufUtils.h"
#include "compute/iceberg/IcebergFormat.h"
#include "utils/ConfigExtractor.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::connector::hive::iceberg;
namespace {

std::shared_ptr<connector::hive::LocationHandle> makeLocationHandle(
    std::string targetDirectory,
    std::optional<std::string> writeDirectory = std::nullopt,
    connector::hive::LocationHandle::TableType tableType = connector::hive::LocationHandle::TableType::kNew) {
  return std::make_shared<connector::hive::LocationHandle>(
      targetDirectory, writeDirectory.value_or(targetDirectory), tableType);
}

std::shared_ptr<IcebergInsertTableHandle> createIcebergInsertTableHandle(
    const RowTypePtr& outputRowType,
    const std::string& outputDirectoryPath,
    dwio::common::FileFormat fileFormat,
    facebook::velox::common::CompressionKind compressionKind,
    std::shared_ptr<const IcebergPartitionSpec> spec) {
  std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>> columnHandles;

  std::vector<std::string> columnNames = outputRowType->names();
  std::vector<TypePtr> columnTypes = outputRowType->children();

  for (auto i = 0; i < columnNames.size(); ++i) {
    columnHandles.push_back(
        std::make_shared<connector::hive::HiveColumnHandle>(
            columnNames.at(i),
            connector::hive::HiveColumnHandle::ColumnType::kRegular,
            columnTypes.at(i),
            columnTypes.at(i)));
  }
  std::shared_ptr<const connector::hive::LocationHandle> locationHandle =
      makeLocationHandle(outputDirectoryPath, std::nullopt, connector::hive::LocationHandle::TableType::kNew);

  return std::make_shared<connector::hive::iceberg::IcebergInsertTableHandle>(
      columnHandles, locationHandle, spec, fileFormat, nullptr, compressionKind);
}

} // namespace

namespace gluten {
IcebergWriter::IcebergWriter(
    const RowTypePtr& rowType,
    int32_t format,
    const std::string& outputDirectory,
    facebook::velox::common::CompressionKind compressionKind,
    std::shared_ptr<const iceberg::IcebergPartitionSpec> spec,
    const std::unordered_map<std::string, std::string>& sparkConfs,
    std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool,
    std::shared_ptr<facebook::velox::memory::MemoryPool> connectorPool)
    : rowType_(rowType), pool_(memoryPool), connectorPool_(connectorPool) {
  auto connectorSessionProperties_ = getHiveConfig(
      std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>(sparkConfs)));
  connectorConfig_ = std::make_shared<facebook::velox::connector::hive::HiveConfig>(connectorSessionProperties_);
  connectorQueryCtx_ = std::make_unique<connector::ConnectorQueryCtx>(
      pool_.get(),
      connectorPool_.get(),
      connectorSessionProperties_.get(),
      nullptr,
      common::PrefixSortConfig(),
      nullptr,
      nullptr,
      "query.IcebergDataSink",
      "task.IcebergDataSink",
      "planNodeId.IcebergDataSink",
      0,
      "");
  dataSink_ = std::make_unique<IcebergDataSink>(
      rowType_,
      createIcebergInsertTableHandle(rowType_, outputDirectory, icebergFormatToVelox(format), compressionKind, spec),
      connectorQueryCtx_.get(),
      facebook::velox::connector::CommitStrategy::kNoCommit,
      connectorConfig_);
}

void IcebergWriter::write(const VeloxColumnarBatch& batch) {
  dataSink_->appendData(batch.getRowVector());
}

std::vector<std::string> IcebergWriter::commit() {
  auto finished = dataSink_->finish();
  VELOX_CHECK(finished);
  return dataSink_->close();
}

std::shared_ptr<const iceberg::IcebergPartitionSpec> parseIcebergPartitionSpec(
    const uint8_t* data,
    const int32_t length) {
  gluten::IcebergPartitionSpec protoSpec;
  gluten::parseProtobuf(data, length, &protoSpec);
  std::vector<iceberg::IcebergPartitionField> fields;
  fields.reserve(protoSpec.fields_size());

  for (const auto& protoField : protoSpec.fields()) {
    // Convert protobuf enum to C++ enum
    iceberg::TransformType transform;
    switch (protoField.transform()) {
      case gluten::IDENTITY:
        transform = iceberg::TransformType::IDENTITY;
        break;
      case gluten::YEAR:
        transform = iceberg::TransformType::YEAR;
        break;
      case gluten::MONTH:
        transform = iceberg::TransformType::MONTH;
        break;
      case gluten::DAY:
        transform = iceberg::TransformType::DAY;
        break;
      case gluten::HOUR:
        transform = iceberg::TransformType::HOUR;
        break;
      case gluten::BUCKET:
        transform = iceberg::TransformType::BUCKET;
        break;
      case gluten::TRUNCATE:
        transform = iceberg::TransformType::TRUNCATE;
        break;
      default:
        throw std::runtime_error("Unknown transform type");
    }

    // Handle optional parameter
    std::optional<int32_t> parameter;
    if (protoField.has_parameter()) {
      parameter = protoField.parameter();
    }

    fields.emplace_back(protoField.name(), transform, parameter);
  }

  return std::make_shared<iceberg::IcebergPartitionSpec>(protoSpec.spec_id(), fields);
}

} // namespace gluten
