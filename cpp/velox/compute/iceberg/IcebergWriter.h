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

#pragma once

#include "memory/VeloxColumnarBatch.h"
#include "velox/connectors/hive/iceberg/IcebergDataSink.h"

namespace gluten {

class IcebergWriter {
 public:
  IcebergWriter(
      const facebook::velox::RowTypePtr& rowType,
      int32_t format,
      const std::string& outputDirectory,
      facebook::velox::common::CompressionKind compressionKind,
      std::shared_ptr<const facebook::velox::connector::hive::iceberg::IcebergPartitionSpec> spec,
      const std::unordered_map<std::string, std::string>& sparkConfs,
      std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> connectorPool);

  void write(const VeloxColumnarBatch& batch);

  std::vector<std::string> commit();

 private:
  facebook::velox::RowTypePtr rowType_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> connectorPool_;
  std::shared_ptr<facebook::velox::connector::hive::HiveConfig> connectorConfig_;
  std::shared_ptr<facebook::velox::config::ConfigBase> connectorSessionProperties_;

  std::unique_ptr<facebook::velox::connector::ConnectorQueryCtx> connectorQueryCtx_;

  std::unique_ptr<facebook::velox::connector::hive::iceberg::IcebergDataSink> dataSink_;
};

std::shared_ptr<const facebook::velox::connector::hive::iceberg::IcebergPartitionSpec> parseIcebergPartitionSpec(
    const uint8_t* data,
    const int32_t length);
} // namespace gluten
