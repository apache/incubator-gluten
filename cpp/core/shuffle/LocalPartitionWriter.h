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

#include <arrow/io/api.h>

#include "shuffle/PartitionWriter.h"
#include "shuffle/ShuffleWriter.h"

#include "PartitionWriterCreator.h"
#include "utils.h"
#include "utils/macros.h"

namespace gluten {

class LocalPartitionWriterBase : public ShuffleWriter::PartitionWriter {
 protected:
  explicit LocalPartitionWriterBase(ShuffleWriter* shuffleWriter) : PartitionWriter(shuffleWriter) {}

  arrow::Status setLocalDirs();

  std::string nextSpilledFileDir();

  arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> getSchemaPayload(std::shared_ptr<arrow::Schema> schema);

  arrow::Status openDataFile();

  virtual arrow::Status clearResource();

  // configured local dirs for spilled file
  int32_t dirSelection_ = 0;
  std::vector<int32_t> subDirSelection_;
  std::vector<std::string> configuredDirs_;

  // shared among all partitions
  std::shared_ptr<arrow::ipc::IpcPayload> schemaPayload_;
  std::shared_ptr<arrow::io::OutputStream> dataFileOs_;
};

class PreferEvictPartitionWriter : public LocalPartitionWriterBase {
 public:
  explicit PreferEvictPartitionWriter(ShuffleWriter* shuffleWriter) : LocalPartitionWriterBase(shuffleWriter) {}

  arrow::Status init() override;

  arrow::Status evictPartition(int32_t partitionId) override;

  arrow::Status stop() override;

  class LocalPartitionWriterInstance;

  std::vector<std::shared_ptr<LocalPartitionWriterInstance>> partitionWriterInstances_;

 private:
  arrow::Status clearResource() override;
};

class PreferCachePartitionWriter : public LocalPartitionWriterBase {
 public:
  explicit PreferCachePartitionWriter(ShuffleWriter* shuffleWriter) : LocalPartitionWriterBase(shuffleWriter) {}

  arrow::Status init() override;

  arrow::Status evictPartition(int32_t partitionId) override;

  arrow::Status stop() override;

 private:
  arrow::Status clearResource() override;

  // TODO: helper methods
  arrow::Status writeSchemaPayload(arrow::io::OutputStream* os) {
    ARROW_ASSIGN_OR_RAISE(auto payload, getSchemaPayload(shuffleWriter_->schema()));
    int32_t metadataLength = 0; // unused
    RETURN_NOT_OK(
        arrow::ipc::WriteIpcPayload(*payload, shuffleWriter_->options().ipc_write_options, os, &metadataLength));
    return arrow::Status::OK();
  }

  arrow::Status flushCachedPayloads(
      arrow::io::OutputStream* os,
      std::vector<std::shared_ptr<arrow::ipc::IpcPayload>>& payloads) {
    int32_t metadataLength = 0; // unused
#ifndef SKIPWRITE
    for (auto& payload : payloads) {
      RETURN_NOT_OK(
          arrow::ipc::WriteIpcPayload(*payload, shuffleWriter_->options().ipc_write_options, os, &metadataLength));
      // Dismiss payload immediately
      payload = nullptr;
    }
#endif
    return arrow::Status::OK();
  }

  arrow::Status writeEos(arrow::io::OutputStream* os) {
    // write EOS
    constexpr int32_t kZeroLength = 0;
    RETURN_NOT_OK(os->Write(&kIpcContinuationToken, sizeof(int32_t)));
    RETURN_NOT_OK(os->Write(&kZeroLength, sizeof(int32_t)));
    return arrow::Status::OK();
  }

  struct PartitionSpillInfo {
    int32_t partitionId;
    int64_t start;
    int64_t length; // in Bytes
  };

  struct SpillInfo {
    std::string spilledFile;
    std::vector<PartitionSpillInfo> partitionSpillInfos;
  };

  std::vector<SpillInfo> spills_;
};

class LocalPartitionWriterCreator : public ShuffleWriter::PartitionWriterCreator {
 public:
  LocalPartitionWriterCreator(bool preferEvict);

  arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> make(ShuffleWriter* shuffleWriter) override;

 private:
  bool preferEvict_;
};
} // namespace gluten
