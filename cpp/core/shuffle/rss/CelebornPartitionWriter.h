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

#include "shuffle/rss/RemotePartitionWriter.h"

#include "jni/JniCommon.h"
#include "shuffle/PartitionWriterCreator.h"
#include "utils/macros.h"

namespace gluten {

class CelebornPartitionWriter : public RemotePartitionWriter {
 public:
  CelebornPartitionWriter(ShuffleWriter* shuffleWriter, std::shared_ptr<RssClient> celebornClient)
      : RemotePartitionWriter(shuffleWriter) {
    celebornClient_ = celebornClient;
  }

  arrow::Status init() override;

  arrow::Status processPayload(uint32_t partitionId, std::unique_ptr<arrow::ipc::IpcPayload> payload) override;

  arrow::Status spill() override;

  arrow::Status stop() override;

  arrow::Status pushPartition(int32_t partitionId, char* data, int64_t size);

  std::shared_ptr<RssClient> celebornClient_;
};

class CelebornPartitionWriterCreator : public ShuffleWriter::PartitionWriterCreator {
 public:
  explicit CelebornPartitionWriterCreator(std::shared_ptr<RssClient> client);

  arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> make(ShuffleWriter* shuffleWriter) override;

 private:
  std::shared_ptr<RssClient> client_;
};

} // namespace gluten
