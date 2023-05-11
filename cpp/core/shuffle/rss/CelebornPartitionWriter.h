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
#include "shuffle/type.h"

#include "shuffle/PartitionWriterCreator.h"
#include "shuffle/utils.h"
#include "utils/macros.h"

namespace gluten {

class CelebornPartitionWriter : public RemotePartitionWriter {
 public:
  CelebornPartitionWriter(ShuffleWriter* shuffleWriter, std::shared_ptr<CelebornClient> celebornClient)
      : RemotePartitionWriter(shuffleWriter) {
    celebornClient_ = celebornClient;
  }

  arrow::Status init() override;

  arrow::Status evictPartition(int32_t partitionId) override;

  arrow::Status stop() override;

  arrow::Status pushPartition(int32_t partitionId);

  arrow::Status writeArrowToOutputStream(int32_t partitionId);

  std::shared_ptr<arrow::io::BufferOutputStream> celebornBufferOs_;

  std::shared_ptr<CelebornClient> celebornClient_;
};

class CelebornPartitionWriterCreator : public ShuffleWriter::PartitionWriterCreator {
 public:
  explicit CelebornPartitionWriterCreator(std::shared_ptr<CelebornClient> client);

  arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> make(ShuffleWriter* shuffleWriter) override;

 private:
  std::shared_ptr<CelebornClient> client_;
};

} // namespace gluten
