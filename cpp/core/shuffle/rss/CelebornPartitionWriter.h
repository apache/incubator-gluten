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

#include "shuffle/utils.h"
#include "utils/macros.h"

namespace gluten {

class CelebornPartitionWriter : public RemotePartitionWriter {
 public:
  static arrow::Result<std::shared_ptr<CelebornPartitionWriter>> Create(
      ShuffleWriter* shuffle_writer,
      int32_t num_partitions);

 private:
  CelebornPartitionWriter(ShuffleWriter* shuffle_writer, int32_t num_partitions)
      : RemotePartitionWriter(shuffle_writer, num_partitions) {}

  arrow::Status Init() override;

  arrow::Status EvictPartition(int32_t partition_id) override;

  arrow::Status Stop() override;

  arrow::Status PushPartition(int32_t partition_id);

  arrow::Status WriteArrowToOutputStream(int32_t partition_id);

  std::shared_ptr<arrow::io::BufferOutputStream> celeborn_buffer_os_;

  std::shared_ptr<CelebornClient> celeborn_client_;
};

} // namespace gluten
