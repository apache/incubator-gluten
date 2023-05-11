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

class LocalPartitionWriter : public ShuffleWriter::PartitionWriter {
 public:
  explicit LocalPartitionWriter(ShuffleWriter* shuffleWriter) : PartitionWriter(shuffleWriter) {}

  arrow::Status init() override;

  arrow::Status evictPartition(int32_t partitionId) override;

  arrow::Status stop() override;

  std::string nextSpilledFileDir();

  arrow::Result<std::shared_ptr<arrow::ipc::IpcPayload>> getSchemaPayload(std::shared_ptr<arrow::Schema> schema);

  std::string spilled_file_;
  std::shared_ptr<arrow::io::FileOutputStream> spilled_file_os_;
  std::shared_ptr<arrow::io::OutputStream> data_file_os_;
  // configured local dirs for spilled file
  int32_t dir_selection_ = 0;
  std::vector<int32_t> sub_dir_selection_;
  std::vector<std::string> configured_dirs_;

  class LocalPartitionWriterInstance;

  std::vector<std::shared_ptr<LocalPartitionWriterInstance>> partition_writer_instance_;

  // shared by all partition writers
  std::shared_ptr<arrow::ipc::IpcPayload> schema_payload_;
};

class LocalPartitionWriterCreator : public ShuffleWriter::PartitionWriterCreator {
 public:
  LocalPartitionWriterCreator();

  arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> make(ShuffleWriter* shuffleWriter) override;
};
} // namespace gluten
