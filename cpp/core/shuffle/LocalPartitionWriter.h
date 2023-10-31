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

#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>

#include "shuffle/PartitionWriter.h"
#include "shuffle/ShuffleWriter.h"

#include "PartitionWriterCreator.h"
#include "utils/macros.h"

namespace gluten {

struct SpillInfo {
  struct PartitionSpillInfo {
    uint32_t partitionId{};
    int64_t length{}; // in Bytes
  };

  bool empty{true};
  std::string spilledFile{};
  std::vector<PartitionSpillInfo> partitionSpillInfos{};
  std::shared_ptr<arrow::io::MemoryMappedFile> inputStream{};

  int32_t mergePos = 0;

  SpillInfo(std::string spilledFile) : spilledFile(spilledFile) {}
};

class LocalPartitionWriter : public ShuffleWriter::PartitionWriter {
 public:
  explicit LocalPartitionWriter(ShuffleWriter* shuffleWriter) : PartitionWriter(shuffleWriter) {}

  arrow::Status init() override;

  arrow::Status requestNextEvict(bool flush) override;

  EvictHandle* getEvictHandle() override;

  arrow::Status finishEvict() override;

  /// The stop function performs several tasks:
  /// 1. Opens the final data file.
  /// 2. Iterates over each partition ID (pid) to:
  ///    a. Merge data from spilled files and write to the final file.
  ///    b. Write cached payloads to the final file.
  ///    c. Create the last payload from partition buffer, and write to the final file.
  ///    d. Optionally, write End of Stream (EOS) if any payload has been written.
  ///    e. Record the offset for each partition in the final file.
  /// 3. Closes and deletes all the spilled files.
  /// 4. Records various metrics such as total write time, bytes evicted, and bytes written.
  /// 5. Clears any buffered resources and closes the final file.
  ///
  /// Spill handling:
  /// Spill is allowed during stop().
  /// Among above steps, 1. and 2.c requires memory allocation and may trigger spill.
  /// If spill is triggered by 1., cached payloads of all partitions will be spilled.
  /// If spill is triggered by 2.c, cached payloads of the remaining unmerged partitions will be spilled.
  /// In both cases, if the cached payload size doesn't free enough memory,
  /// it will shrink partition buffers to free more memory.
  arrow::Status stop() override;

  class LocalEvictHandle;

 private:
  arrow::Status setLocalDirs();

  std::string nextSpilledFileDir();

  arrow::Status openDataFile();

  arrow::Status clearResource();

  std::shared_ptr<arrow::fs::LocalFileSystem> fs_{};
  std::shared_ptr<LocalEvictHandle> evictHandle_;
  std::vector<std::shared_ptr<SpillInfo>> spills_;

  // configured local dirs for spilled file
  int32_t dirSelection_ = 0;
  std::vector<int32_t> subDirSelection_;
  std::vector<std::string> configuredDirs_;

  std::shared_ptr<arrow::io::OutputStream> dataFileOs_;
};

class LocalPartitionWriterCreator : public ShuffleWriter::PartitionWriterCreator {
 public:
  LocalPartitionWriterCreator();

  arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> make(ShuffleWriter* shuffleWriter) override;
};
} // namespace gluten
