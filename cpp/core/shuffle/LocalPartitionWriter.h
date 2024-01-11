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
#include "utils/macros.h"

namespace gluten {

class LocalPartitionWriter : public PartitionWriter {
 public:
  explicit LocalPartitionWriter(
      uint32_t numPartitions,
      PartitionWriterOptions options,
      arrow::MemoryPool* pool,
      const std::string& dataFile,
      const std::vector<std::string>& localDirs);

  arrow::Status evict(
      uint32_t partitionId,
      std::unique_ptr<InMemoryPayload> inMemoryPayload,
      Evict::type evictType,
      bool reuseBuffers,
      bool hasComplexType) override;

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
  arrow::Status stop(ShuffleWriterMetrics* metrics) override;

  // Spill source:
  // 1. Other op.
  // 2. PayloadMerger merging payloads or compressing merged payload.
  // 3. After stop() called,
  arrow::Status reclaimFixedSize(int64_t size, int64_t* actual) override;

  class LocalSpiller;

  class PayloadMerger;

  class PayloadCache;

 private:
  void init();

  arrow::Status requestSpill();

  arrow::Status finishSpill();

  std::string nextSpilledFileDir();

  arrow::Status openDataFile();

  arrow::Status mergeSpills(uint32_t partitionId);

  arrow::Status clearResource();

  arrow::Status populateMetrics(ShuffleWriterMetrics* metrics);

  std::string dataFile_;
  std::vector<std::string> localDirs_;

  bool stopped_{false};
  std::shared_ptr<LocalSpiller> spiller_{nullptr};
  std::shared_ptr<PayloadMerger> merger_{nullptr};
  std::shared_ptr<PayloadCache> payloadCache_{nullptr};
  std::list<std::shared_ptr<Spill>> spills_{};

  // configured local dirs for spilled file
  int32_t dirSelection_{0};
  std::vector<int32_t> subDirSelection_;
  std::shared_ptr<arrow::io::OutputStream> dataFileOs_;

  int64_t totalBytesEvicted_{0};
  int64_t totalBytesWritten_{0};
  std::vector<int64_t> partitionLengths_;
  std::vector<int64_t> rawPartitionLengths_;
};
} // namespace gluten
