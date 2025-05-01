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

#include "memory/ColumnarBatch.h"
#include "memory/Reclaimable.h"
#include "shuffle/Options.h"
#include "shuffle/PartitionWriter.h"
#include "shuffle/Partitioner.h"

namespace gluten {

class ShuffleWriter : public Reclaimable {
 public:
  static constexpr int64_t kMinMemLimit = 128LL * 1024 * 1024;

  static constexpr int64_t kMaxMemLimit = 1LL * 1024 * 1024 * 1024;

  static ShuffleWriterType stringToType(const std::string& typeString);

  static std::string typeToString(ShuffleWriterType type);

  virtual arrow::Status write(std::shared_ptr<ColumnarBatch> cb, int64_t memLimit) = 0;

  virtual arrow::Status stop() = 0;

  int32_t numPartitions() const;

  ShuffleWriterOptions& options();

  int64_t totalBytesWritten() const;

  int64_t totalBytesEvicted() const;

  int64_t totalBytesToEvict() const;

  int64_t totalWriteTime() const;

  int64_t totalEvictTime() const;

  int64_t totalCompressTime() const;

  virtual int64_t peakBytesAllocated() const;

  virtual int64_t totalSortTime() const;

  virtual int64_t totalC2RTime() const;

  const std::vector<int64_t>& partitionLengths() const;

  const std::vector<int64_t>& rawPartitionLengths() const;

 protected:
  ShuffleWriter(int32_t numPartitions, ShuffleWriterOptions options, arrow::MemoryPool* pool);

  ~ShuffleWriter() override = default;

  int32_t numPartitions_;

  ShuffleWriterOptions options_;

  arrow::MemoryPool* pool_;

  ShuffleWriterMetrics metrics_{};
};

} // namespace gluten
