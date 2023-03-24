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

#include "operators/shuffle/type.h"

namespace gluten {

class SplitterBase {
 public:
  /**
   * Evict fixed size of partition data from memory
   */
  virtual arrow::Status EvictFixedSize(int64_t size, int64_t* actual) = 0;

  virtual arrow::Status Split(ColumnarBatch* cb) = 0;

  virtual arrow::Status Stop() = 0;

  int64_t TotalBytesWritten() const {
    return total_bytes_written_;
  }

  int64_t TotalBytesEvicted() const {
    return total_bytes_evicted_;
  }

  int64_t TotalWriteTime() const {
    return total_write_time_;
  }

  int64_t TotalEvictTime() const {
    return total_evict_time_;
  }

  int64_t TotalCompressTime() const {
    return total_compress_time_;
  }

  const std::vector<int64_t>& PartitionLengths() const {
    return partition_lengths_;
  }

  const std::vector<int64_t>& RawPartitionLengths() const {
    return raw_partition_lengths_;
  }

 protected:
  SplitterBase(int32_t num_partitions, SplitOptions options)
      : num_partitions_(num_partitions), options_(std::move(options)) {}
  virtual ~SplitterBase() = default;

  int32_t num_partitions_;
  // options
  SplitOptions options_;

  int64_t total_bytes_written_ = 0;
  int64_t total_bytes_evicted_ = 0;
  int64_t total_write_time_ = 0;
  int64_t total_evict_time_ = 0;
  int64_t total_compress_time_ = 0;
  int64_t peak_memory_allocated_ = 0;

  std::vector<int64_t> partition_lengths_;
  std::vector<int64_t> raw_partition_lengths_;
};
} // namespace gluten