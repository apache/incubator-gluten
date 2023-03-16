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

#include "operators/shuffle/splitter.h"

namespace gluten {
class CelebornSplitter : public Splitter {
 public:
  static arrow::Result<std::shared_ptr<CelebornSplitter>>
  Make(const std::string& short_name, int num_partitions, SplitOptions options = SplitOptions::Defaults());

 protected:
  CelebornSplitter(int32_t num_partitions, SplitOptions options) : Splitter(num_partitions, options) {
    celeborn_client_ = std::move(options.celeborn_client);
  }

  arrow::Status Stop() override;

  arrow::Status DoSplit(const arrow::RecordBatch& rb) override;

  arrow::Status EvictFixedSize(int64_t size, int64_t* actual) override;
  /**
   * push specified partition
   */
  arrow::Status PushPartition(int32_t partition_id);

  arrow::Status Push(int32_t partition_id);

  arrow::Status WriteArrowToOutputStream(int32_t partition_id);

  /**
   * Push the largest partition buffer
   * @return partition id. If no partition to push, return -1
   */
  arrow::Result<int32_t> PushLargestPartition(int64_t* size);

  std::shared_ptr<arrow::io::BufferOutputStream> celeborn_buffer_os_;

  std::shared_ptr<CelebornClient> celeborn_client_;
};

class CelebornRoundRobinSplitter final : public CelebornSplitter {
 public:
  static arrow::Result<std::shared_ptr<CelebornRoundRobinSplitter>> Create(
      int32_t num_partitions,
      SplitOptions options);

 private:
  CelebornRoundRobinSplitter(int32_t num_partitions, SplitOptions options)
      : CelebornSplitter(num_partitions, std::move(options)) {}

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;

  int32_t pid_selection_ = 0;
};

class CelebornSinglePartSplitter final : public CelebornSplitter {
 public:
  static arrow::Result<std::shared_ptr<CelebornSinglePartSplitter>> Create(
      int32_t num_partitions,
      SplitOptions options);

 private:
  CelebornSinglePartSplitter(int32_t num_partitions, SplitOptions options)
      : CelebornSplitter(num_partitions, std::move(options)) {}

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;

  arrow::Status Split(ColumnarBatch* cb) override;

  arrow::Status Init() override;

  arrow::Status Stop() override;
};

class CelebornHashSplitter final : public CelebornSplitter {
 public:
  static arrow::Result<std::shared_ptr<CelebornHashSplitter>> Create(int32_t num_partitions, SplitOptions options);

 private:
  CelebornHashSplitter(int32_t num_partitions, SplitOptions options)
      : CelebornSplitter(num_partitions, std::move(options)) {}

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;

  arrow::Status Split(ColumnarBatch* cb) override;
};

class CelebornFallbackRangeSplitter final : public CelebornSplitter {
 public:
  static arrow::Result<std::shared_ptr<CelebornFallbackRangeSplitter>> Create(
      int32_t num_partitions,
      SplitOptions options);

  arrow::Status Split(ColumnarBatch* cb) override;

 private:
  CelebornFallbackRangeSplitter(int32_t num_partitions, SplitOptions options)
      : CelebornSplitter(num_partitions, std::move(options)) {}

  arrow::Status ComputeAndCountPartitionId(const arrow::RecordBatch& rb) override;
};

} // namespace gluten
