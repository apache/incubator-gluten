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

#include "shuffle/Payload.h"
#include "shuffle/ShuffleReader.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

class VeloxColumnarBatchDeserializer final : public ColumnarBatchIterator {
 public:
  VeloxColumnarBatchDeserializer(
      std::shared_ptr<arrow::io::InputStream> in,
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      const facebook::velox::RowTypePtr& rowType,
      int32_t batchSize,
      arrow::MemoryPool* memoryPool,
      facebook::velox::memory::MemoryPool* veloxPool,
      std::vector<bool>* isValidityBuffer,
      bool hasComplexType,
      int64_t& deserializeTime,
      int64_t& decompressTime);

  std::shared_ptr<ColumnarBatch> next();

 private:
  std::shared_ptr<arrow::io::InputStream> in_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::RowTypePtr rowType_;
  int32_t batchSize_;
  arrow::MemoryPool* memoryPool_;
  facebook::velox::memory::MemoryPool* veloxPool_;
  std::vector<bool>* isValidityBuffer_;
  bool hasComplexType_;

  int64_t& deserializeTime_;
  int64_t& decompressTime_;

  std::unique_ptr<InMemoryPayload> merged_{nullptr};
  bool reachEos_{false};
};

class VeloxColumnarBatchDeserializerFactory : public DeserializerFactory {
 public:
  VeloxColumnarBatchDeserializerFactory(
      const std::shared_ptr<arrow::Schema>& schema,
      const std::shared_ptr<arrow::util::Codec>& codec,
      const facebook::velox::RowTypePtr& rowType,
      int32_t batchSize,
      arrow::MemoryPool* memoryPool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool);

  std::unique_ptr<ColumnarBatchIterator> createDeserializer(std::shared_ptr<arrow::io::InputStream> in) override;

  arrow::MemoryPool* getPool() override;

  int64_t getDecompressTime() override;

  int64_t getDeserializeTime() override;

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::util::Codec> codec_;
  facebook::velox::RowTypePtr rowType_;
  int32_t batchSize_;
  arrow::MemoryPool* memoryPool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;

  std::vector<bool> isValidityBuffer_;
  bool hasComplexType_{false};

  int64_t deserializeTime_{0};
  int64_t decompressTime_{0};

  void initFromSchema();
};

class VeloxShuffleReader final : public ShuffleReader {
 public:
  VeloxShuffleReader(std::unique_ptr<DeserializerFactory> factory);
};
} // namespace gluten
