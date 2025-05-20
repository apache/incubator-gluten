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

#include "shuffle/Dictionary.h"

#include "velox/common/memory/MemoryPool.h"
#include "velox/type/Type.h"

namespace gluten {

class DictionaryUpdater;

class VeloxShuffleDictionaryWriter : public ShuffleDictionaryWriter {
 public:
  VeloxShuffleDictionaryWriter(
      facebook::velox::memory::MemoryPool* veloxPool,
      arrow::MemoryPool* arrowPool,
      arrow::util::Codec* codec);

  arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> updateAndGet(
      const std::shared_ptr<arrow::Schema>& schema,
      int32_t numRows,
      const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) override;

  arrow::Status serialize(arrow::io::OutputStream* out) override;

 private:
  enum class FieldType { kNull, kFixedWidth, kComplex, kSupportsDictionary };

  arrow::Status initSchema(const std::shared_ptr<arrow::Schema>& schema);

  std::shared_ptr<DictionaryUpdater> dictionaryUpdater_;

  facebook::velox::RowTypePtr rowType_{nullptr};
  std::vector<FieldType> fieldTypes_{};
  std::vector<int32_t> dictionaryFields_{};
  bool hasComplexType_{false};
};
} // namespace gluten
