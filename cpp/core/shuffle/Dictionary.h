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

#include "memory/MemoryManager.h"

namespace gluten {

enum class BlockType : uint8_t { kEndOfStream = 0, kPlainPayload = 1, kDictionary = 2, kDictionaryPayload = 3 };

class ShuffleDictionaryStorage {
 public:
  virtual ~ShuffleDictionaryStorage() = default;

  virtual arrow::Status serialize(arrow::io::OutputStream* out) = 0;
};

class ShuffleDictionaryWriter {
 public:
  virtual ~ShuffleDictionaryWriter() = default;

  virtual arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> updateAndGet(
      const std::shared_ptr<arrow::Schema>& schema,
      int32_t numRows,
      const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) = 0;

  virtual arrow::Status serialize(arrow::io::OutputStream* out) = 0;

  virtual int64_t numDictionaryFields() = 0;

  virtual int64_t getDictionarySize() = 0;
};

using ShuffleDictionaryWriterFactory =
    std::function<std::unique_ptr<ShuffleDictionaryWriter>(MemoryManager* memoryManager, arrow::util::Codec* codec)>;

void registerShuffleDictionaryWriterFactory(ShuffleDictionaryWriterFactory factory);

std::unique_ptr<ShuffleDictionaryWriter> createDictionaryWriter(
    MemoryManager* memoryManager,
    arrow::util::Codec* codec);

} // namespace gluten
