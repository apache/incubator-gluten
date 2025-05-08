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

#include <arrow/array/array_decimal.h>
#include <arrow/buffer.h>
#include <arrow/buffer_builder.h>
#include <arrow/stl_allocator.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <arrow/visit_type_inline.h>

#include "shuffle/Dictionary.h"
#include "utils/Exception.h"

namespace gluten {

class ArrowDictionaryWriter : public DictionaryWriter {
 public:
  ArrowDictionaryWriter(arrow::MemoryPool* pool) : pool_(pool) {}

  arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> updateAndGet(
      const std::shared_ptr<arrow::Schema>& schema,
      int32_t numRows,
      const std::vector<std::shared_ptr<arrow::Buffer>>& buffers) override;

  arrow::Status serialize(arrow::io::OutputStream* out) override;

 private:
  enum class FieldType { kNull, kFixedWidth, kComplex, kSupportsDictionary };

  arrow::Status initSchema(const std::shared_ptr<arrow::Schema>& schema);

  arrow::MemoryPool* pool_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<FieldType> fieldTypes_;
  std::vector<int32_t> dictionaryFields_;
  bool hasComplexType_{false};
  std::unordered_map<int32_t, std::shared_ptr<DictionaryStorage>> dictionaries_;

  friend class ValueUpdater;
};
} // namespace gluten