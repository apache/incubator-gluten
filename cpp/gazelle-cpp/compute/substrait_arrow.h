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

#include <arrow/engine/substrait/serde.h>

#include "compute/exec_backend.h"

#include <utility>

namespace gluten {

class ArrowExecBackend : public Backend {
 public:
  ArrowExecBackend();

  ~ArrowExecBackend() override;

  std::shared_ptr<gluten::GlutenResultIterator> GetResultIterator(gluten::MemoryAllocator* allocator) override;

  std::shared_ptr<gluten::GlutenResultIterator> GetResultIterator(
      gluten::MemoryAllocator* allocator,
      std::vector<std::shared_ptr<gluten::GlutenResultIterator>> inputs) override;

  std::shared_ptr<arrow::Schema> GetOutputSchema() override;

 private:
  std::shared_ptr<arrow::compute::Declaration> decl_;
  std::shared_ptr<arrow::compute::ExecPlan> exec_plan_;
  std::shared_ptr<arrow::Schema> output_schema_;
  std::unordered_map<uint64_t, std::shared_ptr<arrow::Schema>> schema_map_;

  /// Parse and get the input schema from the cached plan.
  const std::unordered_map<uint64_t, std::shared_ptr<arrow::Schema>>& GetInputSchemaMap() {
    if (schema_map_.empty()) {
      for (auto& srel : plan_.relations()) {
        if (srel.has_root()) {
          auto& sroot = srel.root();
          if (sroot.has_input()) {
            GLUTEN_THROW_NOT_OK(GetIterInputSchemaFromRel(sroot.input()));
          } else {
            throw gluten::GlutenException("Expect Rel as input.");
          }
        }
        if (srel.has_rel()) {
          GLUTEN_THROW_NOT_OK(GetIterInputSchemaFromRel(srel.rel()));
        }
      }
    }
    return schema_map_;
  }

  arrow::Result<std::shared_ptr<arrow::DataType>> subTypeToArrowType(const ::substrait::Type& stype) {
    // TODO: need to add more types here.
    switch (stype.kind_case()) {
      case ::substrait::Type::KindCase::kBool:
        return arrow::boolean();
      case ::substrait::Type::KindCase::kI32:
        return arrow::int32();
      case ::substrait::Type::KindCase::kI64:
        return arrow::int64();
      case ::substrait::Type::KindCase::kFp64:
        return arrow::float64();
      case ::substrait::Type::KindCase::kString:
        return arrow::utf8();
      case ::substrait::Type::KindCase::kDate:
        return arrow::date32();
      default:
        return arrow::Status::Invalid("Type not supported: " + std::to_string(stype.kind_case()));
    }
  }

  // This method is used to get the input schema in InputRel.
  arrow::Status GetIterInputSchemaFromRel(const ::substrait::Rel& srel);

  void ReplaceSourceDecls(std::vector<arrow::compute::Declaration> source_decls);

  void PushDownFilter();

  static void FieldPathToName(arrow::compute::Expression* expression, const std::shared_ptr<arrow::Schema>& schema);
};

class ArrowExecResultIterator {
 public:
  ArrowExecResultIterator(
      gluten::MemoryAllocator* allocator,
      std::shared_ptr<arrow::Schema> schema,
      arrow::Iterator<nonstd::optional<arrow::compute::ExecBatch>> iter)
      : memory_pool_(gluten::AsWrappedArrowMemoryPool(allocator)), schema_(std::move(schema)), iter_(std::move(iter)) {}

  std::shared_ptr<gluten::ColumnarBatch> Next();

 private:
  std::shared_ptr<arrow::MemoryPool> memory_pool_;
  std::shared_ptr<arrow::Schema> schema_;
  arrow::Iterator<nonstd::optional<arrow::compute::ExecBatch>> iter_;
  arrow::compute::ExecBatch cur_;
};

void GazelleInitialize();

} // namespace gluten
