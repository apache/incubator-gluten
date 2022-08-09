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

#include "jni/exec_backend.h"

#include <utility>

namespace gazellecpp {
namespace compute {

class ArrowExecBackend : public gluten::ExecBackendBase {
 public:
  ArrowExecBackend();

  ~ArrowExecBackend() override;

  std::shared_ptr<gluten::GlutenResultIterator> GetResultIterator(
      gluten::memory::MemoryAllocator* allocator) override;

  std::shared_ptr<gluten::GlutenResultIterator> GetResultIterator(
      gluten::memory::MemoryAllocator* allocator,
      std::vector<std::shared_ptr<gluten::GlutenResultIterator>> inputs)
      override;

 private:
  std::shared_ptr<arrow::compute::Declaration> decl_;
  std::shared_ptr<arrow::compute::ExecPlan> exec_plan_;

  void ReplaceSourceDecls(
      std::vector<arrow::compute::Declaration> source_decls);
  void PushDownFilter();
  static void FieldPathToName(
      arrow::compute::Expression* expression,
      const std::shared_ptr<arrow::Schema>& schema);
};

class ArrowExecResultIterator {
 public:
  ArrowExecResultIterator(
      std::shared_ptr<arrow::Schema> schema,
      arrow::Iterator<nonstd::optional<arrow::compute::ExecBatch>> iter)
      : schema_(std::move(schema)), iter_(std::move(iter)) {}

  std::shared_ptr<ArrowArray> Next();

 private:
  std::shared_ptr<arrow::Schema> schema_;
  arrow::Iterator<nonstd::optional<arrow::compute::ExecBatch>> iter_;
};

void Initialize();

} // namespace compute
} // namespace gazellecpp
