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

#include "substrait_arrow.h"

#include "jni/exec_backend.h"

namespace gazellecpp {
namespace compute {

ArrowExecBackend::~ArrowExecBackend() {
  if (exec_plan_ != nullptr) {
    exec_plan_->finished().Wait();
  }
#ifdef DEBUG
  std::cout << "Plan finished" << std::endl;
#endif
}

std::shared_ptr<gazellejni::RecordBatchResultIterator>
ArrowExecBackend::GetResultIterator() {
  return GetResultIterator({});
}

std::shared_ptr<gazellejni::RecordBatchResultIterator>
ArrowExecBackend::GetResultIterator(
    std::vector<std::shared_ptr<gazellejni::RecordBatchResultIterator>> inputs) {
  GAZELLE_JNI_ASSIGN_OR_THROW(auto decls, arrow::engine::ConvertPlan(plan_));
  if (decls.size() != 1) {
    throw gazellejni::JniPendingException("Expected 1 decl, but got " +
                                          std::to_string(decls.size()));
  }
  auto& decl = decls[0];

  // Prepare and add source decls
  if (!inputs.empty()) {
    std::deque<arrow::compute::Declaration> source_decls;
    for (auto i = 0; i < inputs.size(); ++i) {
      auto it = schema_map_.find(i);
      if (it == schema_map_.end()) {
        throw gazellejni::JniPendingException(
            "Schema not found for input batch iterator " + std::to_string(i));
      }
      auto batch_it = MakeMapIterator(
          [](const std::shared_ptr<arrow::RecordBatch>& batch) {
            return arrow::util::make_optional(arrow::compute::ExecBatch(*batch));
          },
          std::move(*inputs[i]->ToArrowRecordBatchIterator()));
      GAZELLE_JNI_ASSIGN_OR_THROW(
          auto gen, arrow::MakeBackgroundGenerator(std::move(batch_it),
                                                   arrow::internal::GetCpuThreadPool()));
      source_decls.emplace_back(
          "source", arrow::compute::SourceNodeOptions{it->second, std::move(gen)});
    }
    AddSourceDecls(decl, source_decls);
  }

  // Make plan
  GAZELLE_JNI_ASSIGN_OR_THROW(exec_plan_, arrow::compute::ExecPlan::Make());
  GAZELLE_JNI_ASSIGN_OR_THROW(auto node, decl.AddToPlan(exec_plan_.get()));
  auto output_schema = node->output_schema();

  // Add sink node. It's added after constructing plan from decls because sink node
  // doesn't have output schema.
  arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>> sink_gen;
  GAZELLE_JNI_THROW_NOT_OK(arrow::compute::MakeExecNode(
      "sink", exec_plan_.get(), {node}, arrow::compute::SinkNodeOptions{&sink_gen}));

  GAZELLE_JNI_THROW_NOT_OK(exec_plan_->Validate());
  GAZELLE_JNI_THROW_NOT_OK(exec_plan_->StartProducing());

#ifdef DEBUG
  std::cout << std::string(50, '#') << " produced arrow::ExecPlan:" << std::endl;
  std::cout << exec_plan_->ToString() << std::endl;
#endif

  std::shared_ptr<arrow::RecordBatchReader> sink_reader =
      arrow::compute::MakeGeneratorReader(std::move(output_schema), std::move(sink_gen),
                                          arrow::default_memory_pool());
  return std::make_shared<gazellejni::RecordBatchResultIterator>(std::move(sink_reader),
                                                                 shared_from_this());
}

void ArrowExecBackend::AddSourceDecls(
    arrow::compute::Declaration& decl,
    std::deque<arrow::compute::Declaration>& source_decls) {
  if (decl.inputs.empty()) {
    auto need_input = std::find(no_inputs.begin(), no_inputs.end(), decl.factory_name) ==
                      no_inputs.end();
    if (need_input && !source_decls.empty()) {
      decl.inputs.emplace_back(std::move(source_decls.front()));
      source_decls.pop_front();
    }
    return;
  }
  for (auto& input : decl.inputs) {
    AddSourceDecls(arrow::util::get<arrow::compute::Declaration>(input), source_decls);
    if (source_decls.empty()) {
      return;
    }
  }
}

}  // namespace compute
}  // namespace gazellecpp
