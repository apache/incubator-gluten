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

#include <arrow/c/bridge.h>
#include <arrow/compute/exec/options.h>
#include <arrow/compute/registry.h>
#include <arrow/dataset/plan.h>
#include <arrow/dataset/scanner.h>
#include <arrow/type_fwd.h>
#include <arrow/util/async_generator.h>

#include "compute/exec_backend.h"

namespace gluten {

const arrow::FieldVector kAugmentedFields{
    field("__fragment_index", arrow::int32()),
    field("__batch_index", arrow::int32()),
    field("__last_in_fragment", arrow::boolean()),
    field("__filename", arrow::utf8()),
};

ArrowExecBackend::ArrowExecBackend() {
  arrow::dataset::internal::Initialize();
}

ArrowExecBackend::~ArrowExecBackend() {
  if (exec_plan_ != nullptr) {
    exec_plan_->finished().Wait();
  }
#ifdef GLUTEN_PRINT_DEBUG
  std::cout << "Plan finished" << std::endl;
#endif
}

std::shared_ptr<gluten::GlutenResultIterator> ArrowExecBackend::GetResultIterator(gluten::MemoryAllocator* allocator) {
  return GetResultIterator(allocator, {});
}

std::shared_ptr<gluten::GlutenResultIterator> ArrowExecBackend::GetResultIterator(
    gluten::MemoryAllocator* allocator,
    std::vector<std::shared_ptr<gluten::GlutenResultIterator>> inputs) {
  GLUTEN_ASSIGN_OR_THROW(auto decls, arrow::engine::ConvertPlan(plan_));
  if (decls.size() != 1) {
    throw gluten::GlutenException("Expected 1 decl, but got " + std::to_string(decls.size()));
  }
  decl_ = std::make_shared<arrow::compute::Declaration>(std::move(decls[0]));

  // Prepare and add source decls
  if (!inputs.empty()) {
    std::vector<arrow::compute::Declaration> source_decls;
    GetInputSchemaMap();
    for (auto i = 0; i < inputs.size(); ++i) {
      auto it = schema_map_.find(i);
      if (it == schema_map_.end()) {
        throw gluten::GlutenException("Schema not found for input batch iterator " + std::to_string(i));
      }
      auto schema = it->second;
      auto batch_it = MakeMapIterator(
          [schema](const std::shared_ptr<ArrowArray>& array) {
            GLUTEN_ASSIGN_OR_THROW(auto batch, arrow::ImportRecordBatch(array.get(), schema));
            return arrow::util::make_optional(arrow::compute::ExecBatch(*batch));
          },
          std::move(*inputs[i]->ToArrowArrayIterator()));
      GLUTEN_ASSIGN_OR_THROW(
          auto gen, arrow::MakeBackgroundGenerator(std::move(batch_it), arrow::internal::GetCpuThreadPool()));
      source_decls.emplace_back("source", arrow::compute::SourceNodeOptions{schema, std::move(gen)});
    }
    ReplaceSourceDecls(std::move(source_decls));
  }

  PushDownFilter();

  // Make plan
  GLUTEN_ASSIGN_OR_THROW(exec_plan_, arrow::compute::ExecPlan::Make());
  GLUTEN_ASSIGN_OR_THROW(auto node, decl_->AddToPlan(exec_plan_.get()));

  auto include_aug_fields = arrow::FieldRef("__fragment_index").FindOne(*node->output_schema());
  if (include_aug_fields.ok()) {
    std::vector<arrow::compute::Expression> fields;
    auto num_fields = node->output_schema()->num_fields() - kAugmentedFields.size();
    fields.reserve(num_fields);
    for (int i = 0; i < num_fields; ++i) {
      fields.push_back(arrow::compute::field_ref(i));
    }
    GLUTEN_ASSIGN_OR_THROW(
        node,
        arrow::compute::MakeExecNode(
            "project", exec_plan_.get(), {node}, arrow::compute::ProjectNodeOptions(std::move(fields))));
  }

  output_schema_ = node->output_schema();

  // Add sink node. It's added after constructing plan from decls because sink
  // node doesn't have output schema.
  arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>> sink_gen;
  GLUTEN_THROW_NOT_OK(
      arrow::compute::MakeExecNode("sink", exec_plan_.get(), {node}, arrow::compute::SinkNodeOptions{&sink_gen}));

  GLUTEN_THROW_NOT_OK(exec_plan_->Validate());
  GLUTEN_THROW_NOT_OK(exec_plan_->StartProducing());

#ifdef GLUTEN_PRINT_DEBUG
  std::cout << std::string(50, '#') << " produced arrow::ExecPlan:" << std::endl;
  std::cout << exec_plan_->ToString() << std::endl;
  std::cout << "Execplan output schema:" << std::endl << output_schema_->ToString() << std::endl;
#endif

  auto iter = arrow::MakeGeneratorIterator(std::move(sink_gen));
  auto sink_iter = std::make_shared<ArrowExecResultIterator>(allocator, output_schema_, std::move(iter));

  return std::make_shared<gluten::GlutenResultIterator>(std::move(sink_iter), shared_from_this());
}

std::shared_ptr<arrow::Schema> ArrowExecBackend::GetOutputSchema() {
  return output_schema_;
}

void ArrowExecBackend::PushDownFilter() {
  std::vector<arrow::compute::Declaration*> visited;

  visited.push_back(decl_.get());

  while (!visited.empty()) {
    auto top = visited.back();
    visited.pop_back();
    for (auto& input : top->inputs) {
      auto& input_decl = arrow::util::get<arrow::compute::Declaration>(input);
      if (input_decl.factory_name == "filter" && input_decl.inputs.size() == 1) {
        auto scan_decl = arrow::util::get<arrow::compute::Declaration>(input_decl.inputs[0]);
        if (scan_decl.factory_name == "scan") {
          auto expression = arrow::internal::checked_pointer_cast<arrow::compute::FilterNodeOptions>(input_decl.options)
                                ->filter_expression;
          auto scan_options = arrow::internal::checked_pointer_cast<arrow::dataset::ScanNodeOptions>(scan_decl.options);
          const auto& schema = scan_options->dataset->schema();
          FieldPathToName(&expression, schema);
          scan_options->scan_options->filter = std::move(expression);
          continue;
        }
      }
      visited.push_back(&input_decl);
    }
  }
}

// This method is used to get the input schema in InputRel.
arrow::Status ArrowExecBackend::GetIterInputSchemaFromRel(const ::substrait::Rel& srel) {
  // TODO: need to support more Substrait Rels here.
  if (srel.has_aggregate() && srel.aggregate().has_input()) {
    return GetIterInputSchemaFromRel(srel.aggregate().input());
  }
  if (srel.has_project() && srel.project().has_input()) {
    return GetIterInputSchemaFromRel(srel.project().input());
  }
  if (srel.has_filter() && srel.filter().has_input()) {
    return GetIterInputSchemaFromRel(srel.filter().input());
  }
  if (srel.has_join()) {
    if (srel.join().has_left() && srel.join().has_right()) {
      RETURN_NOT_OK(GetIterInputSchemaFromRel(srel.join().left()));
      RETURN_NOT_OK(GetIterInputSchemaFromRel(srel.join().right()));
      return arrow::Status::OK();
    }
    return arrow::Status::Invalid("Incomplete Join Rel.");
  }
  if (!srel.has_read()) {
    return arrow::Status::Invalid("Read Rel expected.");
  }
  const auto& sread = srel.read();

  // Get the iterator index.
  int iterIdx = -1;
  if (sread.has_local_files()) {
    const auto& fileList = sread.local_files().items();
    if (fileList.size() == 0) {
      return arrow::Status::Invalid("At least one file path is expected.");
    }
    std::string filePath = fileList[0].uri_file();
    std::string prefix = "iterator:";
    std::size_t pos = filePath.find(prefix);
    if (pos == std::string::npos) {
      // This is not an iterator input, but a scan input.
      return arrow::Status::OK();
    }
    std::string idxStr = filePath.substr(pos + prefix.size(), filePath.size());
    iterIdx = std::stoi(idxStr);
  }
  if (iterIdx < 0) {
    return arrow::Status::Invalid("Invalid iterator index.");
  }

  // Construct the input schema.
  if (!sread.has_base_schema()) {
    return arrow::Status::Invalid("Base schema expected.");
  }
  const auto& baseSchema = sread.base_schema();
  // Get column names from input schema.
  std::vector<std::string> colNameList;
  for (const auto& name : baseSchema.names()) {
    colNameList.push_back(name);
  }
  // Get column types from input schema.
  const auto& stypes = baseSchema.struct_().types();
  std::vector<std::shared_ptr<arrow::DataType>> arrowTypes;
  arrowTypes.reserve(stypes.size());
  for (const auto& type : stypes) {
    auto typeRes = subTypeToArrowType(type);
    if (!typeRes.status().ok()) {
      return arrow::Status::Invalid(typeRes.status().message());
    }
    arrowTypes.emplace_back(std::move(typeRes).ValueOrDie());
  }
  if (colNameList.size() != arrowTypes.size()) {
    return arrow::Status::Invalid("Incorrect column names or types.");
  }
  // Create input fields.
  std::vector<std::shared_ptr<arrow::Field>> inputFields;
  for (int colIdx = 0; colIdx < colNameList.size(); colIdx++) {
    inputFields.push_back(arrow::field(colNameList[colIdx], arrowTypes[colIdx]));
  }

  // Set up the schema map.
  schema_map_[iterIdx] = arrow::schema(inputFields);

  return arrow::Status::OK();
}

void ArrowExecBackend::FieldPathToName(
    arrow::compute::Expression* expression,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::vector<arrow::compute::Expression*> visited;

  visited.push_back(expression);

  while (!visited.empty()) {
    auto expr = visited.back();
    visited.pop_back();
    if (expr->call()) {
      auto call = const_cast<arrow::compute::Expression::Call*>(expr->call());
      std::transform(
          call->arguments.begin(),
          call->arguments.end(),
          std::back_inserter(visited),
          [](arrow::compute::Expression& arg) { return &arg; });
    } else if (expr->field_ref()) {
      auto field_ref = const_cast<arrow::FieldRef*>(expr->field_ref());
      if (auto field_path = field_ref->field_path()) {
        *expr = arrow::compute::field_ref(schema->field((field_path->indices())[0])->name());
      } else {
        throw gluten::GlutenException("Field Ref is not field path: " + field_ref->ToString());
      }
    }
  }
}

void ArrowExecBackend::ReplaceSourceDecls(std::vector<arrow::compute::Declaration> source_decls) {
  std::vector<arrow::compute::Declaration*> visited;
  std::vector<arrow::compute::Declaration*> source_indexes;

  visited.push_back(decl_.get());

  while (!visited.empty()) {
    auto top = visited.back();
    visited.pop_back();
    for (auto& input : top->inputs) {
      auto& input_decl = arrow::util::get<arrow::compute::Declaration>(input);
      if (input_decl.factory_name == "source_index") {
        source_indexes.push_back(&input_decl);
      } else {
        visited.push_back(&input_decl);
      }
    }
  }

  if (source_indexes.size() != source_decls.size()) {
    throw gluten::GlutenException(
        "Wrong number of source declarations. " + std::to_string(source_indexes.size()) +
        " source(s) needed by source declarations, but got " + std::to_string(source_decls.size()) +
        " from input batches.");
  }

  for (auto& source_index : source_indexes) {
    auto index =
        arrow::internal::checked_pointer_cast<arrow::compute::SourceIndexOptions>(source_index->options)->index;
    *source_index = std::move(source_decls[index]);
  }
}

std::shared_ptr<gluten::ColumnarBatch> ArrowExecResultIterator::Next() {
  GLUTEN_ASSIGN_OR_THROW(auto exec_batch, iter_.Next());
  if (exec_batch.has_value()) {
    cur_ = std::move(exec_batch.value());
    arrow::ArrayDataVector columns(schema_->num_fields());
    for (size_t i = 0; i < columns.size(); ++i) {
      auto type = schema_->field(i)->type();
      auto layout = type->layout();
      const arrow::Datum& value = cur_.values[i];
      if (value.is_array()) {
        auto array = value.make_array();
        if (array->offset() == 0) {
          columns[i] = array->data();
        } else {
          auto offset = array->offset();
          DCHECK_EQ(offset % 8, 0);

          const auto& in_data = array->data();
          DCHECK_EQ(in_data->buffers.size(), layout.buffers.size());
          std::vector<std::shared_ptr<arrow::Buffer>> out_buffers(in_data->buffers.size());

          // Process null bitmap
          DCHECK_GT(layout.buffers.size(), 0);
          DCHECK_EQ(layout.buffers[0].kind, arrow::DataTypeLayout::BITMAP);
          if (in_data->buffers[0] == nullptr) {
            out_buffers[0] = nullptr;
          } else {
            out_buffers[0] = std::make_shared<arrow::Buffer>(
                in_data->buffers[0]->data() + (offset >> 3), arrow::bit_util::BytesForBits(in_data->length));
          }

          // Process data/offset buffer
          if (layout.buffers.size() > 1) {
            DCHECK_EQ(layout.buffers[1].kind, arrow::DataTypeLayout::FIXED_WIDTH);
            auto byte_width = layout.buffers[1].byte_width;
            if (in_data->buffers[1] == nullptr) {
              out_buffers[1] = nullptr;
            } else {
              out_buffers[1] = std::make_shared<arrow::Buffer>(
                  in_data->buffers[1]->data() + offset * byte_width, in_data->length * byte_width);
            }
          }

          // Process data buffer for variable length types
          if (layout.buffers.size() > 2) {
            DCHECK_EQ(layout.buffers[2].kind, arrow::DataTypeLayout::VARIABLE_WIDTH);
            out_buffers[2] = in_data->buffers[2];
          }

          columns[i] = arrow::ArrayData::Make(type, array->length(), std::move(out_buffers), array->null_count(), 0);
        }
      } else {
        GLUTEN_ASSIGN_OR_THROW(auto scalar, MakeArrayFromScalar(*value.scalar(), cur_.length, memory_pool_.get()));
        columns[i] = scalar->data();
      }
    }
    auto batch = arrow::RecordBatch::Make(schema_, cur_.length, columns);

    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
    GLUTEN_THROW_NOT_OK(arrow::ExportRecordBatch(*batch, c_array.get(), c_schema.get()));
    return std::make_shared<gluten::ArrowCStructColumnarBatch>(std::move(c_schema), std::move(c_array));
  }
  return nullptr;
}

void GazelleInitialize() {
  static auto function_registry = arrow::compute::GetFunctionRegistry();
  static auto extension_registry = arrow::engine::default_extension_id_registry();
  if (function_registry && extension_registry) {
    // TODO: Register customized functions to function_registry, and register
    // the mapping from substrait function names to customized function names to
    // extension_registry.
    function_registry = nullptr;
    extension_registry = nullptr;
  }
}

} // namespace gluten
