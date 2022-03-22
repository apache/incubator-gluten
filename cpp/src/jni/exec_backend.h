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

#include <arrow/jniutil/jni_util.h>
#include <arrow/record_batch.h>

#include "compute/protobuf_utils.h"
#include "substrait/plan.pb.h"
#include "utils/exception.h"

namespace gazellejni {

class ExecBackendBase;

template <typename T>
class ResultIteratorBase {
 public:
  virtual ~ResultIteratorBase() = default;

  virtual void Init() {}   // unused
  virtual void Close() {}  // unused
  virtual bool HasNext() = 0;
  virtual std::shared_ptr<T> Next() = 0;
};

class RecordBatchResultIterator : public ResultIteratorBase<arrow::RecordBatch> {
 public:
  /// \brief Iterator may be constructed from any type which has a member function
  /// with signature arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next();
  /// and will be wrapped in arrow::RecordBatchIterator.
  /// For details, please see <arrow/util/iterator.h>
  /// This class is used as input/output iterator for ExecBackendBase. As output,
  /// it can hold the backend to tie their lifetimes, which can be used when the
  /// production of the iterator relies on the backend.
  template <typename T>
  explicit RecordBatchResultIterator(std::shared_ptr<T> iter,
                                     std::shared_ptr<ExecBackendBase> backend = nullptr)
      : iter_(std::make_shared<arrow::RecordBatchIterator>(Wrapper<T>(std::move(iter)))),
        next_(nullptr),
        backend_(std::move(backend)) {}

  bool HasNext() override {
    GetNext();
    return next_ != nullptr;
  }

  std::shared_ptr<arrow::RecordBatch> Next() override {
    GetNext();
    return std::move(next_);
  }

  std::shared_ptr<arrow::RecordBatchIterator> ToArrowRecordBatchIterator() {
    return iter_;
  }

 private:
  template <typename T>
  class Wrapper {
   public:
    explicit Wrapper(std::shared_ptr<T> ptr) : ptr_(std::move(ptr)) {}

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() { return ptr_->Next(); }

   private:
    std::shared_ptr<T> ptr_;
  };

  std::shared_ptr<arrow::RecordBatchIterator> iter_;
  std::shared_ptr<arrow::RecordBatch> next_;
  std::shared_ptr<ExecBackendBase> backend_;

  void GetNext() {
    if (next_ == nullptr) {
      GAZELLE_JNI_ASSIGN_OR_THROW(next_, iter_->Next());
    }
  }
};

class ExecBackendBase : public std::enable_shared_from_this<ExecBackendBase> {
 public:
  virtual ~ExecBackendBase() = default;
  virtual std::shared_ptr<RecordBatchResultIterator> GetResultIterator() = 0;
  virtual std::shared_ptr<RecordBatchResultIterator> GetResultIterator(
      std::vector<std::shared_ptr<RecordBatchResultIterator>> inputs) = 0;

  /// Parse and cache the plan.
  /// Return true if parsed successfully.
  bool ParsePlan(const uint8_t* data, int32_t size) {
#ifdef DEBUG
    auto buf = std::make_shared<arrow::Buffer>(data, size);
    auto maybe_plan_json = SubstraitToJSON("Plan", *buf);
    if (maybe_plan_json.status().ok()) {
      std::cout << std::string(50, '#') << " received substrait::Plan:" << std::endl;
      std::cout << maybe_plan_json.ValueOrDie() << std::endl;
    } else {
      std::cout << "Error parsing substrait plan to json" << std::endl;
    }
#endif
    google::protobuf::io::ArrayInputStream buf_stream{data, size};
    return plan_.ParseFromZeroCopyStream(&buf_stream);
  }

  /// Parse and get the input schema from the cached plan.
  arrow::Status GetInputSchemaMap(
      std::unordered_map<uint64_t, std::shared_ptr<arrow::Schema>>& schema_map) {
    for (auto& srel : plan_.relations()) {
      if (srel.has_root()) {
        auto& sroot = srel.root();
        if (sroot.has_input()) {
          GetIterInputSchemaFromRel(sroot.input(), schema_map);
        } else {
          throw std::runtime_error("Expect Rel as input.");
        }
      }
      if (srel.has_rel()) {
        GetIterInputSchemaFromRel(srel.rel(), schema_map);
      }
    }
    return arrow::Status::OK();
  }

 protected:
  substrait::Plan plan_;

  arrow::Result<std::shared_ptr<arrow::DataType>> subTypeToArrowType(
      const substrait::Type& stype) {
    // TODO: need to add more types here.
    switch (stype.kind_case()) {
      case substrait::Type::KindCase::kBool: {
        return arrow::boolean();
      }
      case substrait::Type::KindCase::kFp64: {
        return arrow::float64();
      }
      case substrait::Type::KindCase::kString: {
        return arrow::utf8();
      }
      default:
        return arrow::Result<std::shared_ptr<arrow::DataType>>(
            arrow::Status::Invalid("Type not supported: " + stype.kind_case()));
    }
  }

 private:
  // This method is used to get the input schema in InputRel.
  arrow::Status GetIterInputSchemaFromRel(
      const substrait::Rel& srel,
      std::unordered_map<uint64_t, std::shared_ptr<arrow::Schema>>& schema_map) {
    // TODO: need to support more Substrait Rels here.
    if (srel.has_aggregate() && srel.aggregate().has_input()) {
      return GetIterInputSchemaFromRel(srel.aggregate().input(), schema_map);
    }
    if (srel.has_project() && srel.project().has_input()) {
      return GetIterInputSchemaFromRel(srel.project().input(), schema_map);
    }
    if (srel.has_filter() && srel.filter().has_input()) {
      return GetIterInputSchemaFromRel(srel.filter().input(), schema_map);
    }
    if (!srel.has_input()) {
      return arrow::Status::Invalid("Input Rel expected.");
    }
    const auto& sinput = srel.input();
    std::vector<std::string> col_name_list;
    if (!sinput.has_input_schema()) {
      return arrow::Status::Invalid("Input schema expected.");
    }
    const auto& input_schema = sinput.input_schema();
    // Get column names from input schema.
    for (const auto& name : input_schema.names()) {
      col_name_list.push_back(name);
    }
    // Get column types from input schema.
    auto& stypes = input_schema.struct_().types();
    std::vector<std::shared_ptr<arrow::DataType>> arrow_types;
    arrow_types.reserve(stypes.size());
    for (const auto& type : stypes) {
      auto type_res = subTypeToArrowType(type);
      if (!type_res.status().ok()) {
        return arrow::Status::Invalid(type_res.status().message());
      }
      arrow_types.emplace_back(std::move(type_res).ValueOrDie());
    }
    if (col_name_list.size() != arrow_types.size()) {
      return arrow::Status::Invalid("Incorrect column names or types.");
    }
    // Create input fields.
    std::vector<std::shared_ptr<arrow::Field>> input_fields;
    for (int col_idx = 0; col_idx < col_name_list.size(); col_idx++) {
      input_fields.push_back(arrow::field(col_name_list[col_idx], arrow_types[col_idx]));
    }
    // Set up the schema map.
    schema_map[sinput.iter_idx()] = arrow::schema(input_fields);
    return arrow::Status::OK();
  }
};

void SetBackendFactory(std::function<std::shared_ptr<ExecBackendBase>()> factory);

std::shared_ptr<ExecBackendBase> CreateBackend();

}  // namespace gazellejni
