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

#include <arrow/c/abi.h>
#include <arrow/record_batch.h>
#include <arrow/util/iterator.h>

#include "compute/protobuf_utils.h"
#include "operators/c2r/arrow_columnar_to_row_converter.h"
#include "substrait/plan.pb.h"
#include "utils/exception.h"
#include "utils/result_iterator.h"

namespace gluten {

using ArrowArrayIterator = arrow::Iterator<std::shared_ptr<ArrowArray>>;
class ArrowArrayResultIterator;

template <typename T>
class ResultIteratorBase {
 public:
  virtual ~ResultIteratorBase() = default;

  virtual void Init() {} // unused
  virtual void Close() {} // unused
  virtual bool HasNext() = 0;
  virtual std::shared_ptr<T> Next() = 0;
};

class ExecBackendBase : public std::enable_shared_from_this<ExecBackendBase> {
 public:
  virtual ~ExecBackendBase() = default;
  virtual std::shared_ptr<ArrowArrayResultIterator> GetResultIterator() = 0;
  virtual std::shared_ptr<ArrowArrayResultIterator> GetResultIterator(
      std::vector<std::shared_ptr<ArrowArrayResultIterator>> inputs) = 0;

  /// Parse and cache the plan.
  /// Return true if parsed successfully.
  bool ParsePlan(const uint8_t* data, int32_t size) {
#ifdef DEBUG
    auto buf = std::make_shared<arrow::Buffer>(data, size);
    auto maybe_plan_json = SubstraitToJSON("Plan", *buf);
    if (maybe_plan_json.status().ok()) {
      std::cout << std::string(50, '#')
                << " received substrait::Plan:" << std::endl;
      std::cout << maybe_plan_json.ValueOrDie() << std::endl;
    } else {
      std::cout << "Error parsing substrait plan to json: "
                << maybe_plan_json.status().ToString() << std::endl;
    }
#endif
    return ParseProtobuf(data, size, &plan_);
  }

  /// Parse and get the input schema from the cached plan.
  const std::unordered_map<uint64_t, std::shared_ptr<arrow::Schema>>&
  GetInputSchemaMap() {
    if (schema_map_.empty()) {
      for (auto& srel : plan_.relations()) {
        if (srel.has_root()) {
          auto& sroot = srel.root();
          if (sroot.has_input()) {
            GLUTEN_THROW_NOT_OK(GetIterInputSchemaFromRel(sroot.input()));
          } else {
            throw GlutenException("Expect Rel as input.");
          }
        }
        if (srel.has_rel()) {
          GLUTEN_THROW_NOT_OK(GetIterInputSchemaFromRel(srel.rel()));
        }
      }
    }
    return schema_map_;
  }

  /// This function is used to create certain converter from the format used by
  /// the backend to Spark unsafe row. By default, Arrow-to-Row converter is
  /// used.
  virtual std::shared_ptr<gluten::columnartorow::ColumnarToRowConverterBase>
  getColumnarConverter(
      std::shared_ptr<arrow::RecordBatch> rb,
      std::shared_ptr<arrow::MemoryPool> memory_pool,
      bool wsChild) {
    return std::make_shared<gluten::columnartorow::ArrowColumnarToRowConverter>(
        rb, memory_pool);
  }

  virtual std::shared_ptr<Metrics> GetMetrics(void* raw_iter) {
    return nullptr;
  };

  virtual std::shared_ptr<arrow::Schema> GetOutputSchema() { return nullptr; }

 protected:
  ::substrait::Plan plan_;
  std::unordered_map<uint64_t, std::shared_ptr<arrow::Schema>> schema_map_;

  arrow::Result<std::shared_ptr<arrow::DataType>> subTypeToArrowType(
      const ::substrait::Type& stype) {
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
        return arrow::Status::Invalid(
            "Type not supported: " + std::to_string(stype.kind_case()));
    }
  }

 private:
  // This method is used to get the input schema in InputRel.
  arrow::Status GetIterInputSchemaFromRel(const ::substrait::Rel& srel) {
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
      std::string idxStr =
          filePath.substr(pos + prefix.size(), filePath.size());
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
      inputFields.push_back(
          arrow::field(colNameList[colIdx], arrowTypes[colIdx]));
    }

    // Set up the schema map.
    schema_map_[iterIdx] = arrow::schema(inputFields);

    return arrow::Status::OK();
  }
};

class ArrowArrayResultIterator : public ResultIteratorBase<ArrowArray> {
 public:
  /// \brief Iterator may be constructed from any type which has a member
  /// function with signature arrow::Result<std::shared_ptr<ArrowArray>> Next();
  /// and will be wrapped in ArrowArrayIterator.
  /// For details, please see <arrow/util/iterator.h>
  /// This class is used as input/output iterator for ExecBackendBase. As
  /// output, it can hold the backend to tie their lifetimes, which can be used
  /// when the production of the iterator relies on the backend.
  template <typename T>
  explicit ArrowArrayResultIterator(
      std::shared_ptr<T> iter,
      std::shared_ptr<ExecBackendBase> backend = nullptr)
      : raw_iter_(iter.get()),
        iter_(
            std::make_unique<ArrowArrayIterator>(Wrapper<T>(std::move(iter)))),
        next_(nullptr),
        backend_(std::move(backend)) {}

  bool HasNext() override {
    CheckValid();
    GetNext();
    return next_ != nullptr;
  }

  std::shared_ptr<ArrowArray> Next() override {
    CheckValid();
    GetNext();
    return std::move(next_);
  }

  /// ArrowArrayIterator doesn't support shared ownership. Once this method is
  /// called, the caller should take it's ownership, and
  /// ArrowArrayResultIterator will no longer have access to the underlying
  /// iterator.
  std::shared_ptr<ArrowArrayIterator> ToArrowArrayIterator() {
    return std::move(iter_);
  }

  // For testing and benchmarking.
  void* GetRaw() {
    return raw_iter_;
  }

  std::shared_ptr<Metrics> GetMetrics() {
    if (backend_ != nullptr) {
      return backend_->GetMetrics(raw_iter_);
    }
    return nullptr;
  }

 private:
  template <typename T>
  class Wrapper {
   public:
    explicit Wrapper(std::shared_ptr<T> ptr) : ptr_(std::move(ptr)) {}

    arrow::Result<std::shared_ptr<ArrowArray>> Next() {
      return ptr_->Next();
    }

   private:
    std::shared_ptr<T> ptr_;
  };

  void* raw_iter_;
  std::unique_ptr<ArrowArrayIterator> iter_;
  std::shared_ptr<ArrowArray> next_;
  std::shared_ptr<ExecBackendBase> backend_;

  inline void CheckValid() {
    if (iter_ == nullptr) {
      throw GlutenException(
          "ArrowExecResultIterator: the underlying iterator has expired.");
    }
  }

  inline void GetNext() {
    if (next_ == nullptr) {
      GLUTEN_ASSIGN_OR_THROW(next_, iter_->Next());
    }
  }
};

void SetBackendFactory(
    std::function<std::shared_ptr<ExecBackendBase>()> factory);

std::shared_ptr<ExecBackendBase> CreateBackend();

} // namespace gluten
