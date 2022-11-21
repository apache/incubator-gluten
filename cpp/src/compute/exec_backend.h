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
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/util/iterator.h>

#include "compute/protobuf_utils.h"
#include "memory/arrow_memory_pool.h"
#include "memory/columnar_batch.h"
#include "operators/c2r/arrow_columnar_to_row_converter.h"
#include "substrait/plan.pb.h"
#include "utils/exception.h"
#include "utils/metrics.h"

#ifdef GLUTEN_PRINT_DEBUG
#include <iostream>
#endif

namespace gluten {
using ArrowArrayIterator = arrow::Iterator<std::shared_ptr<ArrowArray>>;
using GlutenIterator =
    arrow::Iterator<std::shared_ptr<memory::GlutenColumnarBatch>>;
class GlutenResultIterator;

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
  virtual std::shared_ptr<GlutenResultIterator> GetResultIterator(
      gluten::memory::MemoryAllocator* allocator) = 0;
  virtual std::shared_ptr<GlutenResultIterator> GetResultIterator(
      gluten::memory::MemoryAllocator* allocator,
      std::vector<std::shared_ptr<GlutenResultIterator>> inputs) = 0;

  /// Parse and cache the plan.
  /// Return true if parsed successfully.
  bool ParsePlan(const uint8_t* data, int32_t size) {
#ifdef GLUTEN_PRINT_DEBUG
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

  const ::substrait::Plan& GetPlan() {
    return plan_;
  }

  /// This function is used to create certain converter from the format used by
  /// the backend to Spark unsafe row. By default, Arrow-to-Row converter is
  /// used.
  virtual arrow::Result<
      std::shared_ptr<gluten::columnartorow::ColumnarToRowConverterBase>>
  getColumnarConverter(
      gluten::memory::MemoryAllocator* allocator,
      std::shared_ptr<gluten::memory::GlutenColumnarBatch> cb) {
    auto memory_pool = gluten::memory::AsWrappedArrowMemoryPool(allocator);
    std::shared_ptr<ArrowSchema> c_schema = cb->exportArrowSchema();
    std::shared_ptr<ArrowArray> c_array = cb->exportArrowArray();
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::RecordBatch> rb,
        arrow::ImportRecordBatch(c_array.get(), c_schema.get()));
    ArrowSchemaRelease(c_schema.get());
    ArrowArrayRelease(c_array.get());
    return std::make_shared<gluten::columnartorow::ArrowColumnarToRowConverter>(
        rb, memory_pool);
  }

  virtual std::shared_ptr<Metrics> GetMetrics(
      void* raw_iter,
      int64_t exportNanos) {
    return nullptr;
  };

  virtual std::shared_ptr<arrow::Schema> GetOutputSchema() {
    return nullptr;
  }

 protected:
  ::substrait::Plan plan_;
};

class GlutenResultIterator
    : public ResultIteratorBase<memory::GlutenColumnarBatch> {
 public:
  /// \brief Iterator may be constructed from any type which has a member
  /// function with signature arrow::Result<std::shared_ptr<ArrowArray>> Next();
  /// and will be wrapped in ArrowArrayIterator.
  /// For details, please see <arrow/util/iterator.h>
  /// This class is used as input/output iterator for ExecBackendBase. As
  /// output, it can hold the backend to tie their lifetimes, which can be used
  /// when the production of the iterator relies on the backend.
  template <typename T>
  explicit GlutenResultIterator(
      std::shared_ptr<T> iter,
      std::shared_ptr<ExecBackendBase> backend = nullptr)
      : raw_iter_(iter.get()),
        iter_(std::make_unique<GlutenIterator>(Wrapper<T>(std::move(iter)))),
        next_(nullptr),
        backend_(std::move(backend)) {}

  bool HasNext() override {
    CheckValid();
    GetNext();
    return next_ != nullptr;
  }

  std::shared_ptr<memory::GlutenColumnarBatch> Next() override {
    CheckValid();
    GetNext();
    return std::move(next_);
  }

  /// ArrowArrayIterator doesn't support shared ownership. Once this method is
  /// called, the caller should take it's ownership, and
  /// ArrowArrayResultIterator will no longer have access to the underlying
  /// iterator.
  std::shared_ptr<ArrowArrayIterator> ToArrowArrayIterator() {
    ArrowArrayIterator itr = arrow::MakeMapIterator(
        [](std::shared_ptr<memory::GlutenColumnarBatch> b)
            -> std::shared_ptr<ArrowArray> { return b->exportArrowArray(); },
        std::move(*iter_));
    ArrowArrayIterator* itr_ptr = new ArrowArrayIterator();
    *itr_ptr = std::move(itr);
    return std::shared_ptr<ArrowArrayIterator>(itr_ptr);
  }

  // For testing and benchmarking.
  void* GetRaw() {
    return raw_iter_;
  }

  std::shared_ptr<Metrics> GetMetrics() {
    if (backend_) {
      return backend_->GetMetrics(raw_iter_, exportNanos_);
    }
    return nullptr;
  }

  void setExportNanos(int64_t exportNanos) {
    exportNanos_ = exportNanos;
  }

  int64_t getExportNanos() {
    return exportNanos_;
  }

 private:
  template <typename T>
  class Wrapper {
   public:
    explicit Wrapper(std::shared_ptr<T> ptr) : ptr_(std::move(ptr)) {}

    arrow::Result<std::shared_ptr<memory::GlutenColumnarBatch>> Next() {
      return ptr_->Next();
    }

   private:
    std::shared_ptr<T> ptr_;
  };

  void* raw_iter_;
  std::unique_ptr<GlutenIterator> iter_;
  std::shared_ptr<memory::GlutenColumnarBatch> next_;
  std::shared_ptr<ExecBackendBase> backend_;
  int64_t exportNanos_;

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
