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

#include "memory/ColumnarBatch.h"
#include "utils/metrics.h"

namespace gluten {

class Backend;

using ArrowArrayIterator = arrow::Iterator<std::shared_ptr<ArrowArray>>;
using ColumnBatchIterator = arrow::Iterator<std::shared_ptr<ColumnarBatch>>;

class ResultIterator {
 public:
  template <typename T>
  explicit ResultIterator(std::unique_ptr<T>&& iter, std::shared_ptr<Backend> backend = nullptr)
      : raw_iter_(iter.get()),
        iter_(std::make_unique<ColumnBatchIterator>(Wrapper<T>(std::move(iter)))),
        next_(nullptr),
        backend_(std::move(backend)) {}

  bool HasNext() {
    CheckValid();
    GetNext();
    return next_ != nullptr;
  }

  std::shared_ptr<ColumnarBatch> Next() {
    CheckValid();
    GetNext();
    return std::move(next_);
  }

  // For testing and benchmarking.
  void* GetRaw() {
    return raw_iter_;
  }

  const void* GetRaw() const {
    return raw_iter_;
  }

  std::shared_ptr<Metrics> GetMetrics();

  void setExportNanos(int64_t exportNanos) {
    exportNanos_ = exportNanos;
  }

  int64_t getExportNanos() const {
    return exportNanos_;
  }

 private:
  void CheckValid() const {
    if (iter_ == nullptr) {
      throw GlutenException("ArrowExecResultIterator: the underlying iterator has expired.");
    }
  }

  void GetNext() {
    if (next_ == nullptr) {
      GLUTEN_ASSIGN_OR_THROW(next_, iter_->Next());
    }
  }

  template <typename T>
  class Wrapper {
   public:
    explicit Wrapper(std::shared_ptr<T> ptr) : ptr_(std::move(ptr)) {}

    arrow::Result<std::shared_ptr<ColumnarBatch>> Next() {
      return ptr_->Next();
    }

   private:
    std::shared_ptr<T> ptr_;
  };

  void* raw_iter_;
  std::unique_ptr<ColumnBatchIterator> iter_;
  std::shared_ptr<ColumnarBatch> next_;
  std::shared_ptr<Backend> backend_;
  int64_t exportNanos_;
};

} // namespace gluten
