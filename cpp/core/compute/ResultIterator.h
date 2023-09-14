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

#include "memory/ColumnarBatch.h"
#include "memory/ColumnarBatchIterator.h"
#include "utils/metrics.h"

namespace gluten {

class ExecutionCtx;

// FIXME the code is tightly coupled with Velox plan execution. Should cleanup the abstraction for uses from
//  other places.
class ResultIterator {
 public:
  explicit ResultIterator(
      std::unique_ptr<ColumnarBatchIterator> iter,
      std::shared_ptr<ExecutionCtx> executionCtx = nullptr)
      : iter_(std::move(iter)), next_(nullptr), executionCtx_(std::move(executionCtx)) {}

  // copy constructor and copy assignment (deleted)
  ResultIterator(const ResultIterator& in) = delete;
  ResultIterator& operator=(const ResultIterator&) = delete;

  // move constructor and move assignment
  ResultIterator(ResultIterator&& in) = default;
  ResultIterator& operator=(ResultIterator&& in) = default;

  bool hasNext() {
    checkValid();
    getNext();
    return next_ != nullptr;
  }

  std::shared_ptr<ColumnarBatch> next() {
    checkValid();
    getNext();
    return std::move(next_);
  }

  // For testing and benchmarking.
  ColumnarBatchIterator* getInputIter() {
    return iter_.get();
  }

  std::shared_ptr<Metrics> getMetrics();

  void setExportNanos(int64_t exportNanos) {
    exportNanos_ = exportNanos;
  }

  int64_t getExportNanos() const {
    return exportNanos_;
  }

  int64_t spillFixedSize(int64_t size) {
    return iter_->spillFixedSize(size);
  }

 private:
  void checkValid() const {
    if (iter_ == nullptr) {
      throw GlutenException("ResultIterator: the underlying iterator has expired.");
    }
  }

  void getNext() {
    if (next_ == nullptr) {
      next_ = iter_->next();
    }
  }

  std::unique_ptr<ColumnarBatchIterator> iter_;
  std::shared_ptr<ColumnarBatch> next_;
  std::shared_ptr<ExecutionCtx> executionCtx_;
  int64_t exportNanos_;
};

} // namespace gluten
