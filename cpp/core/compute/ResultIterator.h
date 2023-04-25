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

class Backend;

class ResultIterator {
 public:
  explicit ResultIterator(std::unique_ptr<ColumnarBatchIterator> iter, std::shared_ptr<Backend> backend = nullptr)
      : iter_(std::move(iter)), next_(nullptr), backend_(std::move(backend)) {}

  bool HasNext() {
    next_ = iter_->next();
    return next_ != nullptr;
  }

  std::shared_ptr<ColumnarBatch> Next() {
    return std::move(next_);
  }

  // For testing and benchmarking.
  ColumnarBatchIterator* GetInputIter() {
    return iter_.get();
  }

  std::shared_ptr<Metrics> GetMetrics();

  void setExportNanos(int64_t exportNanos) {
    exportNanos_ = exportNanos;
  }

  int64_t getExportNanos() const {
    return exportNanos_;
  }

 private:
  std::unique_ptr<ColumnarBatchIterator> iter_;
  std::shared_ptr<ColumnarBatch> next_;
  std::shared_ptr<Backend> backend_;
  int64_t exportNanos_;
};

} // namespace gluten
