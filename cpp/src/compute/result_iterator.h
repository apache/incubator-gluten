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

#include <memory>

#include "compute/transfer_iterator.h"
#include "memory/columnar_batch.h"
#include "utils/metrics.h"

#ifdef GLUTEN_PRINT_DEBUG
#include <iostream>
#endif

namespace gluten {

class Backend;

using ArrowArrayIterator = arrow::Iterator<std::shared_ptr<ArrowArray>>;
using ColumnBatchIterator = arrow::Iterator<std::shared_ptr<ColumnarBatch>>;

class ResultIterator {
 public:
  virtual ~ResultIterator() = default;
  virtual bool HasNext() = 0;
  virtual std::shared_ptr<ColumnarBatch> Next() = 0;
  virtual std::shared_ptr<ArrowArrayIterator> ToArrowArrayIterator() = 0;
  virtual std::unique_ptr<TransferIterator> ToTransferIterator() = 0;
  virtual void* GetRaw() = 0;
  virtual const void* GetRaw() const = 0;
  virtual std::shared_ptr<Metrics> GetMetrics() = 0;
  virtual void setExportNanos(int64_t exportNanos) = 0;
  virtual int64_t getExportNanos() const = 0;
};

template <typename Iterator> inline
std::shared_ptr<ColumnarBatch> IteratorNext(void* iterator) {
  return ((Iterator*)iterator)->Next();
}

template <typename Iterator> inline 
void IteratorDelete(void* p) {
  delete (Iterator*)p;
}

class GlutenResultIterator : public ResultIterator {
 public:
  template <typename Iterator>
  explicit GlutenResultIterator(std::unique_ptr<Iterator>&& iter, std::shared_ptr<Backend> backend = nullptr)
      : raw_iterator_(iter.get()),
      iterator_(iter.release(), IteratorDelete<Iterator>),
      iterator_next_fn_(IteratorNext<Iterator>),
      next_(nullptr),
      backend_(std::move(backend)) {}

  bool HasNext() override {
    CheckValid();
    GetNext();
    return next_ != nullptr;
  }

  std::shared_ptr<ColumnarBatch> Next() override {
    CheckValid();
    GetNext();
    return std::move(next_);
  }

  std::unique_ptr<TransferIterator> ToTransferIterator() override {
    return std::make_unique<TransferIterator>(raw_iterator_, std::move(iterator_), iterator_next_fn_);
  }

  /// ArrowArrayIterator doesn't support shared ownership. Once this method is
  /// called, the caller should take it's ownership, and
  /// ArrowArrayResultIterator will no longer have access to the underlying
  /// iterator.
  std::shared_ptr<ArrowArrayIterator> ToArrowArrayIterator() override {
#if 0
    ArrowArrayIterator itr = arrow::MakeMapIterator(
        [](std::shared_ptr<ColumnarBatch> b) -> std::shared_ptr<ArrowArray> { return b->exportArrowArray(); },
        std::move(*iter_));
    ArrowArrayIterator* itr_ptr = new ArrowArrayIterator();
    *itr_ptr = std::move(itr);
    return std::shared_ptr<ArrowArrayIterator>(itr_ptr);
#else
    return nullptr;
#endif
  }

  // For testing and benchmarking.
  void* GetRaw() override {
    return raw_iterator_;
  }

  const void* GetRaw() const override {
    return raw_iterator_;
  }

  std::shared_ptr<Metrics> GetMetrics() override {
    if (backend_) {
      return backend_->GetMetrics(raw_iterator_, exportNanos_);
    }
    return nullptr;
  }

  void setExportNanos(int64_t exportNanos) override {
    exportNanos_ = exportNanos;
  }

  int64_t getExportNanos() const override {
    return exportNanos_;
  }

 private:
  bool CheckValid() const {
    if (iterator_ == nullptr) {
      throw GlutenException("ArrowExecResultIterator: the underlying iterator has expired.");
    }
    return iterator_ != nullptr;
  }

  void GetNext() {
    if (next_ == nullptr) {
      next_ = (*iterator_next_fn_)(raw_iterator_);
    }
  }

  void* raw_iterator_;
  std::unique_ptr<void, void (*)(void*)> iterator_;
  std::shared_ptr<ColumnarBatch> (*iterator_next_fn_)(void*);
  std::shared_ptr<ColumnarBatch> next_;
  std::shared_ptr<Backend> backend_;
  int64_t exportNanos_;
};

} // namespace gluten
