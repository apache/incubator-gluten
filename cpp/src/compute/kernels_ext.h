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

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/c/abi.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/iterator.h>

#include "memory/allocator.h"
#include "utils/result_iterator.h"

namespace gluten {
namespace compute {

using ArrowArrayIterator = arrow::Iterator<std::shared_ptr<ArrowArray>>;
// This class is an example shows how to get input from the iter.
// In real computing, the output of the iter should be used as the
// input for the following computing.
class LazyReadIterator {
 public:
  LazyReadIterator(std::shared_ptr<ArrowArrayIterator> array_iter) {
    array_iter_ = std::move(array_iter);
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::MakeBuilder(pool_, arrow::float64(), &array_builder);
    builder_.reset(
        arrow::internal::checked_cast<arrow::DoubleBuilder*>(array_builder.release()));
  }

  bool HasNext();

  arrow::Status Next(std::shared_ptr<ArrowArray>* out);

 private:
  arrow::MemoryPool* pool_ = gluten::memory::GetDefaultWrappedArrowMemoryPool();
  std::shared_ptr<ArrowArrayIterator> array_iter_;
  bool need_process_ = false;
  bool no_next_ = false;
  std::shared_ptr<ArrowArray> next_array_;
  std::unique_ptr<arrow::DoubleBuilder> builder_;
};

}  // namespace compute
}  // namespace gluten
