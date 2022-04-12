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

#include "kernels_ext.h"

#include <arrow/array/array_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>

#include <iostream>

namespace gluten {
namespace compute {

bool LazyReadIterator::HasNext() {
  if (no_next_) {
    return false;
  }
  if (need_process_) {
    // If valid batch is still not processed, no need to get a new batch.
    return true;
  }
  next_batch_ = rb_iter_->Next().ValueOrDie();
  if (next_batch_ == nullptr) {
    no_next_ = true;
    return false;
  }
  std::cout << "Input batch from the Java iter:" << std::endl;
  arrow::PrettyPrint(*next_batch_.get(), 2, &std::cout);
  need_process_ = true;
  return true;
}

arrow::Status LazyReadIterator::Next(std::shared_ptr<arrow::RecordBatch>* out) {
  double res = 900000;
  builder_->Append(res);
  std::shared_ptr<arrow::Array> array;
  auto status = builder_->Finish(&array);
  std::vector<std::shared_ptr<arrow::Field>> ret_types = {
      arrow::field("res", arrow::float64())};
  *out = arrow::RecordBatch::Make(arrow::schema(ret_types), 1, {array});
  if (need_process_) {
    need_process_ = false;
  }
  // Will return result for only once.
  no_next_ = true;
  return arrow::Status::OK();
}

}  // namespace compute
}  // namespace gluten
