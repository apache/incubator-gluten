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

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <iostream>
#include <memory>
#include <sstream>

#include "utils/macros.h"

#define ASSERT_NOT_OK(status)                  \
  do {                                         \
    arrow::Status __s = (status);              \
    if (!__s.ok()) {                           \
      throw std::runtime_error(__s.message()); \
    }                                          \
  } while (false);

#define ARROW_ASSIGN_OR_THROW_IMPL(status_name, lhs, rexpr) \
  do {                                                      \
    auto status_name = (rexpr);                             \
    auto __s = status_name.status();                        \
    if (!__s.ok()) {                                        \
      throw std::runtime_error(__s.message());              \
    }                                                       \
    lhs = std::move(status_name).ValueOrDie();              \
  } while (false);

#define ARROW_ASSIGN_OR_THROW_NAME(x, y) ARROW_CONCAT(x, y)

#define ARROW_ASSIGN_OR_THROW(lhs, rexpr) \
  ARROW_ASSIGN_OR_THROW_IMPL(ARROW_ASSIGN_OR_THROW_NAME(_error_or_value, __COUNTER__), lhs, rexpr);

template <typename T>
arrow::Status equals(const T& expected, const T& actual) {
  if (expected.Equals(actual)) {
    return arrow::Status::OK();
  }
  std::stringstream ppExpected;
  std::stringstream ppActual;
  arrow::PrettyPrintOptions options(/*indent=*/2);
  options.window = 50;
  ASSERT_NOT_OK(PrettyPrint(expected, options, &ppExpected));
  ASSERT_NOT_OK(PrettyPrint(actual, options, &ppActual));
  if (ppExpected.str() == ppActual.str()) {
    return arrow::Status::OK();
  }
  return arrow::Status::Invalid(
      "Expected RecordBatch is ",
      ppExpected.str(),
      " with schema ",
      expected.schema()->ToString(),
      ", while actual is ",
      ppActual.str(),
      " with schema ",
      actual.schema()->ToString());
}

inline void makeInputBatch(
    std::vector<std::string> inputData,
    std::shared_ptr<arrow::Schema> sch,
    std::shared_ptr<arrow::RecordBatch>* inputBatch) {
  // prepare input record Batch
  std::vector<std::shared_ptr<arrow::Array>> arrayList;
  int length = -1;
  int i = 0;
  for (auto& data : inputData) {
    std::shared_ptr<arrow::Array> a0;
    ARROW_ASSIGN_OR_THROW(a0, arrow::ipc::internal::json::ArrayFromJSON(sch->field(i++)->type(), data.c_str()));
    if (length == -1) {
      length = a0->length();
    }
    assert(length == a0->length());
    arrayList.push_back(a0);
  }

  *inputBatch = arrow::RecordBatch::Make(sch, length, arrayList);
}
