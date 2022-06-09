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

#include <arrow/status.h>

#include <stdexcept>

#define GLUTEN_THROW_NOT_OK(s)                                  \
  do {                                                          \
    ::arrow::Status _s = ::arrow::internal::GenericToStatus(s); \
    if (!_s.ok()) {                                             \
      throw gluten::GlutenException(_s.ToString());             \
    }                                                           \
  } while (0)

#define GLUTEN_ASSIGN_OR_THROW_IMPL(status_name, lhs, rexpr) \
  auto status_name = (rexpr);                                \
  GLUTEN_THROW_NOT_OK(status_name.status());                 \
  lhs = std::move(status_name).ValueOrDie();

#define GLUTEN_ASSIGN_OR_THROW(lhs, rexpr)                                              \
  GLUTEN_ASSIGN_OR_THROW_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                              lhs, rexpr);

namespace gluten {

class GlutenException : public std::runtime_error {
 public:
  explicit GlutenException(const std::string& arg) : runtime_error(arg) {}
};

}  // namespace gluten
