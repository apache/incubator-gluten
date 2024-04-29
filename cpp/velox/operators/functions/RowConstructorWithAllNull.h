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

#include "RowConstructorWithNull.h"

namespace gluten {
class RowConstructorWithAllNullCallToSpecialForm : public RowConstructorWithNullCallToSpecialForm {
 public:
  static constexpr const char* kRowConstructorWithAllNull = "row_constructor_with_all_null";

 protected:
  facebook::velox::exec::ExprPtr constructSpecialForm(
      const std::string& name,
      const facebook::velox::TypePtr& type,
      std::vector<facebook::velox::exec::ExprPtr>&& compiledChildren,
      bool trackCpuUsage,
      const facebook::velox::core::QueryConfig& config) {
    return constructSpecialForm(kRowConstructorWithAllNull, type, std::move(compiledChildren), trackCpuUsage, config);
  }
};
} // namespace gluten
