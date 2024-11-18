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
#include "velox/expression/ExprToSubfieldFilter.h"

namespace gluten {

/// Parses Spark expression into subfield filter. Differences from Presto's parser include:
/// 1) Some Spark functions are registered under different names.
/// 2) The supported functions vary.
/// 3) Filter push-down on nested fields is disabled.
class SparkExprToSubfieldFilterParser : public facebook::velox::exec::ExprToSubfieldFilterParser {
 public:
  std::unique_ptr<facebook::velox::common::Filter> leafCallToSubfieldFilter(
      const facebook::velox::core::CallTypedExpr& call,
      facebook::velox::common::Subfield& subfield,
      facebook::velox::core::ExpressionEvaluator* evaluator,
      bool negated) override;

 private:
  // Compared to the upstream 'toSubfield', the push-down of filter on nested field is disabled.
  bool toSparkSubfield(const facebook::velox::core::ITypedExpr* field, facebook::velox::common::Subfield& subfield);
};
} // namespace gluten
