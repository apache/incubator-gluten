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

#include "ArrowTypeUtils.h"

using namespace facebook;

namespace gluten {

std::shared_ptr<arrow::DataType> toArrowType(const velox::TypePtr& type) {
  switch (type->kind()) {
    case velox::TypeKind::INTEGER:
      return arrow::int32();
    case velox::TypeKind::BIGINT:
      return arrow::int64();
    case velox::TypeKind::REAL:
      return arrow::float32();
    case velox::TypeKind::DOUBLE:
      return arrow::float64();
    case velox::TypeKind::VARCHAR:
      return arrow::utf8();
    case velox::TypeKind::VARBINARY:
      return arrow::utf8();
    case velox::TypeKind::TIMESTAMP:
      return arrow::timestamp(arrow::TimeUnit::MICRO);
    default:
      throw std::runtime_error("Type conversion is not supported.");
  }
}

std::shared_ptr<arrow::Schema> toArrowSchema(const std::shared_ptr<const velox::RowType>& rowType) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  auto size = rowType->size();
  fields.reserve(size);
  for (auto i = 0; i < size; ++i) {
    fields.push_back(arrow::field(rowType->nameOf(i), toArrowType(rowType->childAt(i))));
  }
  return arrow::schema(fields);
}

} // namespace gluten