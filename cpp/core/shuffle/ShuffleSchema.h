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

#include <arrow/type.h>

namespace gluten {
inline std::shared_ptr<arrow::Schema> toWriteSchema(arrow::Schema& schema) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.emplace_back(std::make_shared<arrow::Field>("header", arrow::utf8()));
  for (int32_t i = 0; i < schema.num_fields(); i++) {
    switch (schema.field(i)->type()->id()) {
      case arrow::BinaryType::type_id:
      case arrow::StringType::type_id: {
        fields.emplace_back(std::make_shared<arrow::Field>("nullBuffer" + std::to_string(i), arrow::large_utf8()));
        fields.emplace_back(std::make_shared<arrow::Field>("offsetBuffer" + std::to_string(i), arrow::large_utf8()));
        fields.emplace_back(std::make_shared<arrow::Field>("valueBuffer" + std::to_string(i), arrow::large_utf8()));
      } break;
      case arrow::StructType::type_id:
      case arrow::MapType::type_id:
      case arrow::LargeListType::type_id:
      case arrow::ListType::type_id:
        // use PrestoVectorSerde at first
        fields.emplace_back(std::make_shared<arrow::Field>(std::to_string(i), arrow::large_utf8()));
        break;
      case arrow::NullType::type_id:
        break;
      default:
        fields.emplace_back(std::make_shared<arrow::Field>("nullBuffer" + std::to_string(i), arrow::large_utf8()));
        fields.emplace_back(std::make_shared<arrow::Field>("valueBuffer" + std::to_string(i), arrow::large_utf8()));
        break;
    }
  }
  return std::make_shared<arrow::Schema>(fields);
}
} // namespace gluten
