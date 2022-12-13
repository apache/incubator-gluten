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

using namespace facebook::velox;

std::shared_ptr<arrow::DataType> toArrowTypeFromName(const std::string& type_name) {
  if (type_name == "BOOLEAN") {
    return arrow::boolean();
  }
  if (type_name == "TINYINT") {
    return arrow::int8();
  }
  if (type_name == "SMALLINT") {
    return arrow::int16();
  }
  if (type_name == "INTEGER") {
    return arrow::int32();
  }
  if (type_name == "BIGINT") {
    return arrow::int64();
  }
  if (type_name == "REAL") {
    return arrow::float32();
  }
  if (type_name == "DOUBLE") {
    return arrow::float64();
  }
  if (type_name == "VARCHAR") {
    return arrow::utf8();
  }
  if (type_name == "VARBINARY") {
    return arrow::utf8();
  }
  if (type_name == "DATE") {
    return arrow::date32();
  }
  // The type name of Array type is like ARRAY<type>.
  std::string arrayType = "ARRAY";
  if (type_name.substr(0, arrayType.length()) == arrayType) {
    std::size_t start = type_name.find_first_of('<');
    std::size_t end = type_name.find_last_of('>');
    if (start == std::string::npos || end == std::string::npos) {
      throw std::runtime_error("Invalid array type: " + type_name);
    }
    // Extract the inner type of array type.
    auto innerType = type_name.substr(start + 1, end - start - 1);
    return arrow::list(toArrowTypeFromName(innerType));
  }

  // The type name of MAP type is like MAP<type, type>.
  std::string mapType = "MAP";
  if (type_name.substr(0, mapType.length()) == mapType) {
    std::size_t start = type_name.find_first_of('<');
    std::size_t end = type_name.find_last_of('>');
    if (start == std::string::npos || end == std::string::npos) {
      throw std::runtime_error("Invalid map type: " + type_name);
    }

    // Extract the types of map type.
    auto inner_type = type_name.substr(start + 1, end - start - 1);
    std::size_t token_pos = inner_type.find_first_of(',');
    if (token_pos == std::string::npos) {
      throw std::runtime_error("Invalid map type: " + type_name);
    }

    auto split = inner_type.substr(0, token_pos);
    // Count '<', '>' in split.
    auto num_left = std::count_if(split.begin(), split.end(), [](const auto& c) { return c == '<'; });
    auto num_right = std::count_if(split.begin(), split.end(), [](const auto& c) { return c == '>'; });
    if (num_left > num_right) {
      size_t i = token_pos + 1;
      for (; num_left > num_right && i < inner_type.length(); ++i) {
        if (inner_type[i] == '<') {
          ++num_left;
        } else if (inner_type[i] == '>') {
          ++num_right;
        }
      }
      // Next character must be ','.
      if (num_left != num_right || i >= inner_type.length() || inner_type[i] != ',') {
        throw std::runtime_error("Invalid map type: " + type_name);
      }
      token_pos = i;
    }

    auto key_type = toArrowTypeFromName(inner_type.substr(0, token_pos));
    auto value_type = toArrowTypeFromName(inner_type.substr(token_pos + 1, inner_type.length() - 1));

    return arrow::map(key_type, value_type);
  }

  // The type name of ROW type is like ROW<type, type>.
  const std::string structType = "ROW";
  if (type_name.substr(0, structType.length()) == structType) {
    std::size_t start = type_name.find_first_of('<');
    std::size_t end = type_name.find_last_of('>');
    if (start == std::string::npos || end == std::string::npos) {
      throw std::runtime_error("Invalid struct type: " + type_name);
    }

    // Extract the types of struct type.
    auto inner_type = type_name.substr(start + 1, end - start - 1);
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::size_t token_pos = inner_type.find_first_of(',');
    size_t split_start = 0;
    auto col_pos = 0;
    while (token_pos != std::string::npos && token_pos < inner_type.length()) {
      auto split = inner_type.substr(split_start, token_pos - split_start);
      // Count '<', '>' in split.
      auto num_left = std::count_if(split.begin(), split.end(), [](const auto& c) { return c == '<'; });
      auto num_right = std::count_if(split.begin(), split.end(), [](const auto& c) { return c == '>'; });
      if (num_left > num_right) {
        // Has unclosed '<'.
        size_t i = token_pos + 1;
        for (; num_left > num_right && i < inner_type.length(); ++i) {
          if (inner_type[i] == '<') {
            ++num_left;
          } else if (inner_type[i] == '>') {
            ++num_right;
          }
        }
        // If not reach to end of string, next character must be ','.
        if (num_left != num_right || i < inner_type.length() && inner_type[i] != ',') {
          throw std::runtime_error("Invalid struct type: " + type_name);
        }
        token_pos = i;
        split = inner_type.substr(split_start, token_pos - split_start);
      }
      fields.push_back(arrow::field("col_" + std::to_string(col_pos++), toArrowTypeFromName(split)));
      split_start = token_pos + 1;
      token_pos = inner_type.find(',', split_start);
    }
    if (split_start < inner_type.length()) {
      auto finalName = inner_type.substr(split_start);
      fields.push_back(arrow::field("col_" + std::to_string(col_pos), toArrowTypeFromName(finalName)));
    }
    return arrow::struct_(fields);
  }

  throw std::runtime_error("Type name is not supported: " + type_name + ".");
}

std::shared_ptr<arrow::DataType> toArrowType(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::INTEGER:
      return arrow::int32();
    case TypeKind::BIGINT:
      return arrow::int64();
    case TypeKind::REAL:
      return arrow::float32();
    case TypeKind::DOUBLE:
      return arrow::float64();
    case TypeKind::VARCHAR:
      return arrow::utf8();
    case TypeKind::VARBINARY:
      return arrow::utf8();
    case TypeKind::TIMESTAMP:
      return arrow::timestamp(arrow::TimeUnit::MICRO);
    default:
      throw std::runtime_error("Type conversion is not supported.");
  }
}

const char* arrowTypeIdToFormatStr(arrow::Type::type typeId) {
  switch (typeId) {
    case arrow::Type::type::BOOL:
      return "b"; // boolean
    case arrow::Type::type::INT32:
      return "i"; // int32
    case arrow::Type::type::INT64:
      return "l"; // int64
    case arrow::Type::type::DOUBLE:
      return "g"; // float64
    case arrow::Type::type::STRING:
      return "u"; // utf-8 string
    default:
      // Unsupported types.
      throw std::runtime_error("Arrow type id not supported.");
  }
}

std::shared_ptr<arrow::Schema> toArrowSchema(const std::shared_ptr<const RowType>& row_type) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  auto size = row_type->size();
  fields.reserve(size);
  for (auto i = 0; i < size; ++i) {
    fields.push_back(arrow::field(row_type->nameOf(i), toArrowType(row_type->childAt(i))));
  }
  return arrow::schema(fields);
}
