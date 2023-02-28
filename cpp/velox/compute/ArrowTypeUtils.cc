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

std::shared_ptr<arrow::DataType> toArrowTypeFromName(const std::string& typeName) {
  if (typeName == "BOOLEAN") {
    return arrow::boolean();
  }
  if (typeName == "TINYINT") {
    return arrow::int8();
  }
  if (typeName == "SMALLINT") {
    return arrow::int16();
  }
  if (typeName == "INTEGER") {
    return arrow::int32();
  }
  if (typeName == "BIGINT") {
    return arrow::int64();
  }
  if (typeName == "REAL") {
    return arrow::float32();
  }
  if (typeName == "DOUBLE") {
    return arrow::float64();
  }
  if (typeName == "VARCHAR") {
    return arrow::utf8();
  }
  if (typeName == "VARBINARY") {
    return arrow::utf8();
  }
  if (typeName == "DATE") {
    return arrow::date32();
  }
  // The type name of Array type is like ARRAY<type>.
  const std::string arrayType = "ARRAY";
  if (typeName.substr(0, arrayType.length()) == arrayType) {
    std::size_t start = typeName.find_first_of('<');
    std::size_t end = typeName.find_last_of('>');
    if (start == std::string::npos || end == std::string::npos) {
      throw std::runtime_error("Invalid array type: " + typeName);
    }
    // Extract the inner type of array type.
    auto innerType = typeName.substr(start + 1, end - start - 1);
    return arrow::list(toArrowTypeFromName(innerType));
  }

  // The type name of MAP type is like MAP<type, type>.
  const std::string mapType = "MAP";
  if (typeName.substr(0, mapType.length()) == mapType) {
    std::size_t start = typeName.find_first_of('<');
    std::size_t end = typeName.find_last_of('>');
    if (start == std::string::npos || end == std::string::npos) {
      throw std::runtime_error("Invalid map type: " + typeName);
    }

    // Extract the types of map type.
    auto innerType = typeName.substr(start + 1, end - start - 1);
    std::size_t tokenPos = innerType.find_first_of(',');
    if (tokenPos == std::string::npos) {
      throw std::runtime_error("Invalid map type: " + typeName);
    }

    auto part = innerType.substr(0, tokenPos);
    // Count '<', '>' in part.
    auto numLeft = std::count_if(part.begin(), part.end(), [](const auto& c) { return c == '<'; });
    auto numRight = std::count_if(part.begin(), part.end(), [](const auto& c) { return c == '>'; });
    if (numLeft > numRight) {
      // Has unclosed '<'.
      std::size_t i = tokenPos + 1;
      for (; numLeft > numRight && i < innerType.length(); ++i) {
        if (innerType[i] == '<') {
          ++numLeft;
        } else if (innerType[i] == '>') {
          ++numRight;
        }
      }
      // Next character must be ','.
      if (numLeft != numRight || i >= innerType.length() || innerType[i] != ',') {
        throw std::runtime_error("Invalid map type: " + typeName);
      }
      tokenPos = i;
      part = innerType.substr(0, tokenPos);
    }

    auto keyType = toArrowTypeFromName(part);
    auto valueType = toArrowTypeFromName(innerType.substr(tokenPos + 1));

    return arrow::map(keyType, valueType);
  }

  // The type name of ROW type is like ROW<type, type>.
  const std::string structType = "ROW";
  if (typeName.substr(0, structType.length()) == structType) {
    std::size_t start = typeName.find_first_of('<');
    std::size_t end = typeName.find_last_of('>');
    if (start == std::string::npos || end == std::string::npos) {
      throw std::runtime_error("Invalid struct type: " + typeName);
    }

    // Extract the types of struct type.
    auto innerType = typeName.substr(start + 1, end - start - 1);
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::size_t tokenPos = innerType.find_first_of(',');
    std::size_t splitStart = 0;
    auto column = 0;
    while (tokenPos != std::string::npos && tokenPos < innerType.length()) {
      auto part = innerType.substr(splitStart, tokenPos - splitStart);
      // Count '<', '>' in part.
      auto numLeft = std::count_if(part.begin(), part.end(), [](const auto& c) { return c == '<'; });
      auto numRight = std::count_if(part.begin(), part.end(), [](const auto& c) { return c == '>'; });
      if (numLeft > numRight) {
        // Has unclosed '<'.
        std::size_t i = tokenPos + 1;
        for (; numLeft > numRight && i < innerType.length(); ++i) {
          if (innerType[i] == '<') {
            ++numLeft;
          } else if (innerType[i] == '>') {
            ++numRight;
          }
        }
        // If not reach to end of string, next character must be ','.
        if (numLeft != numRight || (i < innerType.length() && innerType[i] != ',')) {
          throw std::runtime_error("Invalid struct type: " + typeName);
        }
        tokenPos = i;
        part = innerType.substr(splitStart, tokenPos - splitStart);
      }
      fields.push_back(arrow::field("col_" + std::to_string(column++), toArrowTypeFromName(part)));
      splitStart = tokenPos + 1;
      tokenPos = innerType.find(',', splitStart);
    }
    if (splitStart < innerType.length()) {
      auto finalName = innerType.substr(splitStart);
      fields.push_back(arrow::field("col_" + std::to_string(column), toArrowTypeFromName(finalName)));
    }
    return arrow::struct_(fields);
  }

  throw std::runtime_error("Type name is not supported: " + typeName + ".");
}

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

/*
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
*/

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