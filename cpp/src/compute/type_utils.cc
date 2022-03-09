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

#include "type_utils.h"

using namespace facebook::velox;

bool isPrimitive(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return true;
    default:
      break;
  }
  return false;
}

bool isString(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::VARCHAR:
      return true;
    default:
      break;
  }
  return false;
}

TypePtr toVeloxTypeFromName(const std::string& type_name) {
  if (type_name == "BOOL") {
    return BOOLEAN();
  } else if (type_name == "FP64") {
    return DOUBLE();
  } else if (type_name == "STRING") {
    return VARCHAR();
  } else {
    throw std::runtime_error("Type name is not supported");
  }
}

std::shared_ptr<arrow::DataType> toArrowTypeFromName(const std::string& type_name) {
  if (type_name == "BOOL") {
    return arrow::boolean();
  } else if (type_name == "FP64") {
    return arrow::float64();
  } else if (type_name == "STRING") {
    return arrow::utf8();
  } else {
    throw std::runtime_error("Type name is not supported");
  }
}

std::shared_ptr<arrow::DataType> toArrowType(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::DOUBLE:
      return arrow::float64();
    case TypeKind::VARCHAR:
      return arrow::utf8();
    default:
      throw std::runtime_error("Type conversion is not supported.");
  }
}

int64_t bytesOfType(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::INTEGER:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return 8;
    default:
      throw std::runtime_error("bytesOfType is not supported.");
  }
}
