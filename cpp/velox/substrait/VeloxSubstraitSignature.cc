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

#include "VeloxSubstraitSignature.h"
#include "velox/functions/FunctionRegistry.h"

namespace gluten {

std::string VeloxSubstraitSignature::toSubstraitSignature(const TypePtr& type) {
  if (type->isDate()) {
    return "date";
  }

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return "bool";
    case TypeKind::TINYINT:
      return "i8";
    case TypeKind::SMALLINT:
      return "i16";
    case TypeKind::INTEGER:
      return "i32";
    case TypeKind::BIGINT:
      return "i64";
    case TypeKind::REAL:
      return "fp32";
    case TypeKind::DOUBLE:
      return "fp64";
    case TypeKind::VARCHAR:
      return "str";
    case TypeKind::VARBINARY:
      return "vbin";
    case TypeKind::TIMESTAMP:
      return "ts";
    case TypeKind::ARRAY:
      return "list";
    case TypeKind::MAP:
      return "map";
    case TypeKind::ROW:
      return "struct";
    case TypeKind::UNKNOWN:
      return "u!name";
    default:
      VELOX_UNSUPPORTED(
          "Substrait type signature conversion not supported for type {}.", mapTypeKindToName(type->kind()));
  }
}

TypePtr VeloxSubstraitSignature::fromSubstraitSignature(const std::string& signature) {
  if (signature == "bool") {
    return BOOLEAN();
  }

  if (signature == "i8") {
    return TINYINT();
  }

  if (signature == "i16") {
    return SMALLINT();
  }

  if (signature == "i32") {
    return INTEGER();
  }

  if (signature == "i64") {
    return BIGINT();
  }

  if (signature == "fp32") {
    return REAL();
  }

  if (signature == "fp64") {
    return DOUBLE();
  }

  if (signature == "str") {
    return VARCHAR();
  }

  if (signature == "vbin") {
    return VARBINARY();
  }

  if (signature == "ts") {
    return TIMESTAMP();
  }

  if (signature == "date") {
    return DATE();
  }

  if (signature.size() > 3 && signature.substr(0, 3) == "dec") {
    // Decimal info is in the format of dec<precision,scale>.
    auto precisionStart = signature.find_first_of('<');
    auto tokenIndex = signature.find_first_of(',');
    auto scaleEnd = signature.find_first_of('>');
    auto precision = stoi(signature.substr(precisionStart + 1, (tokenIndex - precisionStart - 1)));
    auto scale = stoi(signature.substr(tokenIndex + 1, (scaleEnd - tokenIndex - 1)));
    return DECIMAL(precision, scale);
  }

  if (signature.size() > 6 && signature.substr(0, 6) == "struct") {
    // Struct info is in the format of struct<T1,T2,...,Tn>.
    // TODO: nested struct is not supported.
    auto structStart = signature.find_first_of('<');
    auto structEnd = signature.find_last_of('>');
    VELOX_CHECK(
        structEnd - structStart > 1, "Native validation failed due to: more information is needed to create RowType");
    std::string childrenTypes = signature.substr(structStart + 1, structEnd - structStart - 1);

    // Split the types with delimiter.
    std::string delimiter = ",";
    std::size_t pos;
    std::vector<TypePtr> types;
    std::vector<std::string> names;
    while ((pos = childrenTypes.find(delimiter)) != std::string::npos) {
      const auto& typeStr = childrenTypes.substr(0, pos);
      if (typeStr.find("dec") != std::string::npos) {
        std::size_t endPos = childrenTypes.find(">");
        VELOX_CHECK(endPos >= pos + 1, "Decimal scale is expected.");
        const auto& decimalStr = typeStr + childrenTypes.substr(pos, endPos - pos) + ">";
        types.emplace_back(fromSubstraitSignature(decimalStr));
        names.emplace_back("");
        childrenTypes.erase(0, endPos + delimiter.length() + 1);
        continue;
      }

      types.emplace_back(fromSubstraitSignature(typeStr));
      names.emplace_back("");
      childrenTypes.erase(0, pos + delimiter.length());
    }
    types.emplace_back(fromSubstraitSignature(childrenTypes));
    names.emplace_back("");
    return std::make_shared<RowType>(std::move(names), std::move(types));
  }

  VELOX_UNSUPPORTED("Substrait type signature conversion to Velox type not supported for {}.", signature);
}

std::string VeloxSubstraitSignature::toSubstraitSignature(
    const std::string& functionName,
    const std::vector<TypePtr>& arguments) {
  if (arguments.empty()) {
    return functionName;
  }
  std::vector<std::string> substraitTypeSignatures;
  substraitTypeSignatures.reserve(arguments.size());
  for (const auto& type : arguments) {
    substraitTypeSignatures.emplace_back(toSubstraitSignature(type));
  }
  return functionName + ":" + folly::join("_", substraitTypeSignatures);
}

} // namespace gluten
