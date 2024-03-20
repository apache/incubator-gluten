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
    case TypeKind::ROW: {
      std::stringstream buffer;
      buffer << "struct<";
      const auto& rt = asRowType(type);
      for (size_t i = 0; i < rt->children().size(); i++) {
        buffer << toSubstraitSignature(rt->childAt(i));
        if (i == rt->children().size() - 1) {
          continue;
        }
        buffer << ",";
      }
      buffer << ">";
      return buffer.str();
    }
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

  auto startWith = [](const std::string& str, const std::string& prefix) {
    return str.size() >= prefix.size() && str.substr(0, prefix.size()) == prefix;
  };

  auto parseNestedTypeSignature = [&](const std::string& signature) -> std::vector<TypePtr> {
    auto start = signature.find_first_of('<');
    auto end = signature.find_last_of('>');
    VELOX_CHECK(
        end - start > 1,
        "Native validation failed due to: more information is needed to create nested type for {}",
        signature);

    std::string childrenTypes = signature.substr(start + 1, end - start - 1);

    // Split the types with delimiter.
    std::string delimiter = ",";
    std::size_t pos;
    std::vector<TypePtr> types;
    while ((pos = childrenTypes.find(delimiter)) != std::string::npos) {
      auto typeStr = childrenTypes.substr(0, pos);
      std::size_t endPos = pos;
      if (startWith(typeStr, "dec") || startWith(typeStr, "struct") || startWith(typeStr, "map") ||
          startWith(typeStr, "list")) {
        endPos = childrenTypes.find(">") + 1;
        if (endPos > pos) {
          typeStr += childrenTypes.substr(pos, endPos - pos);
        } else {
          // For nested case, the end '>' could missing,
          // so the last position is treated as end.
          typeStr += childrenTypes.substr(pos);
          endPos = childrenTypes.size();
        }
      }
      types.emplace_back(fromSubstraitSignature(typeStr));
      childrenTypes.erase(0, endPos + delimiter.length());
    }
    if (childrenTypes.size() > 0 && !startWith(childrenTypes, ">")) {
      types.emplace_back(fromSubstraitSignature(childrenTypes));
    }
    return types;
  };

  if (startWith(signature, "dec")) {
    // Decimal type name is in the format of dec<precision,scale>.
    auto precisionStart = signature.find_first_of('<');
    auto tokenIndex = signature.find_first_of(',');
    auto scaleEnd = signature.find_first_of('>');
    auto precision = stoi(signature.substr(precisionStart + 1, (tokenIndex - precisionStart - 1)));
    auto scale = stoi(signature.substr(tokenIndex + 1, (scaleEnd - tokenIndex - 1)));
    return DECIMAL(precision, scale);
  }

  if (startWith(signature, "struct")) {
    // Struct type name is in the format of struct<T1,T2,...,Tn>.
    auto types = parseNestedTypeSignature(signature);
    std::vector<std::string> names(types.size());
    for (int i = 0; i < types.size(); i++) {
      names[i] = "";
    }
    return std::make_shared<RowType>(std::move(names), std::move(types));
  }

  if (startWith(signature, "map")) {
    // Map type name is in the format of map<T1,T2>.
    auto types = parseNestedTypeSignature(signature);
    if (types.size() != 2) {
      VELOX_UNSUPPORTED("Substrait type signature conversion to Velox type not supported for {}.", signature);
    }
    return MAP(std::move(types)[0], std::move(types)[1]);
  }

  if (startWith(signature, "list")) {
    auto listStart = signature.find_first_of('<');
    auto listEnd = signature.find_last_of('>');
    VELOX_CHECK(
        listEnd - listStart > 1,
        "Native validation failed due to: more information is needed to create ListType: {}",
        signature);

    auto elementTypeStr = signature.substr(listStart + 1, listEnd - listStart - 1);
    auto elementType = fromSubstraitSignature(elementTypeStr);
    return ARRAY(elementType);
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
