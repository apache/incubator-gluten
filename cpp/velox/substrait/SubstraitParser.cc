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

#include "SubstraitParser.h"
#include "TypeUtils.h"
#include "velox/common/base/Exceptions.h"

#include "VeloxSubstraitSignature.h"

namespace gluten {

TypePtr SubstraitParser::parseType(const ::substrait::Type& substraitType, bool asLowerCase) {
  switch (substraitType.kind_case()) {
    case ::substrait::Type::KindCase::kBool:
      return BOOLEAN();
    case ::substrait::Type::KindCase::kI8:
      return TINYINT();
    case ::substrait::Type::KindCase::kI16:
      return SMALLINT();
    case ::substrait::Type::KindCase::kI32:
      return INTEGER();
    case ::substrait::Type::KindCase::kI64:
      return BIGINT();
    case ::substrait::Type::KindCase::kFp32:
      return REAL();
    case ::substrait::Type::KindCase::kFp64:
      return DOUBLE();
    case ::substrait::Type::KindCase::kString:
      return VARCHAR();
    case ::substrait::Type::KindCase::kBinary:
      return VARBINARY();
    case ::substrait::Type::KindCase::kStruct: {
      const auto& substraitStruct = substraitType.struct_();
      const auto& structTypes = substraitStruct.types();
      const auto& structNames = substraitStruct.names();
      bool nameProvided = structTypes.size() == structNames.size();
      std::vector<TypePtr> types;
      std::vector<std::string> names;
      for (int i = 0; i < structTypes.size(); i++) {
        types.emplace_back(parseType(structTypes[i], asLowerCase));
        std::string fieldName = nameProvided ? structNames[i] : "col_" + std::to_string(i);
        if (asLowerCase) {
          folly::toLowerAscii(fieldName);
        }
        names.emplace_back(fieldName);
      }
      return ROW(std::move(names), std::move(types));
    }
    case ::substrait::Type::KindCase::kList: {
      const auto& fieldType = substraitType.list().type();
      return ARRAY(parseType(fieldType, asLowerCase));
    }
    case ::substrait::Type::KindCase::kMap: {
      const auto& sMap = substraitType.map();
      const auto& keyType = sMap.key();
      const auto& valueType = sMap.value();
      return MAP(parseType(keyType, asLowerCase), parseType(valueType, asLowerCase));
    }
    case ::substrait::Type::KindCase::kUserDefined:
      // We only support UNKNOWN type to handle the null literal whose type is
      // not known.
      return UNKNOWN();
    case ::substrait::Type::KindCase::kDate:
      return DATE();
    case ::substrait::Type::KindCase::kTimestamp:
      return TIMESTAMP();
    case ::substrait::Type::KindCase::kDecimal: {
      auto precision = substraitType.decimal().precision();
      auto scale = substraitType.decimal().scale();
      return DECIMAL(precision, scale);
    }
    default:
      VELOX_NYI("Parsing for Substrait type not supported: {}", substraitType.DebugString());
  }
}

std::vector<TypePtr> SubstraitParser::parseNamedStruct(const ::substrait::NamedStruct& namedStruct, bool asLowerCase) {
  // Note that "names" are not used.

  // Parse Struct.
  const auto& substraitStruct = namedStruct.struct_();
  const auto& substraitTypes = substraitStruct.types();
  std::vector<TypePtr> typeList;
  typeList.reserve(substraitTypes.size());
  for (const auto& type : substraitTypes) {
    typeList.emplace_back(parseType(type, asLowerCase));
  }
  return typeList;
}

std::vector<bool> SubstraitParser::parsePartitionColumns(const ::substrait::NamedStruct& namedStruct) {
  const auto& columnsTypes = namedStruct.column_types();
  std::vector<bool> isPartitionColumns;
  if (columnsTypes.size() == 0) {
    // Regard all columns as non-partitioned columns.
    isPartitionColumns.resize(namedStruct.names().size(), false);
    return isPartitionColumns;
  } else {
    VELOX_CHECK_EQ(columnsTypes.size(), namedStruct.names().size(), "Wrong size for column types and column names.");
  }

  isPartitionColumns.reserve(columnsTypes.size());
  for (const auto& columnType : columnsTypes) {
    switch (columnType) {
      case ::substrait::NamedStruct::NORMAL_COL:
        isPartitionColumns.emplace_back(false);
        break;
      case ::substrait::NamedStruct::PARTITION_COL:
        isPartitionColumns.emplace_back(true);
        break;
      default:
        VELOX_FAIL("Unspecified column type.");
    }
  }
  return isPartitionColumns;
}

int32_t SubstraitParser::parseReferenceSegment(const ::substrait::Expression::ReferenceSegment& refSegment) {
  auto typeCase = refSegment.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::ReferenceSegment::ReferenceTypeCase::kStructField: {
      return refSegment.struct_field().field();
    }
    default:
      VELOX_NYI("Substrait conversion not supported for ReferenceSegment '{}'", std::to_string(typeCase));
  }
}

std::vector<std::string> SubstraitParser::makeNames(const std::string& prefix, int size) {
  std::vector<std::string> names;
  names.reserve(size);
  for (int i = 0; i < size; i++) {
    names.emplace_back(fmt::format("{}_{}", prefix, i));
  }
  return names;
}

std::string SubstraitParser::makeNodeName(int node_id, int col_idx) {
  return fmt::format("n{}_{}", node_id, col_idx);
}

int SubstraitParser::getIdxFromNodeName(const std::string& nodeName) {
  // Get the position of "_" in the function name.
  std::size_t pos = nodeName.find("_");
  if (pos == std::string::npos) {
    VELOX_FAIL("Invalid node name.");
  }
  if (pos == nodeName.size() - 1) {
    VELOX_FAIL("Invalid node name.");
  }
  // Get the column index.
  std::string colIdx = nodeName.substr(pos + 1);
  try {
    return stoi(colIdx);
  } catch (const std::exception& err) {
    VELOX_FAIL(err.what());
  }
}

std::string SubstraitParser::findFunctionSpec(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) {
  auto x = functionMap.find(id);
  if (x == functionMap.end()) {
    VELOX_FAIL("Could not find function id {} in function map.", id);
  }
  return x->second;
}

// TODO Refactor using Bison.
std::string SubstraitParser::getNameBeforeDelimiter(const std::string& signature, const std::string& delimiter) {
  std::size_t pos = signature.find(delimiter);
  if (pos == std::string::npos) {
    return signature;
  }
  return signature.substr(0, pos);
}

std::vector<std::string> SubstraitParser::getSubFunctionTypes(const std::string& substraitFunction) {
  // Get the position of ":" in the function name.
  size_t pos = substraitFunction.find(":");
  // Get the parameter types.
  std::vector<std::string> types;
  if (pos == std::string::npos || pos == substraitFunction.size() - 1) {
    return types;
  }
  // Extract input types with delimiter.
  for (;;) {
    const size_t endPos = substraitFunction.find("_", pos + 1);
    if (endPos == std::string::npos) {
      std::string typeName = substraitFunction.substr(pos + 1);
      if (typeName != "opt" && typeName != "req") {
        types.emplace_back(typeName);
      }
      break;
    }

    const std::string typeName = substraitFunction.substr(pos + 1, endPos - pos - 1);
    if (typeName != "opt" && typeName != "req") {
      types.emplace_back(typeName);
    }
    pos = endPos;
  }
  return types;
}

std::string SubstraitParser::findVeloxFunction(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) {
  std::string funcSpec = findFunctionSpec(functionMap, id);
  std::string funcName = getNameBeforeDelimiter(funcSpec);
  std::vector<std::string> types = getSubFunctionTypes(funcSpec);
  bool isDecimal = false;
  for (const auto& type : types) {
    if (type.find("dec") != std::string::npos) {
      isDecimal = true;
      break;
    }
  }
  return mapToVeloxFunction(funcName, isDecimal);
}

std::string SubstraitParser::mapToVeloxFunction(const std::string& substraitFunction, bool isDecimal) {
  auto it = substraitVeloxFunctionMap_.find(substraitFunction);
  if (isDecimal) {
    if (substraitFunction == "lt" || substraitFunction == "lte" || substraitFunction == "gt" ||
        substraitFunction == "gte" || substraitFunction == "equal") {
      return "decimal_" + it->second;
    }
    if (substraitFunction == "round") {
      return "decimal_round";
    }
  }
  if (it != substraitVeloxFunctionMap_.end()) {
    return it->second;
  }
  // If not finding the mapping from Substrait function name to Velox function
  // name, the original Substrait function name will be used.
  return substraitFunction;
}

bool SubstraitParser::configSetInOptimization(
    const ::substrait::extensions::AdvancedExtension& extension,
    const std::string& config) {
  if (extension.has_optimization()) {
    google::protobuf::StringValue msg;
    extension.optimization().UnpackTo(&msg);
    std::size_t pos = msg.value().find(config);
    if ((pos != std::string::npos) && (msg.value().substr(pos + config.size(), 1) == "1")) {
      return true;
    }
  }
  return false;
}

std::vector<TypePtr> SubstraitParser::sigToTypes(const std::string& signature) {
  std::vector<std::string> typeStrs = SubstraitParser::getSubFunctionTypes(signature);
  std::vector<TypePtr> types;
  types.reserve(typeStrs.size());
  for (const auto& typeStr : typeStrs) {
    types.emplace_back(VeloxSubstraitSignature::fromSubstraitSignature(typeStr));
  }
  return types;
}

template <typename T>
T SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& /* literal */) {
  VELOX_NYI();
}

template <>
int8_t SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return static_cast<int8_t>(literal.i8());
}

template <>
int16_t SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return static_cast<int16_t>(literal.i16());
}

template <>
int32_t SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  if (literal.has_date()) {
    return int32_t(literal.date());
  }
  return literal.i32();
}

template <>
int64_t SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  if (literal.has_decimal()) {
    auto decimal = literal.decimal().value();
    int128_t decimalValue;
    memcpy(&decimalValue, decimal.c_str(), 16);
    return static_cast<int64_t>(decimalValue);
  }
  return literal.i64();
}

template <>
int128_t SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  auto decimal = literal.decimal().value();
  int128_t decimalValue;
  memcpy(&decimalValue, decimal.c_str(), 16);
  return HugeInt::build(static_cast<uint64_t>(decimalValue >> 64), static_cast<uint64_t>(decimalValue));
}

template <>
double SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.fp64();
}

template <>
float SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.fp32();
}

template <>
bool SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.boolean();
}

template <>
Timestamp SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return Timestamp::fromMicros(literal.timestamp());
}

template <>
StringView SubstraitParser::getLiteralValue(const ::substrait::Expression::Literal& literal) {
  if (literal.has_string()) {
    return StringView(literal.string());
  } else if (literal.has_var_char()) {
    return StringView(literal.var_char().value());
  } else if (literal.has_binary()) {
    return StringView(literal.binary());
  } else {
    VELOX_FAIL("Unexpected string or binary literal");
  }
}

std::unordered_map<std::string, std::string> SubstraitParser::substraitVeloxFunctionMap_ = {
    {"is_not_null", "isnotnull"}, /*Spark functions.*/
    {"is_null", "isnull"},
    {"equal", "equalto"},
    {"equal_null_safe", "equalnullsafe"},
    {"lt", "lessthan"},
    {"lte", "lessthanorequal"},
    {"gt", "greaterthan"},
    {"gte", "greaterthanorequal"},
    {"not_equal", "notequalto"},
    {"char_length", "length"},
    {"strpos", "instr"},
    {"ends_with", "endswith"},
    {"starts_with", "startswith"},
    {"named_struct", "row_constructor"},
    {"bit_or", "bitwise_or_agg"},
    {"bit_or_partial", "bitwise_or_agg_partial"},
    {"bit_or_merge", "bitwise_or_agg_merge"},
    {"bit_and", "bitwise_and_agg"},
    {"bit_and_partial", "bitwise_and_agg_partial"},
    {"bit_and_merge", "bitwise_and_agg_merge"},
    {"murmur3hash", "hash_with_seed"},
    {"modulus", "remainder"},
    {"date_format", "format_datetime"},
    {"collect_set", "set_agg"},
    {"collect_set_partial", "set_agg_partial"},
    {"collect_set_merge", "set_agg_merge"},
    {"collect_list", "array_agg"},
    {"collect_list_partial", "array_agg_partial"},
    {"collect_list_merge", "array_agg_merge"}};

const std::unordered_map<std::string, std::string> SubstraitParser::typeMap_ = {
    {"bool", "BOOLEAN"},
    {"i8", "TINYINT"},
    {"i16", "SMALLINT"},
    {"i32", "INTEGER"},
    {"i64", "BIGINT"},
    {"fp32", "REAL"},
    {"fp64", "DOUBLE"},
    {"date", "DATE"},
    {"ts", "TIMESTAMP"},
    {"str", "VARCHAR"},
    {"vbin", "VARBINARY"},
    {"decShort", "SHORT_DECIMAL"},
    {"decLong", "HUGEINT"}};

} // namespace gluten
