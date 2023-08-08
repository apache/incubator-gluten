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

#include "substrait/algebra.pb.h"
#include "substrait/capabilities.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "substrait/function.pb.h"
#include "substrait/parameterized_types.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"
#include "substrait/type_expressions.pb.h"

#include <google/protobuf/wrappers.pb.h>

namespace gluten {

/// This class contains some common functions used to parse Substrait
/// components, and convert them into recognizable representations.
class SubstraitParser {
 public:
  /// Stores the type name and nullability.
  struct SubstraitType {
    std::string type;
    bool nullable;
  };

  /// Used to parse Substrait NamedStruct.
  std::vector<std::shared_ptr<SubstraitType>> parseNamedStruct(const ::substrait::NamedStruct& namedStruct);

  /// Used to parse partition columns from Substrait NamedStruct.
  std::vector<bool> parsePartitionColumns(const ::substrait::NamedStruct& namedStruct);

  /// Parse Substrait Type.
  std::shared_ptr<SubstraitType> parseType(const ::substrait::Type& substraitType);

  // Parse substraitType type such as i32.
  std::string parseType(const std::string& substraitType);

  /// Parse Substrait ReferenceSegment.
  int32_t parseReferenceSegment(const ::substrait::Expression::ReferenceSegment& refSegment);

  /// Make names in the format of {prefix}_{index}.
  std::vector<std::string> makeNames(const std::string& prefix, int size);

  /// Make node name in the format of n{nodeId}_{colIdx}.
  std::string makeNodeName(int nodeId, int colIdx);

  /// Get the column index from a node name in the format of
  /// n{nodeId}_{colIdx}.
  int getIdxFromNodeName(const std::string& nodeName);

  /// Find the Substrait function name according to the function id
  /// from a pre-constructed function map. The function specification can be
  /// a simple name or a compound name. The compound name format is:
  /// <function name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN>.
  /// Currently, the input types in the function specification are not used. But
  /// in the future, they should be used for the validation according the
  /// specifications in Substrait yaml files.
  const std::string& findFunctionSpec(const std::unordered_map<uint64_t, std::string>& functionMap, uint64_t id) const;

  /// Extracts the function name for a function from specified compound name.
  /// When the input is a simple name, it will be returned.
  std::string getSubFunctionName(const std::string& functionSpec) const;

  /// This function is used get the types from the compound name.
  void getSubFunctionTypes(const std::string& subFuncSpec, std::vector<std::string>& types) const;

  /// Used to find the Velox function name according to the function id
  /// from a pre-constructed function map.
  std::string findVeloxFunction(const std::unordered_map<uint64_t, std::string>& functionMap, uint64_t id) const;

  /// Map the Substrait function keyword into Velox function keyword.
  std::string mapToVeloxFunction(const std::string& substraitFunction, bool isDecimal) const;

  /// @brief Return whether a config is set as true in AdvancedExtension
  /// optimization.
  /// @param extension Substrait advanced extension.
  /// @param config the key string of a config.
  /// @return Whether the config is set as true.
  bool configSetInOptimization(const ::substrait::extensions::AdvancedExtension& extension, const std::string& config)
      const;

 private:
  /// A map used for mapping Substrait function keywords into Velox functions'
  /// keywords. Key: the Substrait function keyword, Value: the Velox function
  /// keyword. For those functions with different names in Substrait and Velox,
  /// a mapping relation should be added here.
  std::unordered_map<std::string, std::string> substraitVeloxFunctionMap_ = {
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
      {"datediff", "date_diff"},
      {"named_struct", "row_constructor"},
      {"bit_or", "bitwise_or_agg"},
      {"bit_or_merge", "bitwise_or_agg_merge"},
      {"bit_and", "bitwise_and_agg"},
      {"bit_and_merge", "bitwise_and_agg_merge"},
      {"collect_set", "array_distinct"},
      {"murmur3hash", "hash"},
      {"modulus", "mod"} /*Presto functions.*/};

  // The map is uesd for mapping substrait type.
  // Key: type in function name.
  // Value: substrait type name.
  const std::unordered_map<std::string, std::string> typeMap_ = {
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
      {"decLong", "LONG_DECIMAL"}};
};

} // namespace gluten
