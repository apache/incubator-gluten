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

#include <folly/executors/IOThreadPoolExecutor.h>

#include "substrait/algebra.pb.h"
#include "substrait/capabilities.pb.h"
#include "substrait/extensions/extensions.pb.h"
#include "substrait/function.pb.h"
#include "substrait/parameterized_types.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"
#include "substrait/type_expressions.pb.h"
#include "utils/result_iterator.h"

namespace gazellejni {
namespace compute {

// This class contains some common funcitons used to parse Substrait components, and
// convert it to recognizable representations.
class SubstraitParser {
 public:
  SubstraitParser();
  struct SubstraitType {
    std::string type;
    bool nullable;
    SubstraitType(const std::string& sub_type, const bool& sub_nullable) {
      type = sub_type;
      nullable = sub_nullable;
    }
  };
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> parseNamedStruct(
      const substrait::NamedStruct& named_struct);
  std::shared_ptr<SubstraitType> parseType(const substrait::Type& stype);
  std::vector<std::string> makeNames(const std::string& prefix, int size);
  std::string makeNodeName(int node_id, int col_idx);

  /// Used to find the Substrait function name according to the function id
  /// from a pre-constructed function map. The function specification can be
  /// a simple name or a compound name. The compound name format is:
  /// <function name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN>.
  /// Currently, the input types in the function specification are not used. But
  /// in the future, they should be used for the validation according the specifications
  /// in Substrait yaml files.
  /// in the future, they should be used for the validation according the
  /// specifications in Substrait yaml files.
  std::string findSubstraitFuncSpec(
      const std::unordered_map<uint64_t, std::string>& functionMap, uint64_t id) const;

  /// This function is used to get the function name from the compound name.
  /// When the input is a simple name, it will be returned.
  std::string getSubFunctionName(const std::string& subFuncSpec) const;

  /// Used to find the Velox function name according to the function id
  /// from a pre-constructed function map.
  std::string findVeloxFunction(
      const std::unordered_map<uint64_t, std::string>& functionMap, uint64_t id) const;
  /// Used to map the Substrait function key word into Velox function key word.
  std::string mapToVeloxFunction(const std::string& subFunc) const;

  // Used for mapping Substrait function key word into Velox functions.
  std::unordered_map<std::string, std::string> substraitVeloxFunctionMap = {};
};

}  // namespace compute
}  // namespace gazellejni
