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

#include "common/result_iterator.h"
#include "expression.pb.h"
#include "extensions.pb.h"
#include "function.pb.h"
#include "parameterized_types.pb.h"
#include "plan.pb.h"
#include "relations.pb.h"
#include "selection.pb.h"
#include "type.pb.h"
#include "type_expressions.pb.h"

// This class contains some common funcitons used to parse Substrait components, and
// convert it to recognizable representations.
class SubstraitParser {
 public:
  SubstraitParser();
  struct SubstraitType {
    std::string type;
    std::string name;
    bool nullable;
    SubstraitType(const std::string& t, const std::string& n, const bool& null) {
      type = t;
      name = n;
      nullable = null;
    }
  };
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> parseNamedStruct(
      const io::substrait::Type::NamedStruct& named_struct);
  std::shared_ptr<SubstraitType> parseType(const io::substrait::Type& stype);
  std::vector<std::string> makeNames(const std::string& prefix, int size);
  std::string makeNodeName(int node_id, int col_idx);
  std::string findFunction(const std::unordered_map<uint64_t, std::string>& functions_map,
                           const uint64_t& id) const;
  // Used for mapping Substrait function key word into Velox functions.
  std::unordered_map<std::string, std::string> substrait_velox_function_map = {
      {"MULTIPLY", "multiply"}, {"SUM", "sum"}};
};
