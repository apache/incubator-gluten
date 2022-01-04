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

#include "substrait_utils.h"

#include <arrow/array/array_primitive.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>

namespace substrait = io::substrait;
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector;
using namespace facebook::velox::dwio::common;

VeloxInitializer::VeloxInitializer() {}

// The Init will be called per executor.
void VeloxInitializer::Init() {
  // Setup
  filesystems::registerLocalFileSystem();
  std::unique_ptr<folly::IOThreadPoolExecutor> executor =
      std::make_unique<folly::IOThreadPoolExecutor>(3);
  // auto hiveConnectorFactory = std::make_shared<hive::HiveConnectorFactory>();
  // registerConnectorFactory(hiveConnectorFactory);
  auto hiveConnector = getConnectorFactory("hive")->newConnector(
      "hive-connector", nullptr, nullptr, executor.get());
  registerConnector(hiveConnector);
  dwrf::registerDwrfReaderFactory();
  // Register Velox functions
  functions::prestosql::registerAllFunctions();
  aggregate::registerSumAggregate<aggregate::SumAggregate>("sum");
}

SubstraitParser::SubstraitParser() {}

void SubstraitParser::ParseLiteral(const substrait::Expression::Literal& slit) {
  switch (slit.literal_type_case()) {
    case substrait::Expression_Literal::LiteralTypeCase::kFp64: {
      double val = slit.fp64();
      // std::cout << "double lit: " << val << std::endl;
      break;
    }
    case substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
      bool val = slit.boolean();
      break;
    }
    default:
      std::cout << "not supported" << std::endl;
      break;
  }
}

std::shared_ptr<SubstraitParser::SubstraitType> SubstraitParser::parseType(
    const substrait::Type& stype) {
  std::shared_ptr<SubstraitParser::SubstraitType> substrait_type;
  switch (stype.kind_case()) {
    case substrait::Type::KindCase::kBool: {
      auto sbool = stype.bool_();
      substrait_type = std::make_shared<SubstraitParser::SubstraitType>(
          "BOOL", sbool.variation().name(), sbool.nullability());
      break;
    }
    case substrait::Type::KindCase::kFp64: {
      auto sfp64 = stype.fp64();
      substrait_type = std::make_shared<SubstraitParser::SubstraitType>(
          "FP64", sfp64.variation().name(), sfp64.nullability());
      break;
    }
    case substrait::Type::KindCase::kStruct: {
      // TODO
      auto sstruct = stype.struct_();
      auto stypes = sstruct.types();
      for (auto& type : stypes) {
        parseType(type);
      }
      break;
    }
    case substrait::Type::KindCase::kString: {
      auto sstring = stype.string();
      auto nullable = sstring.nullability();
      auto name = sstring.variation().name();
      substrait_type = std::make_shared<SubstraitParser::SubstraitType>(
          "STRING", sstring.variation().name(), sstring.nullability());
      break;
    }
    default:
      std::cout << "Type not supported" << std::endl;
      break;
  }
  return substrait_type;
}

std::vector<std::shared_ptr<SubstraitParser::SubstraitType>>
SubstraitParser::ParseNamedStruct(const substrait::Type::NamedStruct& named_struct) {
  auto& snames = named_struct.names();
  std::vector<std::string> name_list;
  for (auto& sname : snames) {
    name_list.push_back(sname);
  }
  // Parse Struct
  auto& sstruct = named_struct.struct_();
  auto& stypes = sstruct.types();
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> substrait_type_list;
  for (auto& type : stypes) {
    auto substrait_type = parseType(type);
    substrait_type_list.push_back(substrait_type);
  }
  return substrait_type_list;
}

TypePtr SubstraitParser::getVeloxType(std::string type_name) {
  if (type_name == "BOOL") {
    return BOOLEAN();
  } else if (type_name == "FP64") {
    return DOUBLE();
  } else {
    throw std::runtime_error("not supported");
  }
}

std::vector<std::string> SubstraitVeloxPlanConverter::makeNames(const std::string& prefix,
                                                                int size) {
  std::vector<std::string> names;
  for (int i = 0; i < size; i++) {
    names.push_back(fmt::format("{}_{}", prefix, i));
  }
  return names;
}

std::string makeNodeName(int node_id, int col_idx) {
  return fmt::format("n{}_{}", node_id, col_idx);
}

std::string findFunction(const std::unordered_map<uint64_t, std::string>& functions_map,
                         const uint64_t& id) {
  if (functions_map.find(id) == functions_map.end()) {
    throw std::runtime_error("Could not find function " + std::to_string(id));
  }
  return functions_map[id];
}
