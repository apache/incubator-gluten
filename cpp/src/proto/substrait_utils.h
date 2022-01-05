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
#include "velox/buffer/Buffer.h"
#include "velox/common/caching/DataCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/dwrf/common/CachedBufferedInput.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/SumAggregate.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

class SubstraitParser {
 public:
  SubstraitParser();
  struct SubstraitType {
    std::string type;
    std::string name;
    bool nullable;
    SubstraitType(const std::string& t, const std::string& n, const bool& nul) {
      type = t;
      name = n;
      nullable = nul;
    }
  };
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> parseNamedStruct(
      const io::substrait::Type::NamedStruct& named_struct);
  std::shared_ptr<SubstraitType> parseType(const io::substrait::Type& stype);
  TypePtr getVeloxType(std::string type_name);
  std::vector<std::string> makeNames(const std::string& prefix, int size);
  std::string makeNodeName(int node_id, int col_idx);
  std::string findFunction(const std::unordered_map<uint64_t, std::string>& functions_map,
                           const uint64_t& id) const;
};

class VeloxInitializer {
 public:
  VeloxInitializer();
  void Init();
};
