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

#include <folly/executors/IOThreadPoolExecutor.h>

#include <mutex>

#include "substrait_utils.h"
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

class SubstraitVeloxPlanConverter {
 public:
  SubstraitVeloxPlanConverter();

  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const io::substrait::AggregateRel& sagg);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const io::substrait::ProjectRel& sproject);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(
      const io::substrait::FilterRel& sfilter);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(const io::substrait::ReadRel& sread,
                                                    u_int32_t* index,
                                                    std::vector<std::string>* paths,
                                                    std::vector<u_int64_t>* starts,
                                                    std::vector<u_int64_t>* lengths);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(const io::substrait::Rel& srel);
  std::shared_ptr<const core::PlanNode> toVeloxPlan(const io::substrait::Plan& splan);

  std::shared_ptr<ResultIterator<arrow::RecordBatch>> getResIter(
      std::shared_ptr<const core::PlanNode> plan_node);

 private:
  int plan_node_id_ = 0;
  std::shared_ptr<SubstraitParser> sub_parser_;
  std::unordered_map<uint64_t, std::string> functions_map_;
  u_int32_t partition_index_;
  std::vector<std::string> paths_;
  std::vector<u_int64_t> starts_;
  std::vector<u_int64_t> lengths_;
  std::string findFunction(uint64_t id);
  std::string nextPlanNodeId();
  std::vector<std::string> makeNames(const std::string& prefix, int size);
  std::shared_ptr<const core::PlanNode> getChildNode(const io::substrait::Rel& splan);
  class WholeStageResultIterator;
};