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

#include "SubstraitToVeloxPlan.h"
#include "TypeUtils.h"
#include "VariantToVectorConverter.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/exec/TableWriter.h"
#include "velox/type/Type.h"

#include "utils/ConfigExtractor.h"

#include "config/GlutenConfig.h"

namespace gluten {
namespace {

core::SortOrder toSortOrder(const ::substrait::SortField& sortField) {
  switch (sortField.direction()) {
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
      return core::kAscNullsFirst;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
      return core::kAscNullsLast;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
      return core::kDescNullsFirst;
    case ::substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
      return core::kDescNullsLast;
    default:
      VELOX_FAIL("Sort direction is not supported.");
  }
}

/// Holds the information required to create
/// a project node to simulate the emit
/// behavior in Substrait.
struct EmitInfo {
  std::vector<core::TypedExprPtr> expressions;
  std::vector<std::string> projectNames;
};

/// Helper function to extract the attributes required to create a ProjectNode
/// used for interpretting Substrait Emit.
EmitInfo getEmitInfo(const ::substrait::RelCommon& relCommon, const core::PlanNodePtr& node) {
  const auto& emit = relCommon.emit();
  int emitSize = emit.output_mapping_size();
  EmitInfo emitInfo;
  emitInfo.projectNames.reserve(emitSize);
  emitInfo.expressions.reserve(emitSize);
  const auto& outputType = node->outputType();
  for (int i = 0; i < emitSize; i++) {
    int32_t mapId = emit.output_mapping(i);
    emitInfo.projectNames[i] = outputType->nameOf(mapId);
    emitInfo.expressions[i] =
        std::make_shared<core::FieldAccessTypedExpr>(outputType->childAt(mapId), outputType->nameOf(mapId));
  }
  return emitInfo;
}

template <typename T>
// Get the lowest value for numeric type.
T getLowest() {
  return std::numeric_limits<T>::lowest();
}

// Get the lowest value for string.
template <>
std::string getLowest<std::string>() {
  return "";
}

// Get the max value for numeric type.
template <typename T>
T getMax() {
  return std::numeric_limits<T>::max();
}

// The max value will be used in BytesRange. Return empty string here instead.
template <>
std::string getMax<std::string>() {
  return "";
}

// Substrait function names.
const std::string sIsNotNull = "is_not_null";
const std::string sIsNull = "is_null";
const std::string sGte = "gte";
const std::string sGt = "gt";
const std::string sLte = "lte";
const std::string sLt = "lt";
const std::string sEqual = "equal";
const std::string sOr = "or";
const std::string sNot = "not";

// Substrait types.
const std::string sI32 = "i32";
const std::string sI64 = "i64";

/// @brief Get the input type from both sides of join.
/// @param leftNode the plan node of left side.
/// @param rightNode the plan node of right side.
/// @return the input type.
RowTypePtr getJoinInputType(const core::PlanNodePtr& leftNode, const core::PlanNodePtr& rightNode) {
  auto outputSize = leftNode->outputType()->size() + rightNode->outputType()->size();
  std::vector<std::string> outputNames;
  std::vector<std::shared_ptr<const Type>> outputTypes;
  outputNames.reserve(outputSize);
  outputTypes.reserve(outputSize);
  for (const auto& node : {leftNode, rightNode}) {
    const auto& names = node->outputType()->names();
    outputNames.insert(outputNames.end(), names.begin(), names.end());
    const auto& types = node->outputType()->children();
    outputTypes.insert(outputTypes.end(), types.begin(), types.end());
  }
  return std::make_shared<const RowType>(std::move(outputNames), std::move(outputTypes));
}

/// @brief Get the direct output type of join.
/// @param leftNode the plan node of left side.
/// @param rightNode the plan node of right side.
/// @param joinType the join type.
/// @return the output type.
RowTypePtr getJoinOutputType(
    const core::PlanNodePtr& leftNode,
    const core::PlanNodePtr& rightNode,
    const core::JoinType& joinType) {
  // Decide output type.
  // Output of right semi join cannot include columns from the left side.
  bool outputMayIncludeLeftColumns = !(core::isRightSemiFilterJoin(joinType) || core::isRightSemiProjectJoin(joinType));

  // Output of left semi and anti joins cannot include columns from the right
  // side.
  bool outputMayIncludeRightColumns =
      !(core::isLeftSemiFilterJoin(joinType) || core::isLeftSemiProjectJoin(joinType) || core::isAntiJoin(joinType));

  if (outputMayIncludeLeftColumns && outputMayIncludeRightColumns) {
    return getJoinInputType(leftNode, rightNode);
  }

  if (outputMayIncludeLeftColumns) {
    if (core::isLeftSemiProjectJoin(joinType)) {
      std::vector<std::string> outputNames = leftNode->outputType()->names();
      std::vector<std::shared_ptr<const Type>> outputTypes = leftNode->outputType()->children();
      outputNames.emplace_back("exists");
      outputTypes.emplace_back(BOOLEAN());
      return std::make_shared<const RowType>(std::move(outputNames), std::move(outputTypes));
    } else {
      return leftNode->outputType();
    }
  }

  if (outputMayIncludeRightColumns) {
    if (core::isRightSemiProjectJoin(joinType)) {
      std::vector<std::string> outputNames = rightNode->outputType()->names();
      std::vector<std::shared_ptr<const Type>> outputTypes = rightNode->outputType()->children();
      outputNames.emplace_back("exists");
      outputTypes.emplace_back(BOOLEAN());
      return std::make_shared<const RowType>(std::move(outputNames), std::move(outputTypes));
    } else {
      return rightNode->outputType();
    }
  }
  VELOX_FAIL("Output should include left or right columns.");
}
} // namespace

core::PlanNodePtr SubstraitToVeloxPlanConverter::processEmit(
    const ::substrait::RelCommon& relCommon,
    const core::PlanNodePtr& noEmitNode) {
  switch (relCommon.emit_kind_case()) {
    case ::substrait::RelCommon::EmitKindCase::kDirect:
      return noEmitNode;
    case ::substrait::RelCommon::EmitKindCase::kEmit: {
      auto emitInfo = getEmitInfo(relCommon, noEmitNode);
      return std::make_shared<core::ProjectNode>(
          nextPlanNodeId(), std::move(emitInfo.projectNames), std::move(emitInfo.expressions), noEmitNode);
    }
    default:
      VELOX_FAIL("unrecognized emit kind");
  }
}

core::AggregationNode::Step SubstraitToVeloxPlanConverter::toAggregationStep(const ::substrait::AggregateRel& aggRel) {
  // TODO Simplify Velox's aggregation steps
  if (aggRel.has_advanced_extension() &&
      SubstraitParser::configSetInOptimization(aggRel.advanced_extension(), "allowFlush=")) {
    return core::AggregationNode::Step::kPartial;
  }
  return core::AggregationNode::Step::kSingle;
}

/// Get aggregation function step for AggregateFunction.
/// The returned step value will be used to decide which Velox aggregate function or companion function
/// is used for the actual data processing.
core::AggregationNode::Step SubstraitToVeloxPlanConverter::toAggregationFunctionStep(
    const ::substrait::AggregateFunction& sAggFuc) {
  const auto& phase = sAggFuc.phase();
  switch (phase) {
    case ::substrait::AGGREGATION_PHASE_UNSPECIFIED:
      VELOX_FAIL("Aggregation phase not specified.")
      break;
    case ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE:
      return core::AggregationNode::Step::kPartial;
    case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE:
      return core::AggregationNode::Step::kIntermediate;
    case ::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT:
      return core::AggregationNode::Step::kSingle;
    case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT:
      return core::AggregationNode::Step::kFinal;
    default:
      VELOX_FAIL("Unexpected aggregation phase.")
  }
}

std::string SubstraitToVeloxPlanConverter::toAggregationFunctionName(
    const std::string& baseName,
    const core::AggregationNode::Step& step) {
  std::string suffix;
  switch (step) {
    case core::AggregationNode::Step::kPartial:
      suffix = "_partial";
      break;
    case core::AggregationNode::Step::kFinal:
      suffix = "_merge_extract";
      break;
    case core::AggregationNode::Step::kIntermediate:
      suffix = "_merge";
      break;
    case core::AggregationNode::Step::kSingle:
      suffix = "";
      break;
    default:
      VELOX_FAIL("Unexpected aggregation node step.")
  }
  return baseName + suffix;
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::JoinRel& sJoin) {
  if (!sJoin.has_left()) {
    VELOX_FAIL("Left Rel is expected in JoinRel.");
  }
  if (!sJoin.has_right()) {
    VELOX_FAIL("Right Rel is expected in JoinRel.");
  }

  auto leftNode = toVeloxPlan(sJoin.left());
  auto rightNode = toVeloxPlan(sJoin.right());

  // Map join type.
  core::JoinType joinType;
  bool isNullAwareAntiJoin = false;
  switch (sJoin.type()) {
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
      joinType = core::JoinType::kInner;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
      joinType = core::JoinType::kFull;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
      joinType = core::JoinType::kLeft;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
      joinType = core::JoinType::kRight;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
      // Determine the semi join type based on extracted information.
      if (sJoin.has_advanced_extension() &&
          SubstraitParser::configSetInOptimization(sJoin.advanced_extension(), "isExistenceJoin=")) {
        joinType = core::JoinType::kLeftSemiProject;
      } else {
        joinType = core::JoinType::kLeftSemiFilter;
      }
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
      // Determine the semi join type based on extracted information.
      if (sJoin.has_advanced_extension() &&
          SubstraitParser::configSetInOptimization(sJoin.advanced_extension(), "isExistenceJoin=")) {
        joinType = core::JoinType::kRightSemiProject;
      } else {
        joinType = core::JoinType::kRightSemiFilter;
      }
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_ANTI: {
      // Determine the anti join type based on extracted information.
      if (sJoin.has_advanced_extension() &&
          SubstraitParser::configSetInOptimization(sJoin.advanced_extension(), "isNullAwareAntiJoin=")) {
        isNullAwareAntiJoin = true;
      }
      joinType = core::JoinType::kAnti;
      break;
    }
    default:
      VELOX_NYI("Unsupported Join type: {}", sJoin.type());
  }

  // extract join keys from join expression
  std::vector<const ::substrait::Expression::FieldReference*> leftExprs, rightExprs;
  extractJoinKeys(sJoin.expression(), leftExprs, rightExprs);
  VELOX_CHECK_EQ(leftExprs.size(), rightExprs.size());
  size_t numKeys = leftExprs.size();

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> leftKeys, rightKeys;
  leftKeys.reserve(numKeys);
  rightKeys.reserve(numKeys);
  auto inputRowType = getJoinInputType(leftNode, rightNode);
  for (size_t i = 0; i < numKeys; ++i) {
    leftKeys.emplace_back(exprConverter_->toVeloxExpr(*leftExprs[i], inputRowType));
    rightKeys.emplace_back(exprConverter_->toVeloxExpr(*rightExprs[i], inputRowType));
  }

  core::TypedExprPtr filter;
  if (sJoin.has_post_join_filter()) {
    filter = exprConverter_->toVeloxExpr(sJoin.post_join_filter(), inputRowType);
  }

  if (sJoin.has_advanced_extension() &&
      SubstraitParser::configSetInOptimization(sJoin.advanced_extension(), "isSMJ=")) {
    // Create MergeJoinNode node
    return std::make_shared<core::MergeJoinNode>(
        nextPlanNodeId(),
        joinType,
        leftKeys,
        rightKeys,
        filter,
        leftNode,
        rightNode,
        getJoinOutputType(leftNode, rightNode, joinType));

  } else {
    // Create HashJoinNode node
    return std::make_shared<core::HashJoinNode>(
        nextPlanNodeId(),
        joinType,
        isNullAwareAntiJoin,
        leftKeys,
        rightKeys,
        filter,
        leftNode,
        rightNode,
        getJoinOutputType(leftNode, rightNode, joinType));
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::AggregateRel& aggRel) {
  auto childNode = convertSingleInput<::substrait::AggregateRel>(aggRel);
  core::AggregationNode::Step aggStep = toAggregationStep(aggRel);
  const auto& inputType = childNode->outputType();
  std::vector<core::FieldAccessTypedExprPtr> veloxGroupingExprs;

  // Get the grouping expressions.
  for (const auto& grouping : aggRel.groupings()) {
    for (const auto& groupingExpr : grouping.grouping_expressions()) {
      // Velox's groupings are limited to be Field.
      veloxGroupingExprs.emplace_back(exprConverter_->toVeloxExpr(groupingExpr.selection(), inputType));
    }
  }

  // Parse measures and get the aggregate expressions.
  // Each measure represents one aggregate expression.
  std::vector<core::AggregationNode::Aggregate> aggregates;
  aggregates.reserve(aggRel.measures().size());

  for (const auto& measure : aggRel.measures()) {
    core::FieldAccessTypedExprPtr mask;
    ::substrait::Expression substraitAggMask = measure.filter();
    // Get Aggregation Masks.
    if (measure.has_filter()) {
      if (substraitAggMask.ByteSizeLong() > 0) {
        mask = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
            exprConverter_->toVeloxExpr(substraitAggMask, inputType));
      }
    }
    const auto& aggFunction = measure.measure();
    auto baseFuncName = SubstraitParser::findVeloxFunction(functionMap_, aggFunction.function_reference());
    auto funcName = toAggregationFunctionName(baseFuncName, toAggregationFunctionStep(aggFunction));
    std::vector<core::TypedExprPtr> aggParams;
    aggParams.reserve(aggFunction.arguments().size());
    for (const auto& arg : aggFunction.arguments()) {
      aggParams.emplace_back(exprConverter_->toVeloxExpr(arg.value(), inputType));
    }
    auto aggVeloxType = SubstraitParser::parseType(aggFunction.output_type());
    auto aggExpr = std::make_shared<const core::CallTypedExpr>(aggVeloxType, std::move(aggParams), funcName);

    std::vector<TypePtr> rawInputTypes =
        SubstraitParser::sigToTypes(SubstraitParser::findFunctionSpec(functionMap_, aggFunction.function_reference()));
    aggregates.emplace_back(core::AggregationNode::Aggregate{aggExpr, rawInputTypes, mask, {}, {}});
  }

  bool ignoreNullKeys = false;
  std::vector<core::FieldAccessTypedExprPtr> preGroupingExprs;
  if (aggRel.has_advanced_extension() &&
      SubstraitParser::configSetInOptimization(aggRel.advanced_extension(), "isStreaming=")) {
    preGroupingExprs.reserve(veloxGroupingExprs.size());
    preGroupingExprs.insert(preGroupingExprs.begin(), veloxGroupingExprs.begin(), veloxGroupingExprs.end());
  }

  // Get the output names of Aggregation.
  std::vector<std::string> aggOutNames;
  aggOutNames.reserve(aggRel.measures().size());
  for (int idx = veloxGroupingExprs.size(); idx < veloxGroupingExprs.size() + aggRel.measures().size(); idx++) {
    aggOutNames.emplace_back(SubstraitParser::makeNodeName(planNodeId_, idx));
  }

  auto aggregationNode = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      aggStep,
      veloxGroupingExprs,
      preGroupingExprs,
      aggOutNames,
      aggregates,
      ignoreNullKeys,
      childNode);

  if (aggRel.has_common()) {
    return processEmit(aggRel.common(), std::move(aggregationNode));
  } else {
    return aggregationNode;
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::ProjectRel& projectRel) {
  auto childNode = convertSingleInput<::substrait::ProjectRel>(projectRel);
  // Construct Velox Expressions.
  const auto& projectExprs = projectRel.expressions();
  std::vector<std::string> projectNames;
  std::vector<core::TypedExprPtr> expressions;
  projectNames.reserve(projectExprs.size());
  expressions.reserve(projectExprs.size());

  const auto& inputType = childNode->outputType();
  int colIdx = 0;
  // Note that Substrait projection adds the project expressions on top of the
  // input to the projection node. Thus we need to add the input columns first
  // and then add the projection expressions.

  // First, adding the project names and expressions from the input to
  // the project node.
  for (uint32_t idx = 0; idx < inputType->size(); idx++) {
    const auto& fieldName = inputType->nameOf(idx);
    projectNames.emplace_back(fieldName);
    expressions.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(inputType->childAt(idx), fieldName));
    colIdx += 1;
  }

  // Then, adding project expression related project names and expressions.
  for (const auto& expr : projectExprs) {
    expressions.emplace_back(exprConverter_->toVeloxExpr(expr, inputType));
    projectNames.emplace_back(SubstraitParser::makeNodeName(planNodeId_, colIdx));
    colIdx += 1;
  }

  if (projectRel.has_common()) {
    auto relCommon = projectRel.common();
    const auto& emit = relCommon.emit();
    int emitSize = emit.output_mapping_size();
    std::vector<std::string> emitProjectNames(emitSize);
    std::vector<core::TypedExprPtr> emitExpressions(emitSize);
    for (int i = 0; i < emitSize; i++) {
      int32_t mapId = emit.output_mapping(i);
      emitProjectNames[i] = projectNames[mapId];
      emitExpressions[i] = expressions[mapId];
    }
    return std::make_shared<core::ProjectNode>(
        nextPlanNodeId(), std::move(emitProjectNames), std::move(emitExpressions), std::move(childNode));
  } else {
    return std::make_shared<core::ProjectNode>(
        nextPlanNodeId(), std::move(projectNames), std::move(expressions), std::move(childNode));
  }
}

std::shared_ptr<connector::hive::LocationHandle> makeLocationHandle(
    const std::string& targetDirectory,
    const std::optional<std::string>& writeDirectory = std::nullopt,
    const connector::hive::LocationHandle::TableType& tableType =
        connector::hive::LocationHandle::TableType::kExisting) {
  return std::make_shared<connector::hive::LocationHandle>(
      targetDirectory, writeDirectory.value_or(targetDirectory), tableType);
}

std::shared_ptr<connector::hive::HiveInsertTableHandle> makeHiveInsertTableHandle(
    const std::vector<std::string>& tableColumnNames,
    const std::vector<TypePtr>& tableColumnTypes,
    const std::vector<std::string>& partitionedBy,
    const std::shared_ptr<connector::hive::HiveBucketProperty>& bucketProperty,
    const std::shared_ptr<connector::hive::LocationHandle>& locationHandle,
    const dwio::common::FileFormat& tableStorageFormat = dwio::common::FileFormat::PARQUET,
    const std::optional<common::CompressionKind>& compressionKind = {}) {
  std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>> columnHandles;
  columnHandles.reserve(tableColumnNames.size());
  std::vector<std::string> bucketedBy;
  std::vector<TypePtr> bucketedTypes;
  std::vector<std::shared_ptr<const connector::hive::HiveSortingColumn>> sortedBy;
  if (bucketProperty != nullptr) {
    bucketedBy = bucketProperty->bucketedBy();
    bucketedTypes = bucketProperty->bucketedTypes();
    sortedBy = bucketProperty->sortedBy();
  }
  int32_t numPartitionColumns{0};
  int32_t numSortingColumns{0};
  int32_t numBucketColumns{0};
  for (int i = 0; i < tableColumnNames.size(); ++i) {
    for (int j = 0; j < bucketedBy.size(); ++j) {
      if (bucketedBy[j] == tableColumnNames[i]) {
        ++numBucketColumns;
      }
    }
    for (int j = 0; j < sortedBy.size(); ++j) {
      if (sortedBy[j]->sortColumn() == tableColumnNames[i]) {
        ++numSortingColumns;
      }
    }
    if (std::find(partitionedBy.cbegin(), partitionedBy.cend(), tableColumnNames.at(i)) != partitionedBy.cend()) {
      ++numPartitionColumns;
      columnHandles.emplace_back(std::make_shared<connector::hive::HiveColumnHandle>(
          tableColumnNames.at(i),
          connector::hive::HiveColumnHandle::ColumnType::kPartitionKey,
          tableColumnTypes.at(i),
          tableColumnTypes.at(i)));
    } else {
      columnHandles.emplace_back(std::make_shared<connector::hive::HiveColumnHandle>(
          tableColumnNames.at(i),
          connector::hive::HiveColumnHandle::ColumnType::kRegular,
          tableColumnTypes.at(i),
          tableColumnTypes.at(i)));
    }
  }
  VELOX_CHECK_EQ(numPartitionColumns, partitionedBy.size());
  VELOX_CHECK_EQ(numBucketColumns, bucketedBy.size());
  VELOX_CHECK_EQ(numSortingColumns, sortedBy.size());
  return std::make_shared<connector::hive::HiveInsertTableHandle>(
      columnHandles, locationHandle, tableStorageFormat, bucketProperty, compressionKind);
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::WriteRel& writeRel) {
  core::PlanNodePtr childNode;
  if (writeRel.has_input()) {
    childNode = toVeloxPlan(writeRel.input());
  } else {
    VELOX_FAIL("Child Rel is expected in WriteRel.");
  }
  const auto& inputType = childNode->outputType();

  std::vector<std::string> tableColumnNames;
  std::vector<std::string> partitionedKey;
  std::vector<bool> isPartitionColumns;
  tableColumnNames.reserve(writeRel.table_schema().names_size());

  VELOX_CHECK(writeRel.has_table_schema(), "WriteRel should have the table schema to store the column information");
  const auto& tableSchema = writeRel.table_schema();
  isPartitionColumns = SubstraitParser::parsePartitionColumns(tableSchema);

  for (const auto& name : tableSchema.names()) {
    tableColumnNames.emplace_back(name);
  }

  for (int i = 0; i < tableSchema.names_size(); i++) {
    if (isPartitionColumns[i]) {
      partitionedKey.emplace_back(tableColumnNames[i]);
    }
  }

  std::string writePath;
  if (writeFilesTempPath_.has_value()) {
    writePath = writeFilesTempPath_.value();
  } else {
    VELOX_CHECK(validationMode_, "WriteRel should have the write path before initializing the plan.");
    writePath = "";
  }

  // spark default compression code is snappy.
  common::CompressionKind compressionCodec = common::CompressionKind::CompressionKind_SNAPPY;
  if (writeRel.named_table().has_advanced_extension()) {
    if (SubstraitParser::configSetInOptimization(writeRel.named_table().advanced_extension(), "isSnappy=")) {
      compressionCodec = common::CompressionKind::CompressionKind_SNAPPY;
    } else if (SubstraitParser::configSetInOptimization(writeRel.named_table().advanced_extension(), "isGzip=")) {
      compressionCodec = common::CompressionKind::CompressionKind_GZIP;
    } else if (SubstraitParser::configSetInOptimization(writeRel.named_table().advanced_extension(), "isLzo=")) {
      compressionCodec = common::CompressionKind::CompressionKind_LZO;
    } else if (SubstraitParser::configSetInOptimization(writeRel.named_table().advanced_extension(), "isLz4=")) {
      compressionCodec = common::CompressionKind::CompressionKind_LZ4;
    } else if (SubstraitParser::configSetInOptimization(writeRel.named_table().advanced_extension(), "isZstd=")) {
      compressionCodec = common::CompressionKind::CompressionKind_ZSTD;
    } else if (SubstraitParser::configSetInOptimization(writeRel.named_table().advanced_extension(), "isNone=")) {
      compressionCodec = common::CompressionKind::CompressionKind_NONE;
    } else if (SubstraitParser::configSetInOptimization(
                   writeRel.named_table().advanced_extension(), "isUncompressed=")) {
      compressionCodec = common::CompressionKind::CompressionKind_NONE;
    }
  }

  // Do not hard-code connector ID and allow for connectors other than Hive.
  static const std::string kHiveConnectorId = "test-hive";

  return std::make_shared<core::TableWriteNode>(
      nextPlanNodeId(),
      inputType,
      tableColumnNames,
      nullptr, /*aggregationNode*/
      std::make_shared<core::InsertTableHandle>(
          kHiveConnectorId,
          makeHiveInsertTableHandle(
              tableColumnNames, /*inputType->names() clolumn name is different*/
              inputType->children(),
              partitionedKey,
              nullptr /*bucketProperty*/,
              makeLocationHandle(writePath),
              dwio::common::FileFormat::PARQUET, // Currently only support parquet format.
              compressionCodec)),
      (partitionedKey.size() > 0) ? true : false,
      exec::TableWriteTraits::outputType(nullptr),
      connector::CommitStrategy::kNoCommit,
      childNode);
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::ExpandRel& expandRel) {
  core::PlanNodePtr childNode;
  if (expandRel.has_input()) {
    childNode = toVeloxPlan(expandRel.input());
  } else {
    VELOX_FAIL("Child Rel is expected in ExpandRel.");
  }

  const auto& inputType = childNode->outputType();

  std::vector<std::vector<core::TypedExprPtr>> projectSetExprs;
  projectSetExprs.reserve(expandRel.fields_size());

  for (const auto& projections : expandRel.fields()) {
    std::vector<core::TypedExprPtr> projectExprs;
    projectExprs.reserve(projections.switching_field().duplicates_size());

    for (const auto& projectExpr : projections.switching_field().duplicates()) {
      if (projectExpr.has_selection()) {
        auto expression = exprConverter_->toVeloxExpr(projectExpr.selection(), inputType);
        projectExprs.emplace_back(expression);
      } else if (projectExpr.has_literal()) {
        auto expression = exprConverter_->toVeloxExpr(projectExpr.literal());
        projectExprs.emplace_back(expression);
      } else {
        VELOX_FAIL("The project in Expand Operator only support field or literal.");
      }
    }
    projectSetExprs.emplace_back(projectExprs);
  }

  auto projectSize = expandRel.fields()[0].switching_field().duplicates_size();
  std::vector<std::string> names;
  names.reserve(projectSize);
  for (int idx = 0; idx < projectSize; idx++) {
    names.push_back(SubstraitParser::makeNodeName(planNodeId_, idx));
  }

  return std::make_shared<core::ExpandNode>(nextPlanNodeId(), projectSetExprs, std::move(names), childNode);
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::GenerateRel& generateRel) {
  core::PlanNodePtr childNode;
  if (generateRel.has_input()) {
    childNode = toVeloxPlan(generateRel.input());
  } else {
    VELOX_FAIL("Child Rel is expected in GenerateRel.");
  }
  const auto& inputType = childNode->outputType();

  std::vector<core::FieldAccessTypedExprPtr> replicated;
  std::vector<core::FieldAccessTypedExprPtr> unnest;

  const auto& generator = generateRel.generator();
  const auto& requiredChildOutput = generateRel.child_output();

  replicated.reserve(requiredChildOutput.size());
  for (const auto& output : requiredChildOutput) {
    auto expression = exprConverter_->toVeloxExpr(output, inputType);
    auto expr_field = dynamic_cast<const core::FieldAccessTypedExpr*>(expression.get());
    VELOX_CHECK(expr_field != nullptr, " the output in Generate Operator only support field")

    replicated.emplace_back(std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expression));
  }

  auto projNode = std::dynamic_pointer_cast<const core::ProjectNode>(childNode);

  if (projNode != nullptr && projNode->names().size() > requiredChildOutput.size()) {
    // generator is a scalarfunction node -> explode(array(col, 'all'))
    // use the last one, this is ensure by scala code
    auto innerName = projNode->names().back();
    auto innerExpr = projNode->projections().back();

    auto innerType = innerExpr->type();
    auto unnestFieldExpr = std::make_shared<core::FieldAccessTypedExpr>(innerType, innerName);
    VELOX_CHECK_NOT_NULL(unnestFieldExpr, " the key in unnest Operator only support field");
    unnest.emplace_back(unnestFieldExpr);
  } else {
    // generator should be a array column -> explode(col)
    auto explodeFunc = generator.scalar_function();
    auto unnestExpr = exprConverter_->toVeloxExpr(explodeFunc.arguments(0).value(), inputType);
    auto unnestFieldExpr = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(unnestExpr);
    VELOX_CHECK_NOT_NULL(unnestFieldExpr, " the key in unnest Operator only support field");
    unnest.emplace_back(unnestFieldExpr);
  }

  // TODO(yuan): get from generator output
  std::vector<std::string> unnestNames;
  int unnestIndex = 0;
  for (const auto& variable : unnest) {
    if (variable->type()->isArray()) {
      unnestNames.emplace_back(fmt::format("C{}", unnestIndex++));
    } else if (variable->type()->isMap()) {
      unnestNames.emplace_back(fmt::format("C{}", unnestIndex++));
      unnestNames.emplace_back(fmt::format("C{}", unnestIndex++));
    } else {
      VELOX_FAIL(
          "Unexpected type of unnest variable. Expected ARRAY or MAP, but got {}.", variable->type()->toString());
    }
  }

  auto node = std::make_shared<core::UnnestNode>(
      nextPlanNodeId(), replicated, unnest, std::move(unnestNames), std::nullopt, childNode);

  return node;
}

const core::WindowNode::Frame createWindowFrame(
    const ::substrait::Expression_WindowFunction_Bound& lower_bound,
    const ::substrait::Expression_WindowFunction_Bound& upper_bound,
    const ::substrait::WindowType& type) {
  core::WindowNode::Frame frame;
  switch (type) {
    case ::substrait::WindowType::ROWS:
      frame.type = core::WindowNode::WindowType::kRows;
      break;
    case ::substrait::WindowType::RANGE:
      frame.type = core::WindowNode::WindowType::kRange;
      break;
    default:
      VELOX_FAIL("the window type only support ROWS and RANGE, and the input type is ", type);
  }

  auto boundTypeConversion = [](::substrait::Expression_WindowFunction_Bound boundType) -> core::WindowNode::BoundType {
    if (boundType.has_current_row()) {
      return core::WindowNode::BoundType::kCurrentRow;
    } else if (boundType.has_unbounded_following()) {
      return core::WindowNode::BoundType::kUnboundedFollowing;
    } else if (boundType.has_unbounded_preceding()) {
      return core::WindowNode::BoundType::kUnboundedPreceding;
    } else if (boundType.has_following()) {
      return core::WindowNode::BoundType::kFollowing;
    } else if (boundType.has_preceding()) {
      return core::WindowNode::BoundType::kPreceding;
    } else {
      VELOX_FAIL("The BoundType is not supported.");
    }
  };
  frame.startType = boundTypeConversion(lower_bound);
  switch (frame.startType) {
    case core::WindowNode::BoundType::kPreceding:
      // TODO: support non-literal expression.
      frame.startValue = std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(lower_bound.preceding().offset()));
      break;
    default:
      frame.startValue = nullptr;
  }
  frame.endType = boundTypeConversion(upper_bound);
  switch (frame.endType) {
    // TODO: support non-literal expression.
    case core::WindowNode::BoundType::kFollowing:
      frame.endValue = std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(upper_bound.following().offset()));
      break;
    default:
      frame.endValue = nullptr;
  }
  return frame;
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::WindowRel& windowRel) {
  core::PlanNodePtr childNode;
  if (windowRel.has_input()) {
    childNode = toVeloxPlan(windowRel.input());
  } else {
    VELOX_FAIL("Child Rel is expected in WindowRel.");
  }

  const auto& inputType = childNode->outputType();

  // Parse measures and get the window expressions.
  // Each measure represents one window expression.
  bool ignoreNulls = false;
  std::vector<core::WindowNode::Function> windowNodeFunctions;
  std::vector<std::string> windowColumnNames;

  windowNodeFunctions.reserve(windowRel.measures().size());
  for (const auto& smea : windowRel.measures()) {
    const auto& windowFunction = smea.measure();
    std::string funcName = SubstraitParser::findVeloxFunction(functionMap_, windowFunction.function_reference());
    std::vector<core::TypedExprPtr> windowParams;
    auto& argumentList = windowFunction.arguments();
    windowParams.reserve(argumentList.size());
    // For functions in kOffsetWindowFunctions (see Spark OffsetWindowFunctions),
    // we expect the last arg is passed for setting ignoreNulls.
    if (kOffsetWindowFunctions.find(funcName) != kOffsetWindowFunctions.end()) {
      int i = 0;
      for (; i < argumentList.size() - 1; i++) {
        windowParams.emplace_back(exprConverter_->toVeloxExpr(argumentList[i].value(), inputType));
      }
      auto constantTypedExpr = exprConverter_->toVeloxExpr(argumentList[i].value().literal());
      auto variant = constantTypedExpr->value();
      if (!variant.hasValue()) {
        VELOX_FAIL("Value is expected in variant for setting ignoreNulls.");
      }
      ignoreNulls = variant.value<bool>();
    } else {
      for (const auto& arg : argumentList) {
        windowParams.emplace_back(exprConverter_->toVeloxExpr(arg.value(), inputType));
      }
    }
    auto windowVeloxType = SubstraitParser::parseType(windowFunction.output_type());
    auto windowCall = std::make_shared<const core::CallTypedExpr>(windowVeloxType, std::move(windowParams), funcName);
    auto upperBound = windowFunction.upper_bound();
    auto lowerBound = windowFunction.lower_bound();
    auto type = windowFunction.window_type();

    windowColumnNames.push_back(windowFunction.column_name());

    windowNodeFunctions.push_back(
        {std::move(windowCall), createWindowFrame(lowerBound, upperBound, type), ignoreNulls});
  }

  // Construct partitionKeys
  std::vector<core::FieldAccessTypedExprPtr> partitionKeys;
  std::unordered_set<std::string> keyNames;
  const auto& partitions = windowRel.partition_expressions();
  partitionKeys.reserve(partitions.size());
  for (const auto& partition : partitions) {
    auto expression = exprConverter_->toVeloxExpr(partition, inputType);
    core::FieldAccessTypedExprPtr veloxPartitionKey =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expression);
    VELOX_USER_CHECK_NOT_NULL(veloxPartitionKey, "Window Operator only supports field partition key.");
    // Constructs unique parition keys.
    if (keyNames.insert(veloxPartitionKey->name()).second) {
      partitionKeys.emplace_back(veloxPartitionKey);
    }
  }
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  const auto& [rawSortingKeys, rawSortingOrders] = processSortField(windowRel.sorts(), inputType);
  for (vector_size_t i = 0; i < rawSortingKeys.size(); ++i) {
    // Constructs unique sort keys and excludes keys overlapped with partition keys.
    if (keyNames.insert(rawSortingKeys[i]->name()).second) {
      sortingKeys.emplace_back(rawSortingKeys[i]);
      sortingOrders.emplace_back(rawSortingOrders[i]);
    }
  }

  if (windowRel.has_advanced_extension() &&
      SubstraitParser::configSetInOptimization(windowRel.advanced_extension(), "isStreaming=")) {
    return std::make_shared<core::WindowNode>(
        nextPlanNodeId(),
        partitionKeys,
        sortingKeys,
        sortingOrders,
        windowColumnNames,
        windowNodeFunctions,
        true /*inputsSorted*/,
        childNode);
  } else {
    return std::make_shared<core::WindowNode>(
        nextPlanNodeId(),
        partitionKeys,
        sortingKeys,
        sortingOrders,
        windowColumnNames,
        windowNodeFunctions,
        false /*inputsSorted*/,
        childNode);
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::SortRel& sortRel) {
  auto childNode = convertSingleInput<::substrait::SortRel>(sortRel);
  auto [sortingKeys, sortingOrders] = processSortField(sortRel.sorts(), childNode->outputType());
  return std::make_shared<core::OrderByNode>(
      nextPlanNodeId(), sortingKeys, sortingOrders, false /*isPartial*/, childNode);
}

std::pair<std::vector<core::FieldAccessTypedExprPtr>, std::vector<core::SortOrder>>
SubstraitToVeloxPlanConverter::processSortField(
    const ::google::protobuf::RepeatedPtrField<::substrait::SortField>& sortFields,
    const RowTypePtr& inputType) {
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  sortingKeys.reserve(sortFields.size());
  sortingOrders.reserve(sortFields.size());

  for (const auto& sort : sortFields) {
    sortingOrders.emplace_back(toSortOrder(sort));

    if (sort.has_expr()) {
      auto expression = exprConverter_->toVeloxExpr(sort.expr(), inputType);
      auto fieldExpr = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expression);
      VELOX_USER_CHECK_NOT_NULL(fieldExpr, "Sort Operator only supports field sorting key");
      sortingKeys.emplace_back(fieldExpr);
    }
  }
  return {sortingKeys, sortingOrders};
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::FilterRel& filterRel) {
  auto childNode = convertSingleInput<::substrait::FilterRel>(filterRel);
  auto filterNode = std::make_shared<core::FilterNode>(
      nextPlanNodeId(), exprConverter_->toVeloxExpr(filterRel.condition(), childNode->outputType()), childNode);

  if (filterRel.has_common()) {
    return processEmit(filterRel.common(), std::move(filterNode));
  } else {
    return filterNode;
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::FetchRel& fetchRel) {
  core::PlanNodePtr childNode;
  // Check the input of fetchRel, if it's sortRel, convert them into
  // topNNode. otherwise, to limitNode.
  ::substrait::SortRel sortRel;
  bool topNFlag;
  if (fetchRel.has_input()) {
    topNFlag = fetchRel.input().has_sort();
    if (topNFlag) {
      sortRel = fetchRel.input().sort();
      childNode = toVeloxPlan(sortRel.input());
    } else {
      childNode = toVeloxPlan(fetchRel.input());
    }
  } else {
    VELOX_FAIL("Child Rel is expected in FetchRel.");
  }

  if (topNFlag) {
    auto [sortingKeys, sortingOrders] = processSortField(sortRel.sorts(), childNode->outputType());

    VELOX_CHECK_EQ(fetchRel.offset(), 0);

    return std::make_shared<core::TopNNode>(
        nextPlanNodeId(), sortingKeys, sortingOrders, (int32_t)fetchRel.count(), false /*isPartial*/, childNode);

  } else {
    return std::make_shared<core::LimitNode>(
        nextPlanNodeId(), (int32_t)fetchRel.offset(), (int32_t)fetchRel.count(), false /*isPartial*/, childNode);
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::constructValueStreamNode(
    const ::substrait::ReadRel& readRel,
    int32_t streamIdx) {
  // Get the input schema of this iterator.
  uint64_t colNum = 0;
  std::vector<TypePtr> veloxTypeList;
  if (readRel.has_base_schema()) {
    const auto& baseSchema = readRel.base_schema();
    // Input names is not used. Instead, new input/output names will be created
    // because the ValueStreamNode in Velox does not support name change.
    colNum = baseSchema.names().size();
    veloxTypeList = SubstraitParser::parseNamedStruct(baseSchema);
  }

  std::vector<std::string> outNames;
  outNames.reserve(colNum);
  for (int idx = 0; idx < colNum; idx++) {
    auto colName = SubstraitParser::makeNodeName(planNodeId_, idx);
    outNames.emplace_back(colName);
  }

  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));
  auto node = valueStreamNodeFactory_(nextPlanNodeId(), pool_, streamIdx, outputType);

  auto splitInfo = std::make_shared<SplitInfo>();
  splitInfo->isStream = true;
  splitInfoMap_[node->id()] = splitInfo;
  return node;
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::ReadRel& readRel) {
  // emit is not allowed in TableScanNode and ValuesNode related
  // outputs
  if (readRel.has_common()) {
    VELOX_USER_CHECK(
        !readRel.common().has_emit(), "Emit not supported for ValuesNode and TableScanNode related Substrait plans.");
  }

  // Check if the ReadRel specifies an input of stream. If yes, build ValueStreamNode as the data source.
  auto streamIdx = getStreamIndex(readRel);
  if (streamIdx >= 0) {
    return constructValueStreamNode(readRel, streamIdx);
  }

  // Otherwise, will create TableScan node for ReadRel.
  auto splitInfo = std::make_shared<SplitInfo>();
  if (!validationMode_) {
    VELOX_CHECK_LT(splitInfoIdx_, splitInfos_.size(), "Plan must have readRel and related split info.");
    splitInfo = splitInfos_[splitInfoIdx_++];
  }

  // Get output names and types.
  std::vector<std::string> colNameList;
  std::vector<TypePtr> veloxTypeList;
  std::vector<bool> isPartitionColumns;
  // Convert field names into lower case when not case-sensitive.
  std::shared_ptr<const facebook::velox::Config> veloxCfg =
      std::make_shared<const facebook::velox::core::MemConfigMutable>(confMap_);
  bool asLowerCase = !veloxCfg->get<bool>(kCaseSensitive, false);
  if (readRel.has_base_schema()) {
    const auto& baseSchema = readRel.base_schema();
    colNameList.reserve(baseSchema.names().size());
    for (const auto& name : baseSchema.names()) {
      std::string fieldName = name;
      if (asLowerCase) {
        folly::toLowerAscii(fieldName);
      }
      colNameList.emplace_back(fieldName);
    }
    veloxTypeList = SubstraitParser::parseNamedStruct(baseSchema, asLowerCase);
    isPartitionColumns = SubstraitParser::parsePartitionColumns(baseSchema);
  }

  // Do not hard-code connector ID and allow for connectors other than Hive.
  static const std::string kHiveConnectorId = "test-hive";

  // Velox requires Filter Pushdown must being enabled.
  bool filterPushdownEnabled = true;
  std::shared_ptr<connector::hive::HiveTableHandle> tableHandle;
  if (!readRel.has_filter()) {
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        kHiveConnectorId, "hive_table", filterPushdownEnabled, connector::hive::SubfieldFilters{}, nullptr);
  } else {
    // Flatten the conditions connected with 'and'.
    std::vector<::substrait::Expression_ScalarFunction> scalarFunctions;
    std::vector<::substrait::Expression_SingularOrList> singularOrLists;
    std::vector<::substrait::Expression_IfThen> ifThens;
    flattenConditions(readRel.filter(), scalarFunctions, singularOrLists, ifThens);

    // The vector's subscript stands for the column index.
    std::vector<RangeRecorder> rangeRecorders(veloxTypeList.size());

    // Separate the filters to be two parts. The subfield part can be
    // pushed down.
    std::vector<::substrait::Expression_ScalarFunction> subfieldFunctions;
    std::vector<::substrait::Expression_ScalarFunction> remainingFunctions;
    std::vector<::substrait::Expression_SingularOrList> subfieldOrLists;
    std::vector<::substrait::Expression_SingularOrList> remainingOrLists;

    separateFilters(
        rangeRecorders,
        scalarFunctions,
        subfieldFunctions,
        remainingFunctions,
        singularOrLists,
        subfieldOrLists,
        remainingOrLists,
        veloxTypeList,
        splitInfo->format);

    // Create subfield filters based on the constructed filter info map.
    auto subfieldFilters = createSubfieldFilters(colNameList, veloxTypeList, subfieldFunctions, subfieldOrLists);
    // Connect the remaining filters with 'and'.
    auto remainingFilter = connectWithAnd(colNameList, veloxTypeList, remainingFunctions, remainingOrLists, ifThens);

    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        kHiveConnectorId, "hive_table", filterPushdownEnabled, std::move(subfieldFilters), remainingFilter);
  }

  // Get assignments and out names.
  std::vector<std::string> outNames;
  outNames.reserve(colNameList.size());
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> assignments;
  for (int idx = 0; idx < colNameList.size(); idx++) {
    auto outName = SubstraitParser::makeNodeName(planNodeId_, idx);
    auto columnType = isPartitionColumns[idx] ? connector::hive::HiveColumnHandle::ColumnType::kPartitionKey
                                              : connector::hive::HiveColumnHandle::ColumnType::kRegular;
    assignments[outName] = std::make_shared<connector::hive::HiveColumnHandle>(
        colNameList[idx], columnType, veloxTypeList[idx], veloxTypeList[idx]);
    outNames.emplace_back(outName);
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));

  if (readRel.has_virtual_table()) {
    return toVeloxPlan(readRel, outputType);
  } else {
    auto tableScanNode = std::make_shared<core::TableScanNode>(
        nextPlanNodeId(), std::move(outputType), std::move(tableHandle), std::move(assignments));
    // Set split info map.
    splitInfoMap_[tableScanNode->id()] = splitInfo;
    return tableScanNode;
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ReadRel& readRel,
    const RowTypePtr& type) {
  ::substrait::ReadRel_VirtualTable readVirtualTable = readRel.virtual_table();
  int64_t numVectors = readVirtualTable.values_size();
  int64_t numColumns = type->size();
  int64_t valueFieldNums = readVirtualTable.values(numVectors - 1).fields_size();
  std::vector<RowVectorPtr> vectors;
  vectors.reserve(numVectors);

  int64_t batchSize;
  // For the empty vectors, eg,vectors = makeRowVector(ROW({}, {}), 1).
  if (numColumns == 0) {
    batchSize = 1;
  } else {
    batchSize = valueFieldNums / numColumns;
  }

  for (int64_t index = 0; index < numVectors; ++index) {
    std::vector<VectorPtr> children;
    ::substrait::Expression_Literal_Struct rowValue = readRel.virtual_table().values(index);
    auto fieldSize = rowValue.fields_size();
    VELOX_CHECK_EQ(fieldSize, batchSize * numColumns);

    for (int64_t col = 0; col < numColumns; ++col) {
      const TypePtr& outputChildType = type->childAt(col);
      std::vector<variant> batchChild;
      batchChild.reserve(batchSize);
      for (int64_t batchId = 0; batchId < batchSize; batchId++) {
        // each value in the batch
        auto fieldIdx = col * batchSize + batchId;
        ::substrait::Expression_Literal field = rowValue.fields(fieldIdx);

        auto expr = exprConverter_->toVeloxExpr(field);
        if (auto constantExpr = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
          if (!constantExpr->hasValueVector()) {
            batchChild.emplace_back(constantExpr->value());
          } else {
            VELOX_UNSUPPORTED("Values node with complex type values is not supported yet");
          }
        } else {
          VELOX_FAIL("Expected constant expression");
        }
      }
      children.emplace_back(setVectorFromVariants(outputChildType, batchChild, pool_));
    }

    vectors.emplace_back(std::make_shared<RowVector>(pool_, type, nullptr, batchSize, children));
  }

  return std::make_shared<core::ValuesNode>(nextPlanNodeId(), std::move(vectors));
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::Rel& rel) {
  if (rel.has_aggregate()) {
    return toVeloxPlan(rel.aggregate());
  } else if (rel.has_project()) {
    return toVeloxPlan(rel.project());
  } else if (rel.has_filter()) {
    return toVeloxPlan(rel.filter());
  } else if (rel.has_join()) {
    return toVeloxPlan(rel.join());
  } else if (rel.has_read()) {
    return toVeloxPlan(rel.read());
  } else if (rel.has_sort()) {
    return toVeloxPlan(rel.sort());
  } else if (rel.has_expand()) {
    return toVeloxPlan(rel.expand());
  } else if (rel.has_generate()) {
    return toVeloxPlan(rel.generate());
  } else if (rel.has_fetch()) {
    return toVeloxPlan(rel.fetch());
  } else if (rel.has_window()) {
    return toVeloxPlan(rel.window());
  } else if (rel.has_write()) {
    return toVeloxPlan(rel.write());
  } else {
    VELOX_NYI("Substrait conversion not supported for Rel.");
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::RelRoot& root) {
  // TODO: Use the names as the output names for the whole computing.
  // const auto& names = root.names();
  if (root.has_input()) {
    const auto& rel = root.input();
    return toVeloxPlan(rel);
  } else {
    VELOX_FAIL("Input is expected in RelRoot.");
  }
}

core::PlanNodePtr SubstraitToVeloxPlanConverter::toVeloxPlan(const ::substrait::Plan& substraitPlan) {
  VELOX_CHECK(checkTypeExtension(substraitPlan), "The type extension only have unknown type.")
  // Construct the function map based on the Substrait representation,
  // and initialize the expression converter with it.
  constructFunctionMap(substraitPlan);

  // In fact, only one RelRoot or Rel is expected here.
  VELOX_CHECK_EQ(substraitPlan.relations_size(), 1);
  const auto& rel = substraitPlan.relations(0);
  if (rel.has_root()) {
    return toVeloxPlan(rel.root());
  } else if (rel.has_rel()) {
    return toVeloxPlan(rel.rel());
  } else {
    VELOX_FAIL("RelRoot or Rel is expected in Plan.");
  }
}

std::string SubstraitToVeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

void SubstraitToVeloxPlanConverter::constructFunctionMap(const ::substrait::Plan& substraitPlan) {
  // Construct the function map based on the Substrait representation.
  for (const auto& extension : substraitPlan.extensions()) {
    if (!extension.has_extension_function()) {
      continue;
    }
    const auto& sFmap = extension.extension_function();
    auto id = sFmap.function_anchor();
    auto name = sFmap.name();
    functionMap_[id] = name;
  }
  exprConverter_ = std::make_unique<SubstraitVeloxExprConverter>(pool_, functionMap_);
}

void SubstraitToVeloxPlanConverter::flattenConditions(
    const ::substrait::Expression& substraitFilter,
    std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions,
    std::vector<::substrait::Expression_SingularOrList>& singularOrLists,
    std::vector<::substrait::Expression_IfThen>& ifThens) {
  auto typeCase = substraitFilter.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kScalarFunction: {
      const auto& sFunc = substraitFilter.scalar_function();
      auto filterNameSpec = SubstraitParser::findFunctionSpec(functionMap_, sFunc.function_reference());
      // TODO: Only and relation is supported here.
      if (SubstraitParser::getNameBeforeDelimiter(filterNameSpec) == "and") {
        for (const auto& sCondition : sFunc.arguments()) {
          flattenConditions(sCondition.value(), scalarFunctions, singularOrLists, ifThens);
        }
      } else {
        scalarFunctions.emplace_back(sFunc);
      }
      break;
    }
    case ::substrait::Expression::RexTypeCase::kSingularOrList: {
      singularOrLists.emplace_back(substraitFilter.singular_or_list());
      break;
    }
    case ::substrait::Expression::RexTypeCase::kIfThen: {
      ifThens.emplace_back(substraitFilter.if_then());
      break;
    }
    default:
      VELOX_NYI("GetFlatConditions not supported for type '{}'", typeCase);
  }
}

std::string SubstraitToVeloxPlanConverter::findFuncSpec(uint64_t id) {
  return SubstraitParser::findFunctionSpec(functionMap_, id);
}

int32_t SubstraitToVeloxPlanConverter::getStreamIndex(const ::substrait::ReadRel& sRead) {
  if (validationMode_) {
    return -1;
  }
  if (sRead.has_local_files()) {
    const auto& fileList = sRead.local_files().items();
    if (fileList.size() == 0) {
      // bucketed scan may contains empty file list
      return -1;
    }
    // The stream input will be specified with the format of
    // "iterator:${index}".
    std::string filePath = fileList[0].uri_file();
    std::string prefix = "iterator:";
    std::size_t pos = filePath.find(prefix);
    if (pos == std::string::npos) {
      return -1;
    }

    // Get the index.
    std::string idxStr = filePath.substr(pos + prefix.size(), filePath.size());
    try {
      return stoi(idxStr);
    } catch (const std::exception& err) {
      VELOX_FAIL(err.what());
    }
  }
  return -1;
}

void SubstraitToVeloxPlanConverter::extractJoinKeys(
    const ::substrait::Expression& joinExpression,
    std::vector<const ::substrait::Expression::FieldReference*>& leftExprs,
    std::vector<const ::substrait::Expression::FieldReference*>& rightExprs) {
  std::vector<const ::substrait::Expression*> expressions;
  expressions.push_back(&joinExpression);
  while (!expressions.empty()) {
    auto visited = expressions.back();
    expressions.pop_back();
    if (visited->rex_type_case() == ::substrait::Expression::RexTypeCase::kScalarFunction) {
      const auto& funcName = SubstraitParser::getNameBeforeDelimiter(
          SubstraitParser::findVeloxFunction(functionMap_, visited->scalar_function().function_reference()));
      const auto& args = visited->scalar_function().arguments();
      if (funcName == "and") {
        expressions.push_back(&args[0].value());
        expressions.push_back(&args[1].value());
      } else if (funcName == "eq" || funcName == "equalto" || funcName == "decimal_equalto") {
        VELOX_CHECK(std::all_of(args.cbegin(), args.cend(), [](const ::substrait::FunctionArgument& arg) {
          return arg.value().has_selection();
        }));
        leftExprs.push_back(&args[0].value().selection());
        rightExprs.push_back(&args[1].value().selection());
      } else {
        VELOX_NYI("Join condition {} not supported.", funcName);
      }
    } else {
      VELOX_FAIL("Unable to parse from join expression: {}", joinExpression.DebugString());
    }
  }
}

connector::hive::SubfieldFilters SubstraitToVeloxPlanConverter::createSubfieldFilters(
    const std::vector<std::string>& inputNameList,
    const std::vector<TypePtr>& inputTypeList,
    const std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions,
    const std::vector<::substrait::Expression_SingularOrList>& singularOrLists) {
  // The vector's subscript stands for the column index.
  std::vector<FilterInfo> columnToFilterInfo(inputTypeList.size());

  // Process scalarFunctions.
  for (const auto& scalarFunction : scalarFunctions) {
    auto filterNameSpec = SubstraitParser::findFunctionSpec(functionMap_, scalarFunction.function_reference());
    auto filterName = SubstraitParser::getNameBeforeDelimiter(filterNameSpec);

    if (filterName == sNot) {
      VELOX_CHECK(scalarFunction.arguments().size() == 1);
      auto expr = scalarFunction.arguments()[0].value();
      if (expr.has_scalar_function()) {
        // Set its child to filter info with reverse enabled.
        setFilterInfo(scalarFunction.arguments()[0].value().scalar_function(), inputTypeList, columnToFilterInfo, true);
      } else {
        // TODO: support push down of Not In.
        VELOX_NYI("Scalar function expected.");
      }
    } else if (filterName == sOr) {
      VELOX_CHECK(scalarFunction.arguments().size() == 2);
      VELOX_CHECK(std::all_of(
          scalarFunction.arguments().cbegin(),
          scalarFunction.arguments().cend(),
          [](const ::substrait::FunctionArgument& arg) {
            return arg.value().has_scalar_function() || arg.value().has_singular_or_list();
          }));

      // Set the children functions to filter info. They should be
      // effective to the same field.
      for (const auto& arg : scalarFunction.arguments()) {
        const auto& expr = arg.value();
        if (expr.has_scalar_function()) {
          setFilterInfo(arg.value().scalar_function(), inputTypeList, columnToFilterInfo);
        } else if (expr.has_singular_or_list()) {
          setFilterInfo(expr.singular_or_list(), columnToFilterInfo);
        } else {
          VELOX_NYI("Scalar function or SingularOrList expected.");
        }
      }
    } else {
      setFilterInfo(scalarFunction, inputTypeList, columnToFilterInfo);
    }
  }

  // Process singularOrLists.
  for (const auto& list : singularOrLists) {
    setFilterInfo(list, columnToFilterInfo);
  }

  return mapToFilters(inputNameList, inputTypeList, columnToFilterInfo);
}

bool SubstraitToVeloxPlanConverter::fieldOrWithLiteral(
    const ::google::protobuf::RepeatedPtrField<::substrait::FunctionArgument>& arguments,
    uint32_t& fieldIndex) {
  if (arguments.size() == 1) {
    if (arguments[0].value().has_selection()) {
      // Only field exists.
      fieldIndex = SubstraitParser::parseReferenceSegment(arguments[0].value().selection().direct_reference());
      return true;
    } else {
      return false;
    }
  }

  if (arguments.size() != 2) {
    // Not the field and literal combination.
    return false;
  }
  bool fieldExists = false;
  bool literalExists = false;
  for (const auto& param : arguments) {
    auto typeCase = param.value().rex_type_case();
    switch (typeCase) {
      case ::substrait::Expression::RexTypeCase::kSelection:
        fieldIndex = SubstraitParser::parseReferenceSegment(param.value().selection().direct_reference());
        fieldExists = true;
        break;
      case ::substrait::Expression::RexTypeCase::kLiteral:
        literalExists = true;
        break;
      default:
        break;
    }
  }
  // Whether the field and literal both exist.
  return fieldExists && literalExists;
}

bool SubstraitToVeloxPlanConverter::childrenFunctionsOnSameField(
    const ::substrait::Expression_ScalarFunction& function) {
  // Get the column indices of the children functions.
  std::vector<int32_t> colIndices;
  for (const auto& arg : function.arguments()) {
    if (arg.value().has_scalar_function()) {
      const auto& scalarFunction = arg.value().scalar_function();
      for (const auto& param : scalarFunction.arguments()) {
        if (param.value().has_selection()) {
          const auto& field = param.value().selection();
          VELOX_CHECK(field.has_direct_reference());
          int32_t colIdx = SubstraitParser::parseReferenceSegment(field.direct_reference());
          colIndices.emplace_back(colIdx);
        }
      }
    } else if (arg.value().has_singular_or_list()) {
      const auto& singularOrList = arg.value().singular_or_list();
      int32_t colIdx = getColumnIndexFromSingularOrList(singularOrList);
      colIndices.emplace_back(colIdx);
    } else {
      return false;
    }
  }

  if (std::all_of(colIndices.begin(), colIndices.end(), [&](uint32_t idx) { return idx == colIndices[0]; })) {
    // All indices are the same.
    return true;
  }
  return false;
}

bool SubstraitToVeloxPlanConverter::canPushdownFunction(
    const ::substrait::Expression_ScalarFunction& scalarFunction,
    const std::string& filterName,
    uint32_t& fieldIdx) {
  // Condtions can be pushed down.
  static const std::unordered_set<std::string> supportedFunctions = {sIsNotNull, sIsNull, sGte, sGt, sLte, sLt, sEqual};

  bool canPushdown = false;
  if (supportedFunctions.find(filterName) != supportedFunctions.end() &&
      fieldOrWithLiteral(scalarFunction.arguments(), fieldIdx)) {
    // The arg should be field or field with literal.
    canPushdown = true;
  }
  return canPushdown;
}

bool SubstraitToVeloxPlanConverter::canPushdownNot(
    const ::substrait::Expression_ScalarFunction& scalarFunction,
    std::vector<RangeRecorder>& rangeRecorders) {
  VELOX_CHECK(scalarFunction.arguments().size() == 1, "Only one arg is expected for Not.");
  const auto& notArg = scalarFunction.arguments()[0];
  if (!notArg.value().has_scalar_function()) {
    // Not for a Boolean Literal or Or List is not supported curretly.
    // It can be pushed down with an AlwaysTrue or AlwaysFalse Range.
    return false;
  }

  auto argFunction =
      SubstraitParser::findFunctionSpec(functionMap_, notArg.value().scalar_function().function_reference());
  auto functionName = SubstraitParser::getNameBeforeDelimiter(argFunction);

  static const std::unordered_set<std::string> supportedNotFunctions = {sGte, sGt, sLte, sLt, sEqual};

  uint32_t fieldIdx;
  bool isFieldOrWithLiteral = fieldOrWithLiteral(notArg.value().scalar_function().arguments(), fieldIdx);

  if (supportedNotFunctions.find(functionName) != supportedNotFunctions.end() && isFieldOrWithLiteral &&
      rangeRecorders.at(fieldIdx).setCertainRangeForFunction(functionName, true /*reverse*/)) {
    return true;
  }
  return false;
}

bool SubstraitToVeloxPlanConverter::canPushdownOr(
    const ::substrait::Expression_ScalarFunction& scalarFunction,
    std::vector<RangeRecorder>& rangeRecorders) {
  // OR Conditon whose children functions are on different columns is not
  // supported to be pushed down.
  if (!childrenFunctionsOnSameField(scalarFunction)) {
    return false;
  }

  static const std::unordered_set<std::string> supportedOrFunctions = {sIsNotNull, sGte, sGt, sLte, sLt, sEqual};

  for (const auto& arg : scalarFunction.arguments()) {
    if (arg.value().has_scalar_function()) {
      auto nameSpec =
          SubstraitParser::findFunctionSpec(functionMap_, arg.value().scalar_function().function_reference());
      auto functionName = SubstraitParser::getNameBeforeDelimiter(nameSpec);

      uint32_t fieldIdx;
      bool isFieldOrWithLiteral = fieldOrWithLiteral(arg.value().scalar_function().arguments(), fieldIdx);
      if (supportedOrFunctions.find(functionName) == supportedOrFunctions.end() || !isFieldOrWithLiteral ||
          !rangeRecorders.at(fieldIdx).setCertainRangeForFunction(
              functionName, false /*reverse*/, true /*forOrRelation*/)) {
        // The arg should be field or field with literal.
        return false;
      }
    } else if (arg.value().has_singular_or_list()) {
      const auto& singularOrList = arg.value().singular_or_list();
      if (!canPushdownSingularOrList(singularOrList, true)) {
        return false;
      }
      uint32_t fieldIdx = getColumnIndexFromSingularOrList(singularOrList);
      // Disable IN pushdown for int-like types.
      if (!rangeRecorders.at(fieldIdx).setInRange(true /*forOrRelation*/)) {
        return false;
      }
    } else {
      // Or relation betweeen other expressions is not supported to be pushded
      // down currently.
      return false;
    }
  }
  return true;
}

void SubstraitToVeloxPlanConverter::separateFilters(
    std::vector<RangeRecorder>& rangeRecorders,
    const std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions,
    std::vector<::substrait::Expression_ScalarFunction>& subfieldFunctions,
    std::vector<::substrait::Expression_ScalarFunction>& remainingFunctions,
    const std::vector<::substrait::Expression_SingularOrList>& singularOrLists,
    std::vector<::substrait::Expression_SingularOrList>& subfieldOrLists,
    std::vector<::substrait::Expression_SingularOrList>& remainingOrLists,
    const std::vector<TypePtr>& veloxTypeList,
    const dwio::common::FileFormat& format) {
  for (const auto& singularOrList : singularOrLists) {
    if (!canPushdownSingularOrList(singularOrList)) {
      remainingOrLists.emplace_back(singularOrList);
      continue;
    }
    uint32_t colIdx = getColumnIndexFromSingularOrList(singularOrList);
    if (rangeRecorders.at(colIdx).setInRange()) {
      subfieldOrLists.emplace_back(singularOrList);
    } else {
      remainingOrLists.emplace_back(singularOrList);
    }
  }

  for (const auto& scalarFunction : scalarFunctions) {
    auto filterNameSpec = SubstraitParser::findFunctionSpec(functionMap_, scalarFunction.function_reference());
    auto filterName = SubstraitParser::getNameBeforeDelimiter(filterNameSpec);
    // Add all decimal filters to remaining functions because their pushdown are not supported.
    if (format == dwio::common::FileFormat::ORC && scalarFunction.arguments().size() > 0) {
      auto value = scalarFunction.arguments().at(0).value();
      if (value.has_selection()) {
        uint32_t fieldIndex = SubstraitParser::parseReferenceSegment(value.selection().direct_reference());
        if (!veloxTypeList.empty() && veloxTypeList.at(fieldIndex)->isDecimal()) {
          remainingFunctions.emplace_back(scalarFunction);
          continue;
        }
      }
    }

    // Check whether NOT and OR functions can be pushed down.
    // If yes, the scalar function will be added into the subfield functions.
    if (filterName == sNot) {
      if (canPushdownNot(scalarFunction, rangeRecorders)) {
        subfieldFunctions.emplace_back(scalarFunction);
      } else {
        remainingFunctions.emplace_back(scalarFunction);
      }
    } else if (filterName == sOr) {
      if (canPushdownOr(scalarFunction, rangeRecorders)) {
        subfieldFunctions.emplace_back(scalarFunction);
      } else {
        remainingFunctions.emplace_back(scalarFunction);
      }
    } else {
      // Check if the condition is supported to be pushed down.
      uint32_t fieldIdx;
      if (canPushdownFunction(scalarFunction, filterName, fieldIdx) &&
          rangeRecorders.at(fieldIdx).setCertainRangeForFunction(filterName)) {
        subfieldFunctions.emplace_back(scalarFunction);
      } else {
        remainingFunctions.emplace_back(scalarFunction);
      }
    }
  }
}

bool SubstraitToVeloxPlanConverter::RangeRecorder::setCertainRangeForFunction(
    const std::string& functionName,
    bool reverse,
    bool forOrRelation) {
  if (functionName == sLt || functionName == sLte) {
    if (reverse) {
      return setLeftBound(forOrRelation);
    } else {
      return setRightBound(forOrRelation);
    }
  } else if (functionName == sGt || functionName == sGte) {
    if (reverse) {
      return setRightBound(forOrRelation);
    } else {
      return setLeftBound(forOrRelation);
    }
  } else if (functionName == sEqual) {
    if (reverse) {
      // Not equal means lt or gt.
      return setMultiRange();
    } else {
      return setLeftBound(forOrRelation) && setRightBound(forOrRelation);
    }
  } else if (functionName == sOr) {
    if (reverse) {
      // Not supported.
      return false;
    } else {
      return setMultiRange();
    }
  } else if (functionName == sIsNotNull) {
    if (reverse) {
      // Not supported.
      return false;
    } else {
      // Is not null can always coexist with the other range.
      return true;
    }
  } else if (functionName == sIsNull) {
    if (reverse) {
      return setCertainRangeForFunction(sIsNotNull, false, forOrRelation);
    } else {
      return setIsNull();
    }
  } else {
    return false;
  }
}

void SubstraitToVeloxPlanConverter::setColumnFilterInfo(
    const std::string& filterName,
    std::optional<variant> literalVariant,
    FilterInfo& columnFilterInfo,
    bool reverse) {
  if (filterName == sIsNotNull) {
    if (reverse) {
      columnFilterInfo.setNull();
    } else {
      columnFilterInfo.forbidsNull();
    }
  } else if (filterName == sIsNull) {
    if (reverse) {
      columnFilterInfo.forbidsNull();
    } else {
      columnFilterInfo.setNull();
    }
  } else if (filterName == sGte) {
    if (reverse) {
      columnFilterInfo.setUpper(literalVariant, true);
    } else {
      columnFilterInfo.setLower(literalVariant, false);
    }
  } else if (filterName == sGt) {
    if (reverse) {
      columnFilterInfo.setUpper(literalVariant, false);
    } else {
      columnFilterInfo.setLower(literalVariant, true);
    }
  } else if (filterName == sLte) {
    if (reverse) {
      columnFilterInfo.setLower(literalVariant, true);
    } else {
      columnFilterInfo.setUpper(literalVariant, false);
    }
  } else if (filterName == sLt) {
    if (reverse) {
      columnFilterInfo.setLower(literalVariant, false);
    } else {
      columnFilterInfo.setUpper(literalVariant, true);
    }
  } else if (filterName == sEqual) {
    if (reverse) {
      columnFilterInfo.setNotValue(literalVariant);
    } else {
      columnFilterInfo.setLower(literalVariant, false);
      columnFilterInfo.setUpper(literalVariant, false);
    }
  } else {
    VELOX_NYI("setColumnFilterInfo not supported for filter name '{}'", filterName);
  }
}

void SubstraitToVeloxPlanConverter::setFilterInfo(
    const ::substrait::Expression_ScalarFunction& scalarFunction,
    const std::vector<TypePtr>& inputTypeList,
    std::vector<FilterInfo>& columnToFilterInfo,
    bool reverse) {
  auto nameSpec = SubstraitParser::findFunctionSpec(functionMap_, scalarFunction.function_reference());
  auto functionName = SubstraitParser::getNameBeforeDelimiter(nameSpec);

  // Extract the column index and column bound from the scalar function.
  std::optional<uint32_t> colIdx;
  std::optional<::substrait::Expression_Literal> substraitLit;
  std::vector<std::string> typeCases;

  for (const auto& param : scalarFunction.arguments()) {
    auto typeCase = param.value().rex_type_case();
    switch (typeCase) {
      case ::substrait::Expression::RexTypeCase::kSelection:
        typeCases.emplace_back("kSelection");
        colIdx = SubstraitParser::parseReferenceSegment(param.value().selection().direct_reference());
        break;
      case ::substrait::Expression::RexTypeCase::kLiteral:
        typeCases.emplace_back("kLiteral");
        substraitLit = param.value().literal();
        break;
      default:
        VELOX_NYI("Substrait conversion not supported for arg type '{}'", typeCase);
    }
  }

  static const std::unordered_map<std::string, std::string> functionRevertMap = {
      {sLt, sGt}, {sGt, sLt}, {sGte, sLte}, {sLte, sGte}};

  // Handle the case where literal is before the variable in a binary function, e.g. "123 < q1".
  if (typeCases.size() > 1 && (typeCases[0] == "kLiteral" && typeCases[1] == "kSelection")) {
    auto x = functionRevertMap.find(functionName);
    if (x != functionRevertMap.end()) {
      // Change the function name: lt => gt, gt => lt, gte => lte, lte => gte.
      functionName = x->second;
    }
  }

  if (!colIdx.has_value()) {
    VELOX_NYI("Column index is expected in subfield filters creation.");
  }

  // Set the extracted bound to the specific column.
  uint32_t colIdxVal = colIdx.value();
  std::optional<variant> val;

  auto inputType = inputTypeList[colIdxVal];
  if (inputType->isDate()) {
    if (substraitLit) {
      val = variant(int(substraitLit.value().date()));
    }
    setColumnFilterInfo(functionName, val, columnToFilterInfo[colIdxVal], reverse);
    return;
  }
  switch (inputType->kind()) {
    case TypeKind::TINYINT:
      if (substraitLit) {
        val = variant(substraitLit.value().i8());
      }
      break;
    case TypeKind::SMALLINT:
      if (substraitLit) {
        val = variant(static_cast<int16_t>(substraitLit.value().i16()));
      }
      break;
    case TypeKind::INTEGER:
      if (substraitLit) {
        val = variant(substraitLit.value().i32());
      }
      break;
    case TypeKind::BIGINT:
      if (substraitLit) {
        if (inputType->isShortDecimal()) {
          auto decimal = substraitLit.value().decimal().value();
          int128_t decimalValue;
          memcpy(&decimalValue, decimal.c_str(), 16);
          val = variant(static_cast<int64_t>(decimalValue));
        } else {
          val = variant(substraitLit.value().i64());
        }
      }
      break;
    case TypeKind::REAL:
      if (substraitLit) {
        val = variant(substraitLit.value().fp32());
      }
      break;
    case TypeKind::DOUBLE:
      if (substraitLit) {
        val = variant(substraitLit.value().fp64());
      }
      break;
    case TypeKind::BOOLEAN:
      if (substraitLit) {
        val = variant(substraitLit.value().boolean());
      }
      break;
    case TypeKind::VARCHAR:
      if (substraitLit) {
        val = variant(substraitLit.value().string());
      }
      break;
    case TypeKind::HUGEINT:
      if (substraitLit) {
        if (inputType->isLongDecimal()) {
          auto decimal = substraitLit.value().decimal().value();
          int128_t decimalValue;
          memcpy(&decimalValue, decimal.c_str(), 16);
          val = variant(decimalValue);
        } else {
          VELOX_NYI("TypeKind::HUGEINT only support inputType LongDecimal");
        }
      }
      break;
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      // Doing nothing here can let filter IsNotNull still work.
      break;
    default:
      VELOX_NYI("Subfield filters creation not supported for input type '{}' in setFilterInfo", inputType);
  }

  setColumnFilterInfo(functionName, val, columnToFilterInfo[colIdxVal], reverse);
}

template <TypeKind KIND, typename FilterType>
void SubstraitToVeloxPlanConverter::createNotEqualFilter(
    variant notVariant,
    bool nullAllowed,
    std::vector<std::unique_ptr<FilterType>>& colFilters) {
  using NativeType = typename RangeTraits<KIND>::NativeType;
  using RangeType = typename RangeTraits<KIND>::RangeType;
  // Value > lower
  std::unique_ptr<FilterType> lowerFilter;
  if constexpr (std::is_same_v<RangeType, common::BigintRange>) {
    if (notVariant.value<NativeType>() < getMax<NativeType>()) {
      lowerFilter = std::make_unique<common::BigintRange>(
          notVariant.value<NativeType>() + 1 /*lower*/, getMax<NativeType>() /*upper*/, nullAllowed);
    }
  } else {
    lowerFilter = std::make_unique<RangeType>(
        notVariant.value<NativeType>() /*lower*/,
        false /*lowerUnbounded*/,
        true /*lowerExclusive*/,
        getMax<NativeType>() /*upper*/,
        true /*upperUnbounded*/,
        false /*upperExclusive*/,
        nullAllowed);
  }

  // Value < upper
  std::unique_ptr<FilterType> upperFilter;
  if constexpr (std::is_same_v<RangeType, common::BigintRange>) {
    if (getLowest<NativeType>() < notVariant.value<NativeType>()) {
      upperFilter = std::make_unique<common::BigintRange>(
          getLowest<NativeType>() /*lower*/, notVariant.value<NativeType>() - 1 /*upper*/, nullAllowed);
    }
  } else {
    upperFilter = std::make_unique<RangeType>(
        getLowest<NativeType>() /*lower*/,
        true /*lowerUnbounded*/,
        false /*lowerExclusive*/,
        notVariant.value<NativeType>() /*upper*/,
        false /*upperUnbounded*/,
        true /*upperExclusive*/,
        nullAllowed);
  }

  // To avoid overlap of BigintMultiRange, keep this appending order to make sure lower bound of one range is less than
  // the upper bounds of others.
  if (upperFilter) {
    colFilters.emplace_back(std::move(upperFilter));
  }
  if (lowerFilter) {
    colFilters.emplace_back(std::move(lowerFilter));
  }
}

template <TypeKind KIND>
void SubstraitToVeloxPlanConverter::setInFilter(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {}

template <>
void SubstraitToVeloxPlanConverter::setInFilter<TypeKind::BIGINT>(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {
  std::vector<int64_t> values;
  values.reserve(variants.size());
  for (const auto& variant : variants) {
    int64_t value = variant.value<int64_t>();
    values.emplace_back(value);
  }
  filters[common::Subfield(inputName)] = common::createBigintValues(values, nullAllowed);
}

template <>
void SubstraitToVeloxPlanConverter::setInFilter<TypeKind::INTEGER>(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {
  // Use bigint values for int type.
  std::vector<int64_t> values;
  values.reserve(variants.size());
  for (const auto& variant : variants) {
    // Use the matched type to get value from variant.
    int64_t value = variant.value<int32_t>();
    values.emplace_back(value);
  }
  filters[common::Subfield(inputName)] = common::createBigintValues(values, nullAllowed);
}

template <>
void SubstraitToVeloxPlanConverter::setInFilter<TypeKind::SMALLINT>(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {
  // Use bigint values for small int type.
  std::vector<int64_t> values;
  values.reserve(variants.size());
  for (const auto& variant : variants) {
    // Use the matched type to get value from variant.
    int64_t value = variant.value<int16_t>();
    values.emplace_back(value);
  }
  filters[common::Subfield(inputName)] = common::createBigintValues(values, nullAllowed);
}

template <>
void SubstraitToVeloxPlanConverter::setInFilter<TypeKind::TINYINT>(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {
  // Use bigint values for tiny int type.
  std::vector<int64_t> values;
  values.reserve(variants.size());
  for (const auto& variant : variants) {
    // Use the matched type to get value from variant.
    int64_t value = variant.value<int8_t>();
    values.emplace_back(value);
  }
  filters[common::Subfield(inputName)] = common::createBigintValues(values, nullAllowed);
}

template <>
void SubstraitToVeloxPlanConverter::setInFilter<TypeKind::VARCHAR>(
    const std::vector<variant>& variants,
    bool nullAllowed,
    const std::string& inputName,
    connector::hive::SubfieldFilters& filters) {
  std::vector<std::string> values;
  values.reserve(variants.size());
  for (const auto& variant : variants) {
    std::string value = variant.value<std::string>();
    values.emplace_back(value);
  }
  filters[common::Subfield(inputName)] = std::make_unique<common::BytesValues>(values, nullAllowed);
}

template <TypeKind KIND, typename FilterType>
void SubstraitToVeloxPlanConverter::setSubfieldFilter(
    std::vector<std::unique_ptr<FilterType>> colFilters,
    const std::string& inputName,
    bool nullAllowed,
    connector::hive::SubfieldFilters& filters) {
  using MultiRangeType = typename RangeTraits<KIND>::MultiRangeType;

  if (colFilters.size() == 1) {
    filters[common::Subfield(inputName)] = std::move(colFilters[0]);
  } else if (colFilters.size() > 1) {
    // BigintMultiRange should have been sorted
    if (colFilters[0]->kind() == common::FilterKind::kBigintRange) {
      std::sort(colFilters.begin(), colFilters.end(), [](const auto& a, const auto& b) {
        return dynamic_cast<common::BigintRange*>(a.get())->lower() <
            dynamic_cast<common::BigintRange*>(b.get())->lower();
      });
    }
    if constexpr (std::is_same_v<MultiRangeType, common::MultiRange>) {
      filters[common::Subfield(inputName)] =
          std::make_unique<common::MultiRange>(std::move(colFilters), nullAllowed, true /*nanAllowed*/);
    } else {
      filters[common::Subfield(inputName)] = std::make_unique<MultiRangeType>(std::move(colFilters), nullAllowed);
    }
  }
}

template <TypeKind KIND, typename FilterType>
void SubstraitToVeloxPlanConverter::constructSubfieldFilters(
    uint32_t colIdx,
    const std::string& inputName,
    const TypePtr& inputType,
    const FilterInfo& filterInfo,
    connector::hive::SubfieldFilters& filters) {
  if (!filterInfo.isInitialized()) {
    return;
  }

  bool nullAllowed = filterInfo.nullAllowed_;
  bool isNull = filterInfo.isNull_;
  uint32_t rangeSize = std::max(filterInfo.lowerBounds_.size(), filterInfo.upperBounds_.size());

  if constexpr (KIND == facebook::velox::TypeKind::HUGEINT) {
    // TODO: open it when the Velox's modification is ready.
    VELOX_NYI("constructSubfieldFilters not support for HUGEINT type");
  } else if constexpr (KIND == facebook::velox::TypeKind::BOOLEAN) {
    // Handle bool type filters.
    // Not equal.
    if (filterInfo.notValue_) {
      filters[common::Subfield(inputName)] =
          std::move(std::make_unique<common::BoolValue>(!filterInfo.notValue_.value().value<bool>(), nullAllowed));
    } else if (rangeSize == 0) {
      // IsNull/IsNotNull.
      if (!nullAllowed) {
        filters[common::Subfield(inputName)] = std::move(std::make_unique<common::IsNotNull>());
      } else if (isNull) {
        filters[common::Subfield(inputName)] = std::move(std::make_unique<common::IsNull>());
      } else {
        VELOX_NYI("Only IsNotNull and IsNull are supported in constructSubfieldFilters when no other filter ranges.");
      }
      return;
    } else {
      // Equal.
      auto value = filterInfo.lowerBounds_[0].value().value<bool>();
      VELOX_CHECK(value == filterInfo.upperBounds_[0].value().value<bool>(), "invalid state of bool equal");
      filters[common::Subfield(inputName)] = std::move(std::make_unique<common::BoolValue>(value, nullAllowed));
    }
  } else if constexpr (KIND == facebook::velox::TypeKind::ARRAY || KIND == facebook::velox::TypeKind::MAP) {
    // Only IsNotNull and IsNull are supported for array and map types.
    if (rangeSize == 0) {
      if (!nullAllowed) {
        filters[common::Subfield(inputName)] = std::move(std::make_unique<common::IsNotNull>());
      } else if (isNull) {
        filters[common::Subfield(inputName)] = std::move(std::make_unique<common::IsNull>());
      } else {
        VELOX_NYI(
            "Only IsNotNull and IsNull are supported in constructSubfieldFilters for input type '{}'.", inputType);
      }
    }
  } else {
    using NativeType = typename RangeTraits<KIND>::NativeType;
    using RangeType = typename RangeTraits<KIND>::RangeType;
    using MultiRangeType = typename RangeTraits<KIND>::MultiRangeType;

    // Handle 'in' filter.
    if (filterInfo.values_.size() > 0) {
      // To filter out null is a default behaviour of Spark IN expression.
      nullAllowed = false;
      setInFilter<KIND>(filterInfo.values_, nullAllowed, inputName, filters);
      // Currently, In cannot coexist with other filter conditions
      // due to multirange is in 'OR' relation but 'AND' is needed.
      VELOX_CHECK(rangeSize == 0, "LowerBounds or upperBounds conditons cannot be supported after IN filter.");
      VELOX_CHECK(!filterInfo.notValue_.has_value(), "Not equal cannot be supported after IN filter.");
      return;
    }

    // Construct the Filters.
    std::vector<std::unique_ptr<FilterType>> colFilters;

    // Handle not(equal) filter.
    if (filterInfo.notValue_) {
      variant notVariant = filterInfo.notValue_.value();
      createNotEqualFilter<KIND, FilterType>(notVariant, filterInfo.nullAllowed_, colFilters);
      // Currently, Not-equal cannot coexist with other filter conditions
      // due to multirange is in 'OR' relation but 'AND' is needed.
      VELOX_CHECK(rangeSize == 0, "LowerBounds or upperBounds conditons cannot be supported after not-equal filter.");
      if constexpr (std::is_same_v<MultiRangeType, common::MultiRange>) {
        if (colFilters.size() == 1) {
          filters[common::Subfield(inputName)] = std::move(colFilters.front());
        } else {
          filters[common::Subfield(inputName)] =
              std::make_unique<common::MultiRange>(std::move(colFilters), nullAllowed, true /*nanAllowed*/);
        }
      } else {
        if (colFilters.size() == 1) {
          filters[common::Subfield(inputName)] = std::move(colFilters.front());
        } else {
          filters[common::Subfield(inputName)] = std::make_unique<MultiRangeType>(std::move(colFilters), nullAllowed);
        }
      }
      return;
    }

    // Handle null filtering.
    if (rangeSize == 0) {
      if (!nullAllowed) {
        filters[common::Subfield(inputName)] = std::move(std::make_unique<common::IsNotNull>());
      } else if (isNull) {
        filters[common::Subfield(inputName)] = std::move(std::make_unique<common::IsNull>());
      } else {
        VELOX_NYI("Only IsNotNull and IsNull are supported in constructSubfieldFilters when no other filter ranges.");
      }
      return;
    }

    NativeType lowerBound;
    if constexpr (KIND == facebook::velox::TypeKind::BIGINT) {
      if (inputType->isShortDecimal()) {
        lowerBound = DecimalUtil::kShortDecimalMin;
      } else {
        lowerBound = getLowest<NativeType>();
      }
    } else {
      lowerBound = getLowest<NativeType>();
    }

    NativeType upperBound;
    if constexpr (KIND == facebook::velox::TypeKind::BIGINT) {
      if (inputType->isShortDecimal()) {
        upperBound = DecimalUtil::kShortDecimalMax;
      } else {
        upperBound = getMax<NativeType>();
      }
    } else {
      upperBound = getMax<NativeType>();
    }

    bool lowerUnbounded = true;
    bool upperUnbounded = true;
    bool lowerExclusive = false;
    bool upperExclusive = false;

    // Handle other filter ranges.
    for (uint32_t idx = 0; idx < rangeSize; idx++) {
      if (idx < filterInfo.lowerBounds_.size() && filterInfo.lowerBounds_[idx]) {
        lowerUnbounded = false;
        variant lowerVariant = filterInfo.lowerBounds_[idx].value();
        lowerBound = lowerVariant.value<NativeType>();
        lowerExclusive = filterInfo.lowerExclusives_[idx];
      }

      if (idx < filterInfo.upperBounds_.size() && filterInfo.upperBounds_[idx]) {
        upperUnbounded = false;
        variant upperVariant = filterInfo.upperBounds_[idx].value();
        upperBound = upperVariant.value<NativeType>();
        upperExclusive = filterInfo.upperExclusives_[idx];
      }

      std::unique_ptr<FilterType> filter;
      if constexpr (std::is_same_v<RangeType, common::BigintRange>) {
        filter = std::move(std::make_unique<common::BigintRange>(
            lowerExclusive ? lowerBound + 1 : lowerBound, upperExclusive ? upperBound - 1 : upperBound, nullAllowed));
      } else {
        filter = std::move(std::make_unique<RangeType>(
            lowerBound, lowerUnbounded, lowerExclusive, upperBound, upperUnbounded, upperExclusive, nullAllowed));
      }

      colFilters.emplace_back(std::move(filter));
    }

    // Set the SubfieldFilter.
    setSubfieldFilter<KIND, FilterType>(std::move(colFilters), inputName, filterInfo.nullAllowed_, filters);
  }
}

bool SubstraitToVeloxPlanConverter::checkTypeExtension(const ::substrait::Plan& substraitPlan) {
  for (const auto& sExtension : substraitPlan.extensions()) {
    if (!sExtension.has_extension_type()) {
      continue;
    }

    // Only support UNKNOWN type in UserDefined type extension.
    if (sExtension.extension_type().name() != "UNKNOWN") {
      return false;
    }
  }
  return true;
}

connector::hive::SubfieldFilters SubstraitToVeloxPlanConverter::mapToFilters(
    const std::vector<std::string>& inputNameList,
    const std::vector<TypePtr>& inputTypeList,
    std::vector<FilterInfo>& columnToFilterInfo) {
  // Construct the subfield filters based on the filter info map.
  connector::hive::SubfieldFilters filters;
  for (uint32_t colIdx = 0; colIdx < inputNameList.size(); colIdx++) {
    if (columnToFilterInfo[colIdx].isInitialized()) {
      auto inputType = inputTypeList[colIdx];
      if (inputType->isDate()) {
        constructSubfieldFilters<TypeKind::INTEGER, common::BigintRange>(
            colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
        continue;
      }
      switch (inputType->kind()) {
        case TypeKind::TINYINT:
          constructSubfieldFilters<TypeKind::TINYINT, common::BigintRange>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        case TypeKind::SMALLINT:
          constructSubfieldFilters<TypeKind::SMALLINT, common::BigintRange>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        case TypeKind::INTEGER:
          constructSubfieldFilters<TypeKind::INTEGER, common::BigintRange>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        case TypeKind::BIGINT:
          constructSubfieldFilters<TypeKind::BIGINT, common::BigintRange>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        case TypeKind::REAL:
          constructSubfieldFilters<TypeKind::REAL, common::Filter>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        case TypeKind::DOUBLE:
          constructSubfieldFilters<TypeKind::DOUBLE, common::Filter>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        case TypeKind::BOOLEAN:
          constructSubfieldFilters<TypeKind::BOOLEAN, common::BoolValue>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        case TypeKind::VARCHAR:
          constructSubfieldFilters<TypeKind::VARCHAR, common::Filter>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        case TypeKind::HUGEINT:
          constructSubfieldFilters<TypeKind::HUGEINT, common::HugeintRange>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        case TypeKind::ARRAY:
          constructSubfieldFilters<TypeKind::ARRAY, common::Filter>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        case TypeKind::MAP:
          constructSubfieldFilters<TypeKind::MAP, common::Filter>(
              colIdx, inputNameList[colIdx], inputType, columnToFilterInfo[colIdx], filters);
          break;
        default:
          VELOX_NYI("Subfield filters creation not supported for input type '{}' in mapToFilters", inputType);
      }
    }
  }

  return filters;
}

core::TypedExprPtr SubstraitToVeloxPlanConverter::connectWithAnd(
    std::vector<std::string> inputNameList,
    std::vector<TypePtr> inputTypeList,
    const std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions,
    const std::vector<::substrait::Expression_SingularOrList>& singularOrLists,
    const std::vector<::substrait::Expression_IfThen>& ifThens) {
  if (scalarFunctions.size() == 0 && singularOrLists.size() == 0 && ifThens.size() == 0) {
    return nullptr;
  }
  auto inputType = ROW(std::move(inputNameList), std::move(inputTypeList));

  // Filter for scalar functions.
  std::vector<core::TypedExprPtr> allFilters;
  for (auto scalar : scalarFunctions) {
    auto filter = exprConverter_->toVeloxExpr(scalar, inputType);
    if (filter != nullptr) {
      allFilters.emplace_back(filter);
    }
  }

  for (auto orList : singularOrLists) {
    auto filter = exprConverter_->toVeloxExpr(orList, inputType);
    if (filter != nullptr) {
      allFilters.emplace_back(filter);
    }
  }

  for (auto ifThen : ifThens) {
    auto filter = exprConverter_->toVeloxExpr(ifThen, inputType);
    if (filter != nullptr) {
      allFilters.emplace_back(filter);
    }
  }
  VELOX_CHECK_GT(allFilters.size(), 0, "One filter should be valid.")
  core::TypedExprPtr andFilter = allFilters[0];
  for (auto i = 1; i < allFilters.size(); i++) {
    andFilter = connectWithAnd(andFilter, allFilters[i]);
  }

  return andFilter;
}

core::TypedExprPtr SubstraitToVeloxPlanConverter::connectWithAnd(
    core::TypedExprPtr leftExpr,
    core::TypedExprPtr rightExpr) {
  std::vector<core::TypedExprPtr> params;
  params.reserve(2);
  params.emplace_back(leftExpr);
  params.emplace_back(rightExpr);
  return std::make_shared<const core::CallTypedExpr>(BOOLEAN(), std::move(params), "and");
}

bool SubstraitToVeloxPlanConverter::canPushdownSingularOrList(
    const ::substrait::Expression_SingularOrList& singularOrList,
    bool disableIntLike) {
  VELOX_CHECK(singularOrList.options_size() > 0, "At least one option is expected.");
  // Check whether the value is field.
  bool hasField = singularOrList.value().has_selection();
  const auto& options = singularOrList.options();
  for (const auto& option : options) {
    VELOX_CHECK(option.has_literal(), "Literal is expected as option.");
    auto type = option.literal().literal_type_case();
    // Only BigintValues and BytesValues are supported.
    if (type != ::substrait::Expression_Literal::LiteralTypeCase::kI32 &&
        type != ::substrait::Expression_Literal::LiteralTypeCase::kI64 &&
        type != ::substrait::Expression_Literal::LiteralTypeCase::kString) {
      return false;
    }

    // BigintMultiRange can only accept BigintRange, so disableIntLike is set to
    // true for OR pushdown of int-like types.
    if (disableIntLike &&
        (type == ::substrait::Expression_Literal::LiteralTypeCase::kI32 ||
         type == ::substrait::Expression_Literal::LiteralTypeCase::kI64)) {
      return false;
    }
  }
  return hasField;
}

uint32_t SubstraitToVeloxPlanConverter::getColumnIndexFromSingularOrList(
    const ::substrait::Expression_SingularOrList& singularOrList) {
  // Get the column index.
  ::substrait::Expression_FieldReference selection;
  if (singularOrList.value().has_scalar_function()) {
    selection = singularOrList.value().scalar_function().arguments()[0].value().selection();
  } else if (singularOrList.value().has_selection()) {
    selection = singularOrList.value().selection();
  } else {
    VELOX_FAIL("Unsupported type in IN pushdown.");
  }
  return SubstraitParser::parseReferenceSegment(selection.direct_reference());
}

void SubstraitToVeloxPlanConverter::setFilterInfo(
    const ::substrait::Expression_SingularOrList& singularOrList,
    std::vector<FilterInfo>& columnToFilterInfo) {
  VELOX_CHECK(singularOrList.options_size() > 0, "At least one option is expected.");
  // Get the column index.
  uint32_t colIdx = getColumnIndexFromSingularOrList(singularOrList);

  // Get the value list.
  const auto& options = singularOrList.options();
  std::vector<variant> variants;
  variants.reserve(options.size());
  for (const auto& option : options) {
    VELOX_CHECK(option.has_literal(), "Literal is expected as option.");
    variants.emplace_back(exprConverter_->toVeloxExpr(option.literal())->value());
  }
  // Set the value list to filter info.
  columnToFilterInfo[colIdx].setValues(variants);
}

} // namespace gluten
