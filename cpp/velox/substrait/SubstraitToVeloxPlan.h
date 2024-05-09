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

#include "SubstraitToVeloxExpr.h"
#include "TypeUtils.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/Options.h"

namespace gluten {
class ResultIterator;

struct SplitInfo {
  /// Whether the split comes from arrow array stream node.
  bool isStream = false;

  /// The Partition index.
  u_int32_t partitionIndex;

  /// The partition columns associated with partitioned table.
  std::vector<std::unordered_map<std::string, std::string>> partitionColumns;

  /// The metadata columns associated with partitioned table.
  std::vector<std::unordered_map<std::string, std::string>> metadataColumns;

  /// The file paths to be scanned.
  std::vector<std::string> paths;

  /// The file starts in the scan.
  std::vector<u_int64_t> starts;

  /// The lengths to be scanned.
  std::vector<u_int64_t> lengths;

  /// The file format of the files to be scanned.
  dwio::common::FileFormat format;

  /// Make SplitInfo polymorphic
  virtual ~SplitInfo() = default;
};

/// This class is used to convert the Substrait plan into Velox plan.
class SubstraitToVeloxPlanConverter {
 public:
  SubstraitToVeloxPlanConverter(
      memory::MemoryPool* pool,
      const std::unordered_map<std::string, std::string>& confMap = {},
      const std::optional<std::string> writeFilesTempPath = std::nullopt,
      bool validationMode = false)
      : pool_(pool), confMap_(confMap), writeFilesTempPath_(writeFilesTempPath), validationMode_(validationMode) {}

  /// Used to convert Substrait WriteRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::WriteRel& writeRel);

  /// Used to convert Substrait ExpandRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ExpandRel& expandRel);

  /// Used to convert Substrait GenerateRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::GenerateRel& generateRel);

  /// Used to convert Substrait WindowRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::WindowRel& windowRel);

  /// Used to convert Substrait WindowGroupLimitRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::WindowGroupLimitRel& windowGroupLimitRel);

  /// Used to convert Substrait JoinRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::JoinRel& joinRel);

  /// Used to convert Substrait CrossRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::CrossRel& crossRel);

  /// Used to convert Substrait AggregateRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::AggregateRel& aggRel);

  /// Convert Substrait ProjectRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ProjectRel& projectRel);

  /// Convert Substrait FilterRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::FilterRel& filterRel);

  /// Convert Substrait FetchRel into Velox LimitNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::FetchRel& fetchRel);

  /// Convert Substrait TopNRel into Velox TopNNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::TopNRel& topNRel);

  /// Convert Substrait ReadRel into Velox Values Node.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ReadRel& readRel, const RowTypePtr& type);

  /// Convert Substrait SortRel into Velox OrderByNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::SortRel& sortRel);

  /// Convert Substrait ReadRel into Velox PlanNode.
  /// Index: the index of the partition this item belongs to.
  /// Starts: the start positions in byte to read from the items.
  /// Lengths: the lengths in byte to read from the items.
  core::PlanNodePtr toVeloxPlan(const ::substrait::ReadRel& sRead);

  core::PlanNodePtr constructValueStreamNode(const ::substrait::ReadRel& sRead, int32_t streamIdx);

  /// Used to convert Substrait Rel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::Rel& sRel);

  /// Used to convert Substrait RelRoot into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::RelRoot& sRoot);

  /// Used to convert Substrait Plan into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(const ::substrait::Plan& substraitPlan);

  // return the raw ptr of ExprConverter
  SubstraitVeloxExprConverter* getExprConverter() {
    return exprConverter_.get();
  }

  /// Used to construct the function map between the index
  /// and the Substrait function name. Initialize the expression
  /// converter based on the constructed function map.
  void constructFunctionMap(const ::substrait::Plan& substraitPlan);

  /// Will return the function map used by this plan converter.
  const std::unordered_map<uint64_t, std::string>& getFunctionMap() const {
    return functionMap_;
  }

  /// Return the splitInfo map used by this plan converter.
  const std::unordered_map<core::PlanNodeId, std::shared_ptr<SplitInfo>>& splitInfos() const {
    return splitInfoMap_;
  }

  /// Used to insert certain plan node as input. The plan node
  /// id will start from the setted one.
  void insertInputNode(uint64_t inputIdx, const std::shared_ptr<const core::PlanNode>& inputNode, int planNodeId) {
    inputNodesMap_[inputIdx] = inputNode;
    planNodeId_ = planNodeId;
  }

  void setSplitInfos(std::vector<std::shared_ptr<SplitInfo>> splitInfos) {
    splitInfos_ = splitInfos;
  }

  void setValueStreamNodeFactory(
      std::function<core::PlanNodePtr(std::string, memory::MemoryPool*, int32_t, RowTypePtr)> factory) {
    valueStreamNodeFactory_ = std::move(factory);
  }

  /// Used to check if ReadRel specifies an input of stream.
  /// If yes, the index of input stream will be returned.
  /// If not, -1 will be returned.
  int32_t getStreamIndex(const ::substrait::ReadRel& sRel);

  /// Used to find the function specification in the constructed function map.
  std::string findFuncSpec(uint64_t id);

  /// Extract join keys from joinExpression.
  /// joinExpression is a boolean condition that describes whether each record
  /// from the left set “match” the record from the right set. The condition
  /// must only include the following operations: AND, ==, field references.
  /// Field references correspond to the direct output order of the data.
  void extractJoinKeys(
      const ::substrait::Expression& joinExpression,
      std::vector<const ::substrait::Expression::FieldReference*>& leftExprs,
      std::vector<const ::substrait::Expression::FieldReference*>& rightExprs);

  /// Get aggregation step from AggregateRel.
  /// If returned Partial, it means the aggregate generated can leveraging flushing and abandoning like
  /// what streaming pre-aggregation can do in MPP databases.
  core::AggregationNode::Step toAggregationStep(const ::substrait::AggregateRel& sAgg);

  /// Get aggregation function step for AggregateFunction.
  /// The returned step value will be used to decide which Velox aggregate function or companion function
  /// is used for the actual data processing.
  core::AggregationNode::Step toAggregationFunctionStep(const ::substrait::AggregateFunction& sAggFuc);

  /// We use companion functions if the aggregate is not single.
  std::string toAggregationFunctionName(const std::string& baseName, const core::AggregationNode::Step& step);

  /// Helper Function to convert Substrait sortField to Velox sortingKeys and
  /// sortingOrders.
  std::pair<std::vector<core::FieldAccessTypedExprPtr>, std::vector<core::SortOrder>> processSortField(
      const ::google::protobuf::RepeatedPtrField<::substrait::SortField>& sortField,
      const RowTypePtr& inputType);

 private:
  /// Integrate Substrait emit feature. Here a given 'substrait::RelCommon'
  /// is passed and check if emit is defined for this relation. Basically a
  /// ProjectNode is added on top of 'noEmitNode' to represent output order
  /// specified in 'relCommon::emit'. Return 'noEmitNode' as is
  /// if output order is 'kDriect'.
  core::PlanNodePtr processEmit(const ::substrait::RelCommon& relCommon, const core::PlanNodePtr& noEmitNode);

  /// Multiple conditions are connected to a binary tree structure with
  /// the relation key words, including AND, OR, and etc. Currently, only
  /// AND is supported. This function is used to extract all the Substrait
  /// conditions in the binary tree structure into a vector.
  void flattenConditions(
      const ::substrait::Expression& sFilter,
      std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions,
      std::vector<::substrait::Expression_SingularOrList>& singularOrLists,
      std::vector<::substrait::Expression_IfThen>& ifThens);

  /// Check the Substrait type extension only has one unknown extension.
  static bool checkTypeExtension(const ::substrait::Plan& substraitPlan);

  /// Range filter recorder for a field is used to make sure only the conditions
  /// that can coexist for this field being pushed down with a range filter.
  class RangeRecorder {
   public:
    /// Set the existence of values range and returns whether this condition can
    /// coexist with existing conditions for one field. Conditions in OR
    /// relation can coexist with each other.
    bool setInRange(bool forOrRelation = false) {
      if (forOrRelation) {
        return true;
      }
      if (inRange_ || multiRange_ || leftBound_ || rightBound_ || isNull_) {
        return false;
      }
      inRange_ = true;
      return true;
    }

    /// Set the existence of left bound and returns whether it can coexist with
    /// existing conditions for this field.
    bool setLeftBound(bool forOrRelation = false) {
      if (forOrRelation) {
        if (!rightBound_)
          leftBound_ = true;
        return !rightBound_;
      }
      if (leftBound_ || inRange_ || multiRange_ || isNull_) {
        return false;
      }
      leftBound_ = true;
      return true;
    }

    /// Set the existence of right bound and returns whether it can coexist with
    /// existing conditions for this field.
    bool setRightBound(bool forOrRelation = false) {
      if (forOrRelation) {
        if (!leftBound_)
          rightBound_ = true;
        return !leftBound_;
      }
      if (rightBound_ || inRange_ || multiRange_ || isNull_) {
        return false;
      }
      rightBound_ = true;
      return true;
    }

    /// Set the existence of multi-range and returns whether it can coexist with
    /// existing conditions for this field.
    bool setMultiRange() {
      if (inRange_ || multiRange_ || leftBound_ || rightBound_ || isNull_) {
        return false;
      }
      multiRange_ = true;
      return true;
    }

    /// Set the existence of IsNull and returns whether it can coexist with
    /// existing conditions for this field.
    bool setIsNull() {
      if (inRange_ || multiRange_ || leftBound_ || rightBound_) {
        return false;
      }
      isNull_ = true;
      return true;
    }

    /// Set certain existence according to function name and returns whether it
    /// can coexist with existing conditions for this field.
    bool setCertainRangeForFunction(const std::string& functionName, bool reverse = false, bool forOrRelation = false);

   private:
    /// The existence of values range.
    bool inRange_ = false;

    /// The existence of left bound.
    bool leftBound_ = false;

    /// The existence of right bound.
    bool rightBound_ = false;

    /// The existence of multi-range.
    bool multiRange_ = false;

    /// The existence of IsNull.
    bool isNull_ = false;
  };

  /// Filter info for a column used in filter push down.
  class FilterInfo {
   public:
    // Null is not allowed.
    void forbidsNull() {
      nullAllowed_ = false;
      if (!initialized_) {
        initialized_ = true;
      }
      forbidsNullSet_ = true;
      if (isNullSet_) {
        existIsNullAndIsNotNull_ = true;
      }
    }

    // Only null is allowed.
    void setNull() {
      isNull_ = true;
      nullAllowed_ = true;
      if (!initialized_) {
        initialized_ = true;
      }
      isNullSet_ = true;
      if (forbidsNullSet_) {
        existIsNullAndIsNotNull_ = true;
      }
    }

    // Return the initialization status.
    bool isInitialized() const {
      return initialized_;
    }

    // Add a lower bound to the range. Multiple lower bounds are
    // regarded to be in 'or' relation.
    void setLower(const std::optional<variant>& left, bool isExclusive) {
      lowerBounds_.emplace_back(left);
      lowerExclusives_.emplace_back(isExclusive);
      if (!initialized_) {
        initialized_ = true;
      }
    }

    // Add a upper bound to the range. Multiple upper bounds are
    // regarded to be in 'or' relation.
    void setUpper(const std::optional<variant>& right, bool isExclusive) {
      upperBounds_.emplace_back(right);
      upperExclusives_.emplace_back(isExclusive);
      if (!initialized_) {
        initialized_ = true;
      }
    }

    // Set a list of values to be used in the push down of 'in' expression.
    void setValues(const std::vector<variant>& values) {
      for (const auto& value : values) {
        values_.emplace_back(value);
      }
      if (!initialized_) {
        initialized_ = true;
      }
    }

    // Set a value for the not(equal) condition.
    void setNotValue(const std::optional<variant>& notValue) {
      notValue_ = notValue;
      if (!initialized_) {
        initialized_ = true;
      }
    }

    // Whether this filter map is initialized.
    bool initialized_ = false;

    bool nullAllowed_ = false;
    bool isNull_ = false;
    bool forbidsNullSet_ = false;
    bool isNullSet_ = false;
    bool existIsNullAndIsNotNull_ = false;

    // If true, left bound will be exclusive.
    std::vector<bool> lowerExclusives_;

    // If true, right bound will be exclusive.
    std::vector<bool> upperExclusives_;

    // A value should not be equal to.
    std::optional<variant> notValue_ = std::nullopt;

    // The lower bounds in 'or' relation.
    std::vector<std::optional<variant>> lowerBounds_;

    // The upper bounds in 'or' relation.
    std::vector<std::optional<variant>> upperBounds_;

    // The list of values used in 'in' expression.
    std::vector<variant> values_;
  };

  /// Returns unique ID to use for plan node. Produces sequential numbers
  /// starting from zero.
  std::string nextPlanNodeId();

  /// Returns whether the args of a scalar function being field or
  /// field with literal. If yes, extract and set the field index.
  static bool fieldOrWithLiteral(
      const ::google::protobuf::RepeatedPtrField<::substrait::FunctionArgument>& arguments,
      uint32_t& fieldIndex);

  /// Separate the functions to be two parts:
  /// subfield functions to be handled by the subfieldFilters in HiveConnector,
  /// and remaining functions to be handled by the remainingFilter in
  /// HiveConnector.
  void separateFilters(
      std::vector<RangeRecorder>& rangeRecorders,
      const std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions,
      std::vector<::substrait::Expression_ScalarFunction>& subfieldFunctions,
      std::vector<::substrait::Expression_ScalarFunction>& remainingFunctions,
      const std::vector<::substrait::Expression_SingularOrList>& singularOrLists,
      std::vector<::substrait::Expression_SingularOrList>& subfieldrOrLists,
      std::vector<::substrait::Expression_SingularOrList>& remainingrOrLists,
      const std::vector<TypePtr>& veloxTypeList,
      const dwio::common::FileFormat& format);

  /// Returns whether a function can be pushed down.
  static bool canPushdownFunction(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      const std::string& filterName,
      uint32_t& fieldIdx);

  /// Returns whether a NOT function can be pushed down.
  bool canPushdownNot(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      std::vector<RangeRecorder>& rangeRecorders);

  /// Returns whether a OR function can be pushed down.
  bool canPushdownOr(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      std::vector<RangeRecorder>& rangeRecorders);

  /// Returns whether a SingularOrList can be pushed down.
  static bool canPushdownSingularOrList(
      const ::substrait::Expression_SingularOrList& singularOrList,
      bool disableIntLike = false);

  /// Check whether the children functions of this scalar function have the same
  /// column index. Curretly used to check whether the two chilren functions of
  /// 'or' expression are effective on the same column.
  static bool childrenFunctionsOnSameField(const ::substrait::Expression_ScalarFunction& function);

  /// Extract the scalar function, and set the filter info for different types
  /// of columns. If reverse is true, the opposite filter info will be set.
  void setFilterInfo(
      const ::substrait::Expression_ScalarFunction& scalarFunction,
      const std::vector<TypePtr>& inputTypeList,
      std::vector<FilterInfo>& columnToFilterInfo,
      bool reverse = false);

  /// Extract SingularOrList and set it to the filter info map.
  void setFilterInfo(
      const ::substrait::Expression_SingularOrList& singularOrList,
      std::vector<FilterInfo>& columnToFilterInfo);

  /// Extract SingularOrList and returns the field index.
  static uint32_t getColumnIndexFromSingularOrList(const ::substrait::Expression_SingularOrList&);

  /// Set the filter info for a column base on the information
  /// extracted from filter condition.
  static void setColumnFilterInfo(
      const std::string& filterName,
      std::optional<variant> literalVariant,
      FilterInfo& columnToFilterInfo,
      bool reverse);

  /// Create a multirange to specify the filter 'x != notValue' with:
  /// x > notValue or x < notValue.
  template <TypeKind KIND, typename FilterType>
  void createNotEqualFilter(variant notVariant, bool nullAllowed, std::vector<std::unique_ptr<FilterType>>& colFilters);

  /// Create a values range to handle in filter.
  /// variants: the list of values extracted from the in expression.
  /// inputName: the column input name.
  template <TypeKind KIND>
  void setInFilter(
      const std::vector<variant>& variants,
      bool nullAllowed,
      const std::string& inputName,
      connector::hive::SubfieldFilters& filters);

  /// Set the constructed filters into SubfieldFilters.
  /// The FilterType is used to distinguish BigintRange and
  /// Filter (the base class). This is needed because BigintMultiRange
  /// can only accept the unique ptr of BigintRange as parameter.
  template <TypeKind KIND, typename FilterType>
  void setSubfieldFilter(
      std::vector<std::unique_ptr<FilterType>> colFilters,
      const std::string& inputName,
      bool nullAllowed,
      connector::hive::SubfieldFilters& filters);

  /// Create the subfield filter based on the constructed filter info.
  /// inputName: the input name of a column.
  template <TypeKind KIND, typename FilterType>
  void constructSubfieldFilters(
      uint32_t colIdx,
      const std::string& inputName,
      const TypePtr& inputType,
      const FilterInfo& filterInfo,
      connector::hive::SubfieldFilters& filters);

  /// Construct subfield filters according to the pre-set map of filter info.
  connector::hive::SubfieldFilters mapToFilters(
      const std::vector<std::string>& inputNameList,
      const std::vector<TypePtr>& inputTypeList,
      std::vector<FilterInfo>& columnToFilterInfo);

  /// Convert subfield functions into subfieldFilters to
  /// be used in Hive Connector.
  connector::hive::SubfieldFilters createSubfieldFilters(
      const std::vector<std::string>& inputNameList,
      const std::vector<TypePtr>& inputTypeList,
      const std::vector<::substrait::Expression_ScalarFunction>& subfieldFunctions,
      const std::vector<::substrait::Expression_SingularOrList>& singularOrLists);

  /// Connect all remaining functions with 'and' relation
  /// for the use of remaingFilter in Hive Connector.
  core::TypedExprPtr connectWithAnd(
      std::vector<std::string> inputNameList,
      std::vector<TypePtr> inputTypeList,
      const std::vector<::substrait::Expression_ScalarFunction>& remainingFunctions,
      const std::vector<::substrait::Expression_SingularOrList>& singularOrLists,
      const std::vector<::substrait::Expression_IfThen>& ifThens);

  /// Connect the left and right expressions with 'and' relation.
  core::TypedExprPtr connectWithAnd(core::TypedExprPtr leftExpr, core::TypedExprPtr rightExpr);

  /// Used to convert AggregateRel into Velox plan node.
  /// The output of child node will be used as the input of Aggregation.
  std::shared_ptr<const core::PlanNode> toVeloxAgg(
      const ::substrait::AggregateRel& sAgg,
      const std::shared_ptr<const core::PlanNode>& childNode,
      const core::AggregationNode::Step& aggStep);

  /// Helper function to convert the input of Substrait Rel to Velox Node.
  template <typename T>
  core::PlanNodePtr convertSingleInput(T rel) {
    VELOX_CHECK(rel.has_input(), "Child Rel is expected here.");
    return toVeloxPlan(rel.input());
  }

  /// The unique identification for each PlanNode.
  int planNodeId_ = 0;

  /// The map storing the relations between the function id and the function
  /// name. Will be constructed based on the Substrait representation.
  std::unordered_map<uint64_t, std::string> functionMap_;

  /// The map storing the split stats for each PlanNode.
  std::unordered_map<core::PlanNodeId, std::shared_ptr<SplitInfo>> splitInfoMap_;

  std::function<core::PlanNodePtr(std::string, memory::MemoryPool*, int32_t, RowTypePtr)> valueStreamNodeFactory_;

  /// The map storing the pre-built plan nodes which can be accessed through
  /// index. This map is only used when the computation of a Substrait plan
  /// depends on other input nodes.
  std::unordered_map<uint64_t, std::shared_ptr<const core::PlanNode>> inputNodesMap_;

  int32_t splitInfoIdx_{0};
  std::vector<std::shared_ptr<SplitInfo>> splitInfos_;

  /// The Expression converter used to convert Substrait representations into
  /// Velox expressions.
  std::unique_ptr<SubstraitVeloxExprConverter> exprConverter_;

  /// Memory pool.
  memory::MemoryPool* pool_;

  /// A map of custom configs.
  std::unordered_map<std::string, std::string> confMap_;

  /// The temporary path used to write files.
  std::optional<std::string> writeFilesTempPath_;

  /// A flag used to specify validation.
  bool validationMode_ = false;
};

} // namespace gluten
