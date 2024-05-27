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

#include <velox/exec/SimpleAggregateAdapter.h>
#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/lib/aggregates/AverageAggregateBase.h>

#include "udf/Udaf.h"
#include "udf/examples/UdfCommon.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

static const char* kBoolean = "boolean";
static const char* kInteger = "int";
static const char* kBigInt = "bigint";
static const char* kFloat = "float";
static const char* kDouble = "double";

namespace myavg {
// Copied from velox/exec/tests/SimpleAverageAggregate.cpp

// Implementation of the average aggregation function through the
// SimpleAggregateAdapter.
template <typename T>
class AverageAggregate {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = Row<T>;

  // Type of intermediate result vector wrapped in Row.
  using IntermediateType =
      Row</*sum*/ double,
          /*count*/ int64_t>;

  // Type of output vector.
  using OutputType = std::conditional_t<std::is_same_v<T, float>, float, double>;

  static bool toIntermediate(exec::out_type<Row<double, int64_t>>& out, exec::arg_type<T> in) {
    out.copy_from(std::make_tuple(static_cast<double>(in), 1));
    return true;
  }

  struct AccumulatorType {
    double sum_;
    int64_t count_;

    AccumulatorType() = delete;

    // Constructor used in initializeNewGroups().
    explicit AccumulatorType(HashStringAllocator* /*allocator*/) {
      sum_ = 0;
      count_ = 0;
    }

    // addInput expects one parameter of exec::arg_type<T> for each child-type T
    // wrapped in InputType.
    void addInput(HashStringAllocator* /*allocator*/, exec::arg_type<T> data) {
      sum_ += data;
      count_ = checkedPlus<int64_t>(count_, 1);
    }

    // combine expects one parameter of exec::arg_type<IntermediateType>.
    void combine(HashStringAllocator* /*allocator*/, exec::arg_type<Row<double, int64_t>> other) {
      // Both field of an intermediate result should be non-null because
      // writeIntermediateResult() never make an intermediate result with a
      // single null.
      VELOX_CHECK(other.at<0>().has_value());
      VELOX_CHECK(other.at<1>().has_value());
      sum_ += other.at<0>().value();
      count_ = checkedPlus<int64_t>(count_, other.at<1>().value());
    }

    bool writeFinalResult(exec::out_type<OutputType>& out) {
      out = sum_ / count_;
      return true;
    }

    bool writeIntermediateResult(exec::out_type<IntermediateType>& out) {
      out = std::make_tuple(sum_, count_);
      return true;
    }
  };
};

class MyAvgRegisterer final : public gluten::UdafRegisterer {
  int getNumUdaf() override {
    return 4;
  }

  void populateUdafEntries(int& index, gluten::UdafEntry* udafEntries) override {
    for (const auto& argTypes : {myAvgArg1_, myAvgArg2_, myAvgArg3_, myAvgArg4_}) {
      udafEntries[index++] = {name_.c_str(), kDouble, 1, argTypes, myAvgIntermediateType_};
    }
  }

  void registerSignatures() override {
    registerSimpleAverageAggregate();
  }

 private:
  exec::AggregateRegistrationResult registerSimpleAverageAggregate() {
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

    for (const auto& inputType : {"smallint", "integer", "bigint", "double"}) {
      signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                               .returnType("double")
                               .intermediateType("row(double,bigint)")
                               .argumentType(inputType)
                               .build());
    }

    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("real")
                             .intermediateType("row(double,bigint)")
                             .argumentType("real")
                             .build());

    return exec::registerAggregateFunction(
        name_,
        std::move(signatures),
        [this](
            core::AggregationNode::Step step,
            const std::vector<TypePtr>& argTypes,
            const TypePtr& resultType,
            const core::QueryConfig& /*config*/) -> std::unique_ptr<exec::Aggregate> {
          VELOX_CHECK_LE(argTypes.size(), 1, "{} takes at most one argument", name_);
          auto inputType = argTypes[0];
          if (exec::isRawInput(step)) {
            switch (inputType->kind()) {
              case TypeKind::SMALLINT:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<int16_t>>>(resultType);
              case TypeKind::INTEGER:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<int32_t>>>(resultType);
              case TypeKind::BIGINT:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<int64_t>>>(resultType);
              case TypeKind::REAL:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<float>>>(resultType);
              case TypeKind::DOUBLE:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<double>>>(resultType);
              default:
                VELOX_FAIL("Unknown input type for {} aggregation {}", name_, inputType->kindName());
            }
          } else {
            switch (resultType->kind()) {
              case TypeKind::REAL:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<float>>>(resultType);
              case TypeKind::DOUBLE:
              case TypeKind::ROW:
                return std::make_unique<SimpleAggregateAdapter<AverageAggregate<double>>>(resultType);
              default:
                VELOX_FAIL("Unsupported result type for final aggregation: {}", resultType->kindName());
            }
          }
        },
        true /*registerCompanionFunctions*/,
        true /*overwrite*/);
  }

  const std::string name_ = "myavg";
  const char* myAvgArg1_[1] = {kInteger};
  const char* myAvgArg2_[1] = {kBigInt};
  const char* myAvgArg3_[1] = {kFloat};
  const char* myAvgArg4_[1] = {kDouble};

  const char* myAvgIntermediateType_ = "struct<a:double,b:bigint>";
};
} // namespace myavg

namespace mycountif {

// Copied from velox/functions/prestosql/aggregates/CountIfAggregate.cpp
class CountIfAggregate : public exec::Aggregate {
 public:
  explicit CountIfAggregate() : exec::Aggregate(BIGINT()) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(int64_t);
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result) override {
    extractValues(groups, numGroups, result);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result) override {
    auto* vector = (*result)->as<FlatVector<int64_t>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);

    auto* rawValues = vector->mutableRawValues();
    for (vector_size_t i = 0; i < numGroups; ++i) {
      rawValues[i] = *value<int64_t>(groups[i]);
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    if (decoded.isConstantMapping()) {
      if (decoded.isNullAt(0)) {
        return;
      }
      if (decoded.valueAt<bool>(0)) {
        rows.applyToSelected([&](vector_size_t i) { addToGroup(groups[i], 1); });
      }
    } else if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.isNullAt(i)) {
          return;
        }
        if (decoded.valueAt<bool>(i)) {
          addToGroup(groups[i], 1);
        }
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.valueAt<bool>(i)) {
          addToGroup(groups[i], 1);
        }
      });
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    if (decoded.isConstantMapping()) {
      auto numTrue = decoded.valueAt<int64_t>(0);
      rows.applyToSelected([&](vector_size_t i) { addToGroup(groups[i], numTrue); });
      return;
    }

    rows.applyToSelected([&](vector_size_t i) {
      auto numTrue = decoded.valueAt<int64_t>(i);
      addToGroup(groups[i], numTrue);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    // Constant mapping - check once and add number of selected rows if true.
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        auto isTrue = decoded.valueAt<bool>(0);
        if (isTrue) {
          addToGroup(group, rows.countSelected());
        }
      }
      return;
    }

    int64_t numTrue = 0;
    if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.isNullAt(i)) {
          return;
        }
        if (decoded.valueAt<bool>(i)) {
          ++numTrue;
        }
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.valueAt<bool>(i)) {
          ++numTrue;
        }
      });
    }
    addToGroup(group, numTrue);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    auto arg = args[0]->as<SimpleVector<int64_t>>();

    int64_t numTrue = 0;
    rows.applyToSelected([&](auto row) { numTrue += arg->valueAt(row); });

    addToGroup(group, numTrue);
  }

 protected:
  void initializeNewGroupsInternal(char** groups, folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      *value<int64_t>(groups[i]) = 0;
    }
  }

 private:
  inline void addToGroup(char* group, int64_t numTrue) {
    *value<int64_t>(group) += numTrue;
  }
};

class MyCountIfRegisterer final : public gluten::UdafRegisterer {
  int getNumUdaf() override {
    return 1;
  }

  void populateUdafEntries(int& index, gluten::UdafEntry* udafEntries) override {
    udafEntries[index++] = {name_.c_str(), kBigInt, 1, myCountIfArg_, kBigInt};
  }

  void registerSignatures() override {
    registerCountIfAggregate();
  }

 private:
  void registerCountIfAggregate() {
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
        exec::AggregateFunctionSignatureBuilder()
            .returnType("bigint")
            .intermediateType("bigint")
            .argumentType("boolean")
            .build(),
    };

    exec::registerAggregateFunction(
        name_,
        std::move(signatures),
        [this](
            core::AggregationNode::Step step,
            std::vector<TypePtr> argTypes,
            const TypePtr& /*resultType*/,
            const core::QueryConfig& /*config*/) -> std::unique_ptr<exec::Aggregate> {
          VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes one argument", name_);

          auto isPartial = exec::isRawInput(step);
          if (isPartial) {
            VELOX_CHECK_EQ(argTypes[0]->kind(), TypeKind::BOOLEAN, "{} function only accepts boolean parameter", name_);
          }

          return std::make_unique<CountIfAggregate>();
        },
        {false /*orderSensitive*/},
        true,
        true);
  }

  const std::string name_ = "mycount_if";
  const char* myCountIfArg_[1] = {kBoolean};
};
} // namespace mycountif

std::vector<std::shared_ptr<gluten::UdafRegisterer>>& globalRegisters() {
  static std::vector<std::shared_ptr<gluten::UdafRegisterer>> registerers;
  return registerers;
}

void setupRegisterers() {
  static bool inited = false;
  if (inited) {
    return;
  }
  auto& registerers = globalRegisters();
  registerers.push_back(std::make_shared<myavg::MyAvgRegisterer>());
  registerers.push_back(std::make_shared<mycountif::MyCountIfRegisterer>());
  inited = true;
}
} // namespace

DEFINE_GET_NUM_UDAF {
  setupRegisterers();

  int numUdf = 0;
  for (const auto& registerer : globalRegisters()) {
    numUdf += registerer->getNumUdaf();
  }
  return numUdf;
}

DEFINE_GET_UDAF_ENTRIES {
  setupRegisterers();

  int index = 0;
  for (const auto& registerer : globalRegisters()) {
    registerer->populateUdafEntries(index, udafEntries);
  }
}

DEFINE_REGISTER_UDAF {
  setupRegisterers();

  for (const auto& registerer : globalRegisters()) {
    registerer->registerSignatures();
  }
}
