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

#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <iostream>
#include "udf/Udf.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

static const char* kInteger = "int";
static const char* kBigInt = "bigint";
static const char* kDate = "date";

class UdfRegisterer {
 public:
  ~UdfRegisterer() = default;

  // Returns the number of UDFs in populateUdfEntries.
  virtual int getNumUdf() = 0;

  // Populate the udfEntries, starting at the given index.
  virtual void populateUdfEntries(int& index, gluten::UdfEntry* udfEntries) = 0;

  // Register all function signatures to velox.
  virtual void registerSignatures() = 0;
};

namespace myudf {

template <TypeKind Kind>
class PlusFiveFunction : public exec::VectorFunction {
 public:
  explicit PlusFiveFunction() {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    using nativeType = typename TypeTraits<Kind>::NativeType;

    BaseVector::ensureWritable(rows, createScalarType<Kind>(), context.pool(), result);

    auto* flatResult = result->asFlatVector<nativeType>();
    auto* rawResult = flatResult->mutableRawValues();

    flatResult->clearNulls(rows);

    rows.applyToSelected([&](auto row) { rawResult[row] = 5; });

    if (args.size() == 0) {
      return;
    }

    for (int i = 0; i < args.size(); i++) {
      auto& arg = args[i];
      VELOX_CHECK(arg->isFlatEncoding() || arg->isConstantEncoding());
      if (arg->isConstantEncoding()) {
        auto value = arg->as<ConstantVector<nativeType>>()->valueAt(0);
        rows.applyToSelected([&](auto row) { rawResult[row] += value; });
      } else {
        auto* rawInput = arg->as<FlatVector<nativeType>>()->rawValues();
        rows.applyToSelected([&](auto row) { rawResult[row] += rawInput[row]; });
      }
    }
  }
};

static std::shared_ptr<facebook::velox::exec::VectorFunction> makePlusConstant(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  if (inputArgs.size() == 0) {
    return std::make_shared<PlusFiveFunction<TypeKind::INTEGER>>();
  }
  auto typeKind = inputArgs[0].type->kind();
  switch (typeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<PlusFiveFunction<TypeKind::INTEGER>>();
    case TypeKind::BIGINT:
      return std::make_shared<PlusFiveFunction<TypeKind::BIGINT>>();
    default:
      VELOX_UNREACHABLE();
  }
}

// name: myudf1
// signatures:
//    bigint -> bigint
// type: VectorFunction
class MyUdf1Registerer final : public UdfRegisterer {
 public:
  int getNumUdf() override {
    return 1;
  }

  void populateUdfEntries(int& index, gluten::UdfEntry* udfEntries) override {
    udfEntries[index++] = {name_.c_str(), kBigInt, 1, bigintArg_};
  }

  void registerSignatures() override {
    facebook::velox::exec::registerVectorFunction(
        name_, bigintSignatures(), std::make_unique<PlusFiveFunction<facebook::velox::TypeKind::BIGINT>>());
  }

 private:
  std::vector<std::shared_ptr<exec::FunctionSignature>> bigintSignatures() {
    return {exec::FunctionSignatureBuilder().returnType("bigint").argumentType("bigint").build()};
  }

  const std::string name_ = "myudf1";
  const char* bigintArg_[1] = {kBigInt};
};

// name: myudf2
// signatures:
//    integer -> integer
//    bigint -> bigint
// type: StatefulVectorFunction
class MyUdf2Registerer final : public UdfRegisterer {
 public:
  int getNumUdf() override {
    return 2;
  }

  void populateUdfEntries(int& index, gluten::UdfEntry* udfEntries) override {
    udfEntries[index++] = {name_.c_str(), kInteger, 1, integerArg_};
    udfEntries[index++] = {name_.c_str(), kBigInt, 1, bigintArg_};
  }

  void registerSignatures() override {
    facebook::velox::exec::registerStatefulVectorFunction(name_, integerAndBigintSignatures(), makePlusConstant);
  }

 private:
  std::vector<std::shared_ptr<exec::FunctionSignature>> integerAndBigintSignatures() {
    return {
        exec::FunctionSignatureBuilder().returnType("integer").argumentType("integer").build(),
        exec::FunctionSignatureBuilder().returnType("bigint").argumentType("bigint").build()};
  }

  const std::string name_ = "myudf2";
  const char* integerArg_[1] = {kInteger};
  const char* bigintArg_[1] = {kBigInt};
};

// name: myudf3
// signatures:
//    [integer,] ... -> integer
//    bigint, [bigint,] ... -> bigint
// type: StatefulVectorFunction with variable arity
class MyUdf3Registerer final : public UdfRegisterer {
 public:
  int getNumUdf() override {
    return 2;
  }

  void populateUdfEntries(int& index, gluten::UdfEntry* udfEntries) override {
    udfEntries[index++] = {name_.c_str(), kInteger, 1, integerArg_, true};
    udfEntries[index++] = {name_.c_str(), kBigInt, 2, bigintArgs_, true};
  }

  void registerSignatures() override {
    facebook::velox::exec::registerStatefulVectorFunction(
        name_, integerAndBigintSignaturesWithVariableArity(), makePlusConstant);
  }

 private:
  std::vector<std::shared_ptr<exec::FunctionSignature>> integerAndBigintSignaturesWithVariableArity() {
    return {
        exec::FunctionSignatureBuilder().returnType("integer").argumentType("integer").variableArity().build(),
        exec::FunctionSignatureBuilder()
            .returnType("bigint")
            .argumentType("bigint")
            .argumentType("bigint")
            .variableArity()
            .build()};
  }

  const std::string name_ = "myudf3";
  const char* integerArg_[1] = {kInteger};
  const char* bigintArgs_[2] = {kBigInt, kBigInt};
};
} // namespace myudf

namespace mydate {
template <typename T>
struct MyDateSimpleFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date, const arg_type<int32_t> addition) {
    result = date + addition;
  }
};

// name: mydate
// signatures:
//    date, integer -> bigint
// type: SimpleFunction
class MyDateRegisterer final : public UdfRegisterer {
 public:
  int getNumUdf() override {
    return 1;
  }

  void populateUdfEntries(int& index, gluten::UdfEntry* udfEntries) override {
    udfEntries[index++] = {name_.c_str(), kDate, 2, myDateArg_};
  }

  void registerSignatures() override {
    facebook::velox::registerFunction<mydate::MyDateSimpleFunction, Date, Date, int32_t>({name_});
  }

 private:
  const std::string name_ = "mydate";
  const char* myDateArg_[2] = {kDate, kInteger};
};
} // namespace mydate

std::vector<std::shared_ptr<UdfRegisterer>>& globalRegisters() {
  static std::vector<std::shared_ptr<UdfRegisterer>> registerers;
  return registerers;
}

void setupRegisterers() {
  static bool inited = false;
  if (inited) {
    return;
  }
  auto& registerers = globalRegisters();
  registerers.push_back(std::make_shared<myudf::MyUdf1Registerer>());
  registerers.push_back(std::make_shared<myudf::MyUdf2Registerer>());
  registerers.push_back(std::make_shared<myudf::MyUdf3Registerer>());
  registerers.push_back(std::make_shared<mydate::MyDateRegisterer>());
  inited = true;
}

DEFINE_GET_NUM_UDF {
  setupRegisterers();

  int numUdf = 0;
  for (const auto& registerer : globalRegisters()) {
    numUdf += registerer->getNumUdf();
  }
  return numUdf;
}

DEFINE_GET_UDF_ENTRIES {
  setupRegisterers();

  int index = 0;
  for (const auto& registerer : globalRegisters()) {
    registerer->populateUdfEntries(index, udfEntries);
  }
}

DEFINE_REGISTER_UDF {
  setupRegisterers();

  for (const auto& registerer : globalRegisters()) {
    registerer->registerSignatures();
  }
}
