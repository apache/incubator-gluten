/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"
#include "velox/type/DecimalUtil.h"

using namespace facebook::velox;

namespace gluten {
namespace {

template <class T>
class MakeDecimalFunction final : public exec::VectorFunction {
 public:
  explicit MakeDecimalFunction(uint8_t precision) : precision_(precision) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 3);
    context.ensureWritable(rows, outputType, resultRef);
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto toType = args[1]->type();
    auto unscaledVec = decodedArgs.at(0);
    auto result = resultRef->asUnchecked<FlatVector<T>>()->mutableRawValues();
    if constexpr (std::is_same_v<T, int64_t>) {
      auto nullOnOverflow = decodedArgs.at(2)->valueAt<bool>(0);
      rows.applyToSelected([&](int row) {
        auto unscaled = unscaledVec->valueAt<int64_t>(row);
        int128_t bound = DecimalUtil::kPowersOfTen[precision_];
        if (unscaled <= -bound || unscaled >= bound) {
          // Requested precision is too low to represent this value.
          if (nullOnOverflow) {
            resultRef->setNull(row, true);
          } else {
            VELOX_USER_FAIL("Unscaled value too large for precision");
          }
        } else {
          result[row] = unscaled;
        }
      });
    } else {
      rows.applyToSelected([&](int row) {
        int128_t unscaled = unscaledVec->valueAt<int64_t>(row);
        result[row] = unscaled;
      });
    }
  }

 private:
  uint8_t precision_;
};

class CheckOverflowFunction final : public exec::VectorFunction {
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 3);
    // This VectorPtr type is different with type in makeCheckOverflow, because
    // we cannot get input type by signature the input vector origins from
    // DecimalArithmetic, it is a computed type by arithmetic operation
    auto fromType = args[0]->type();
    auto toType = args[2]->type();
    context.ensureWritable(rows, toType, resultRef);
    if (toType->isShortDecimal()) {
      if (fromType->isShortDecimal()) {
        applyForVectorType<int64_t, int64_t>(rows, args, outputType, context, resultRef);
      } else {
        applyForVectorType<int128_t, int64_t>(rows, args, outputType, context, resultRef);
      }
    } else {
      if (fromType->isShortDecimal()) {
        applyForVectorType<int64_t, int128_t>(rows, args, outputType, context, resultRef);
      } else {
        applyForVectorType<int128_t, int128_t>(rows, args, outputType, context, resultRef);
      }
    }
  }

 private:
  template <typename TInput, typename TOutput>
  void applyForVectorType(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const {
    auto fromType = args[0]->type();
    auto toType = args[2]->type();
    auto result = resultRef->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto decimalValue = decodedArgs.at(0);
    VELOX_CHECK(decodedArgs.at(1)->isConstantMapping());
    auto nullOnOverflow = decodedArgs.at(1)->valueAt<bool>(0);

    const auto& fromPrecisionScale = getDecimalPrecisionScale(*fromType);
    const auto& toPrecisionScale = getDecimalPrecisionScale(*toType);
    rows.applyToSelected([&](int row) {
      auto rescaledValue = DecimalUtil::rescaleWithRoundUp<TInput, TOutput>(
          decimalValue->valueAt<TInput>(row),
          fromPrecisionScale.first,
          fromPrecisionScale.second,
          toPrecisionScale.first,
          toPrecisionScale.second,
          nullOnOverflow);
      if (rescaledValue.has_value()) {
        result[row] = rescaledValue.value();
      } else {
        resultRef->setNull(row, true);
      }
    });
  }
};

class UnscaledValueFunction final : public exec::VectorFunction {
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 1);
    VELOX_CHECK(args[0]->type()->isShortDecimal(), "ShortDecimal type is required.");
    resultRef = std::move(args[0]);
  }
};
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> makeDecimalSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("r_precision", "min(38, a_precision)")
              .integerVariable("r_scale", "min(38, a_scale)")
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("bigint")
              .constantArgumentType("DECIMAL(a_precision, a_scale)")
              .constantArgumentType("boolean")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> checkOverflowSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("b_precision")
              .integerVariable("b_scale")
              .integerVariable("r_precision", "min(38, b_precision)")
              .integerVariable("r_scale", "min(38, b_scale)")
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("boolean")
              .argumentType("DECIMAL(b_precision, b_scale)")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> unscaledValueSignatures() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .returnType("bigint")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeMakeDecimal(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 3);
  auto type = inputArgs[1].type;
  if (type->isShortDecimal()) {
    return std::make_shared<MakeDecimalFunction<int64_t>>(
        std::dynamic_pointer_cast<const ShortDecimalType>(type)->precision());
  } else {
    return std::make_shared<MakeDecimalFunction<int128_t>>(
        std::dynamic_pointer_cast<const LongDecimalType>(type)->precision());
  }
}

std::shared_ptr<exec::VectorFunction> makeCheckOverflow(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 3);
  static const auto kCheckOverflowFunction = std::make_shared<CheckOverflowFunction>();
  return kCheckOverflowFunction;
}

std::shared_ptr<exec::VectorFunction> makeUnscaledValue(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  VELOX_CHECK_EQ(inputArgs.size(), 1);
  static const auto kUnscaledValueFunction = std::make_shared<UnscaledValueFunction>();
  return kUnscaledValueFunction;
}
} // namespace gluten
