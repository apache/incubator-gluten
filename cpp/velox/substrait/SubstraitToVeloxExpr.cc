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

#include "SubstraitToVeloxExpr.h"
#include "TypeUtils.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VariantToVector.h"

#include "velox/type/Timestamp.h"

using namespace facebook::velox;

namespace {

// Get values for the different supported types.
template <typename T>
T getLiteralValue(const ::substrait::Expression::Literal& /* literal */) {
  VELOX_NYI();
}

template <>
int8_t getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return static_cast<int8_t>(literal.i8());
}

template <>
int16_t getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return static_cast<int16_t>(literal.i16());
}

template <>
int32_t getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.i32();
}

template <>
int64_t getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.i64();
}

template <>
double getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.fp64();
}

template <>
float getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.fp32();
}

template <>
bool getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return literal.boolean();
}

template <>
Timestamp getLiteralValue(const ::substrait::Expression::Literal& literal) {
  return Timestamp::fromMicros(literal.timestamp());
}

ArrayVectorPtr makeArrayVector(const VectorPtr& elements) {
  BufferPtr offsets = allocateOffsets(1, elements->pool());
  BufferPtr sizes = allocateOffsets(1, elements->pool());
  sizes->asMutable<vector_size_t>()[0] = elements->size();

  return std::make_shared<ArrayVector>(elements->pool(), ARRAY(elements->type()), nullptr, 1, offsets, sizes, elements);
}

MapVectorPtr makeMapVector(const VectorPtr& keyVector, const VectorPtr& valueVector) {
  BufferPtr offsets = allocateOffsets(1, keyVector->pool());
  BufferPtr sizes = allocateOffsets(1, keyVector->pool());
  sizes->asMutable<vector_size_t>()[0] = keyVector->size();

  return std::make_shared<MapVector>(
      keyVector->pool(),
      MAP(keyVector->type(), valueVector->type()),
      nullptr,
      1,
      offsets,
      sizes,
      keyVector,
      valueVector);
}

RowVectorPtr makeRowVector(const std::vector<VectorPtr>& children) {
  std::vector<std::shared_ptr<const Type>> types;
  types.resize(children.size());
  for (int i = 0; i < children.size(); i++) {
    types[i] = children[i]->type();
  }
  const size_t vectorSize = children.empty() ? 0 : children.front()->size();
  auto rowType = ROW(std::move(types));
  return std::make_shared<RowVector>(children[0]->pool(), rowType, BufferPtr(nullptr), vectorSize, children);
}

ArrayVectorPtr makeEmptyArrayVector(memory::MemoryPool* pool) {
  BufferPtr offsets = allocateOffsets(1, pool);
  BufferPtr sizes = allocateOffsets(1, pool);
  return std::make_shared<ArrayVector>(pool, ARRAY(UNKNOWN()), nullptr, 1, offsets, sizes, nullptr);
}

MapVectorPtr makeEmptyMapVector(memory::MemoryPool* pool) {
  BufferPtr offsets = allocateOffsets(1, pool);
  BufferPtr sizes = allocateOffsets(1, pool);
  return std::make_shared<MapVector>(pool, MAP(UNKNOWN(), UNKNOWN()), nullptr, 1, offsets, sizes, nullptr, nullptr);
}

RowVectorPtr makeEmptyRowVector(memory::MemoryPool* pool) {
  return makeRowVector({});
}

template <typename T>
void setLiteralValue(const ::substrait::Expression::Literal& literal, FlatVector<T>* vector, vector_size_t index) {
  if (literal.has_null()) {
    vector->setNull(index, true);
  } else if constexpr (std::is_same_v<T, StringView>) {
    if (literal.has_string()) {
      vector->set(index, StringView(literal.string()));
    } else if (literal.has_var_char()) {
      vector->set(index, StringView(literal.var_char().value()));
    } else if (literal.has_binary()) {
      vector->set(index, StringView(literal.binary()));
    } else {
      VELOX_FAIL("Unexpected string or binary literal");
    }
  } else if (vector->type()->isDate()) {
    auto dateVector = vector->template asFlatVector<int32_t>();
    dateVector->set(index, int(literal.date()));
  } else {
    vector->set(index, getLiteralValue<T>(literal));
  }
}

template <TypeKind kind>
VectorPtr constructFlatVector(
    std::function<::substrait::Expression::Literal(vector_size_t /* idx */)> elementAt,
    const vector_size_t size,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  VELOX_CHECK(type->isPrimitiveType());
  auto vector = BaseVector::create(type, size, pool);
  using T = typename TypeTraits<kind>::NativeType;
  auto flatVector = vector->as<FlatVector<T>>();

  for (int i = 0; i < size; i++) {
    auto element = elementAt(i);
    setLiteralValue(element, flatVector, i);
  }
  return vector;
}

/// Whether null will be returned on cast failure.
bool isNullOnFailure(::substrait::Expression::Cast::FailureBehavior failureBehavior) {
  switch (failureBehavior) {
    case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_UNSPECIFIED:
    case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_THROW_EXCEPTION:
      return false;
    case ::substrait::Expression_Cast_FailureBehavior_FAILURE_BEHAVIOR_RETURN_NULL:
      return true;
    default:
      VELOX_NYI("The given failure behavior is NOT supported: '{}'", failureBehavior);
  }
}

template <TypeKind kind>
VectorPtr constructFlatVectorForStruct(
    const ::substrait::Expression::Literal& child,
    const vector_size_t size,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  VELOX_CHECK(type->isPrimitiveType());
  auto vector = BaseVector::create(type, size, pool);
  using T = typename TypeTraits<kind>::NativeType;
  auto flatVector = vector->as<FlatVector<T>>();
  setLiteralValue(child, flatVector, 0);
  return vector;
}

core::FieldAccessTypedExprPtr
makeFieldAccessExpr(const std::string& name, const TypePtr& type, core::FieldAccessTypedExprPtr input) {
  if (input) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, input, name);
  }

  return std::make_shared<core::FieldAccessTypedExpr>(type, name);
}

} // namespace

using facebook::velox::core::variantArrayToVector;

namespace gluten {

std::shared_ptr<const core::FieldAccessTypedExpr> SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::FieldReference& substraitField,
    const RowTypePtr& inputType) {
  auto typeCase = substraitField.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::FieldReference::ReferenceTypeCase::kDirectReference: {
      const auto& directRef = substraitField.direct_reference();
      core::FieldAccessTypedExprPtr fieldAccess{nullptr};
      const auto* tmp = &directRef.struct_field();

      auto inputColumnType = inputType;
      for (;;) {
        auto idx = tmp->field();
        fieldAccess = makeFieldAccessExpr(inputColumnType->nameOf(idx), inputColumnType->childAt(idx), fieldAccess);

        if (!tmp->has_child()) {
          break;
        }

        inputColumnType = asRowType(inputColumnType->childAt(idx));
        tmp = &tmp->child().struct_field();
      }
      return fieldAccess;
    }
    default:
      VELOX_NYI("Substrait conversion not supported for Reference '{}'", typeCase);
  }
}

core::TypedExprPtr SubstraitVeloxExprConverter::toExtractExpr(
    const std::vector<core::TypedExprPtr>& params,
    const TypePtr& outputType) {
  VELOX_CHECK_EQ(params.size(), 2);
  auto functionArg = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(params[0]);
  if (functionArg) {
    // Get the function argument.
    auto variant = functionArg->value();
    if (!variant.hasValue()) {
      VELOX_FAIL("Value expected in variant.");
    }
    // The first parameter specifies extracting from which field.
    std::string from = variant.value<std::string>();

    // The second parameter is the function parameter.
    std::vector<core::TypedExprPtr> exprParams;
    exprParams.reserve(1);
    exprParams.emplace_back(params[1]);
    auto iter = extractDatetimeFunctionMap_.find(from);
    if (iter != extractDatetimeFunctionMap_.end()) {
      return std::make_shared<const core::CallTypedExpr>(outputType, std::move(exprParams), iter->second);
    } else {
      VELOX_NYI("Extract from {} not supported.", from);
    }
  }
  VELOX_FAIL("Constant is expected to be the first parameter in extract.");
}

core::TypedExprPtr SubstraitVeloxExprConverter::toLambdaExpr(
    const ::substrait::Expression::ScalarFunction& substraitFunc,
    const RowTypePtr& inputType) {
  // Arguments names and types.
  std::vector<std::string> argumentNames;
  VELOX_CHECK_GT(substraitFunc.arguments().size(), 1, "lambda should have at least 2 args.");
  argumentNames.reserve(substraitFunc.arguments().size() - 1);
  std::vector<TypePtr> argumentTypes;
  argumentTypes.reserve(substraitFunc.arguments().size() - 1);
  for (int i = 1; i < substraitFunc.arguments().size(); i++) {
    auto arg = substraitFunc.arguments(i).value();
    CHECK(arg.has_scalar_function());
    const auto& veloxFunction =
        SubstraitParser::findVeloxFunction(functionMap_, arg.scalar_function().function_reference());
    CHECK_EQ(veloxFunction, "namedlambdavariable");
    argumentNames.emplace_back(arg.scalar_function().arguments(0).value().literal().string());
    argumentTypes.emplace_back(substraitTypeToVeloxType(substraitFunc.output_type()));
  }
  auto rowType = ROW(std::move(argumentNames), std::move(argumentTypes));
  // Arg[0] -> function.
  auto lambda =
      std::make_shared<core::LambdaTypedExpr>(rowType, toVeloxExpr(substraitFunc.arguments(0).value(), inputType));
  return lambda;
}

core::TypedExprPtr SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::ScalarFunction& substraitFunc,
    const RowTypePtr& inputType) {
  std::vector<core::TypedExprPtr> params;
  params.reserve(substraitFunc.arguments().size());
  for (const auto& sArg : substraitFunc.arguments()) {
    params.emplace_back(toVeloxExpr(sArg.value(), inputType));
  }
  const auto& veloxFunction = SubstraitParser::findVeloxFunction(functionMap_, substraitFunc.function_reference());
  std::string typeName = SubstraitParser::parseType(substraitFunc.output_type()).type;

  if (veloxFunction == "lambdafunction") {
    return toLambdaExpr(substraitFunc, inputType);
  } else if (veloxFunction == "namedlambdavariable") {
    return makeFieldAccessExpr(substraitFunc.arguments(0).value().literal().string(), toVeloxType(typeName), nullptr);
  } else if (veloxFunction == "extract") {
    return toExtractExpr(std::move(params), toVeloxType(typeName));
  } else {
    return std::make_shared<const core::CallTypedExpr>(toVeloxType(typeName), std::move(params), veloxFunction);
  }
}

std::shared_ptr<const core::ConstantTypedExpr> SubstraitVeloxExprConverter::literalsToConstantExpr(
    const std::vector<::substrait::Expression::Literal>& literals) {
  std::vector<variant> variants;
  variants.reserve(literals.size());
  VELOX_CHECK_GE(literals.size(), 0, "List should have at least one item.");
  std::optional<TypePtr> literalType = std::nullopt;
  for (const auto& literal : literals) {
    auto veloxVariant = toVeloxExpr(literal);
    if (!literalType.has_value()) {
      literalType = veloxVariant->type();
    }
    variants.emplace_back(veloxVariant->value());
  }
  VELOX_CHECK(literalType.has_value(), "Type expected.");
  auto varArray = variant::array(variants);
  ArrayVectorPtr arrayVector = variantArrayToVector(ARRAY(literalType.value()), varArray.array(), pool_);
  // Wrap the array vector into constant vector.
  auto constantVector = BaseVector::wrapInConstant(1 /*length*/, 0 /*index*/, arrayVector);
  return std::make_shared<const core::ConstantTypedExpr>(constantVector);
}

core::TypedExprPtr SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::SingularOrList& singularOrList,
    const RowTypePtr& inputType) {
  VELOX_CHECK(singularOrList.options_size() > 0, "At least one option is expected.");
  auto options = singularOrList.options();
  std::vector<::substrait::Expression::Literal> literals;
  literals.reserve(options.size());
  for (const auto& option : options) {
    VELOX_CHECK(option.has_literal(), "Literal is expected as option.");
    literals.emplace_back(option.literal());
  }

  std::vector<core::TypedExprPtr> params;
  params.reserve(2);
  // First param is the value, second param is the list.
  params.emplace_back(toVeloxExpr(singularOrList.value(), inputType));
  params.emplace_back(literalsToConstantExpr(literals));
  return std::make_shared<const core::CallTypedExpr>(BOOLEAN(), std::move(params), "in");
}

std::shared_ptr<const core::ConstantTypedExpr> SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::Literal& substraitLit) {
  auto typeCase = substraitLit.literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
      return std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), variant(substraitLit.boolean()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI8:
      // SubstraitLit.i8() will return int32, so we need this type conversion.
      return std::make_shared<core::ConstantTypedExpr>(TINYINT(), variant(static_cast<int8_t>(substraitLit.i8())));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI16:
      // SubstraitLit.i16() will return int32, so we need this type conversion.
      return std::make_shared<core::ConstantTypedExpr>(SMALLINT(), variant(static_cast<int16_t>(substraitLit.i16())));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
      return std::make_shared<core::ConstantTypedExpr>(INTEGER(), variant(substraitLit.i32()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp32:
      return std::make_shared<core::ConstantTypedExpr>(REAL(), variant(substraitLit.fp32()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
      return std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(substraitLit.i64()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
      return std::make_shared<core::ConstantTypedExpr>(DOUBLE(), variant(substraitLit.fp64()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kString:
      return std::make_shared<core::ConstantTypedExpr>(VARCHAR(), variant(substraitLit.string()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kDate:
      return std::make_shared<core::ConstantTypedExpr>(DATE(), variant(int(substraitLit.date())));
    case ::substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
      return std::make_shared<core::ConstantTypedExpr>(
          TIMESTAMP(), variant(Timestamp::fromMicros(substraitLit.timestamp())));
    case ::substrait::Expression_Literal::LiteralTypeCase::kVarChar:
      return std::make_shared<core::ConstantTypedExpr>(VARCHAR(), variant(substraitLit.var_char().value()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kList: {
      auto constantVector = BaseVector::wrapInConstant(1, 0, literalsToArrayVector(substraitLit));
      return std::make_shared<const core::ConstantTypedExpr>(constantVector);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kMap: {
      auto constantVector = BaseVector::wrapInConstant(1, 0, literalsToMapVector(substraitLit));
      return std::make_shared<const core::ConstantTypedExpr>(constantVector);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kBinary:
      return std::make_shared<core::ConstantTypedExpr>(VARBINARY(), variant::binary(substraitLit.binary()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kStruct: {
      auto constantVector = BaseVector::wrapInConstant(1, 0, literalsToRowVector(substraitLit));
      return std::make_shared<const core::ConstantTypedExpr>(constantVector);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
      auto decimal = substraitLit.decimal().value();
      auto precision = substraitLit.decimal().precision();
      auto scale = substraitLit.decimal().scale();
      int128_t decimalValue;
      memcpy(&decimalValue, decimal.c_str(), 16);
      if (precision <= 18) {
        auto type = DECIMAL(precision, scale);
        return std::make_shared<core::ConstantTypedExpr>(type, variant(static_cast<int64_t>(decimalValue)));
      } else {
        auto type = DECIMAL(precision, scale);
        return std::make_shared<core::ConstantTypedExpr>(
            type,
            variant(HugeInt::build(static_cast<uint64_t>(decimalValue >> 64), static_cast<uint64_t>(decimalValue))));
      }
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
      auto veloxType = substraitTypeToVeloxType(substraitLit.null());
      if (veloxType->isShortDecimal()) {
        return std::make_shared<core::ConstantTypedExpr>(veloxType, variant::null(TypeKind::BIGINT));
      } else if (veloxType->isLongDecimal()) {
        return std::make_shared<core::ConstantTypedExpr>(veloxType, variant::null(TypeKind::HUGEINT));
      } else {
        return std::make_shared<core::ConstantTypedExpr>(veloxType, variant::null(veloxType->kind()));
      }
    }
    default:
      VELOX_NYI("Substrait conversion not supported for type case '{}'", typeCase);
  }
}

ArrayVectorPtr SubstraitVeloxExprConverter::literalsToArrayVector(const ::substrait::Expression::Literal& literal) {
  auto childSize = literal.list().values().size();
  if (childSize == 0) {
    return makeEmptyArrayVector(pool_);
  }
  auto childTypeCase = literal.list().values(0).literal_type_case();
  auto elementAtFunc = [&](vector_size_t idx) { return literal.list().values(idx); };
  auto childVector = literalsToVector(childTypeCase, childSize, literal, elementAtFunc);
  return makeArrayVector(childVector);
}

MapVectorPtr SubstraitVeloxExprConverter::literalsToMapVector(const ::substrait::Expression::Literal& literal) {
  auto childSize = literal.map().key_values().size();
  if (childSize == 0) {
    return makeEmptyMapVector(pool_);
  }
  auto keyTypeCase = literal.map().key_values(0).key().literal_type_case();
  auto valueTypeCase = literal.map().key_values(0).value().literal_type_case();
  auto keyAtFunc = [&](vector_size_t idx) { return literal.map().key_values(idx).key(); };
  auto valueAtFunc = [&](vector_size_t idx) { return literal.map().key_values(idx).value(); };
  auto keyVector = literalsToVector(keyTypeCase, childSize, literal, keyAtFunc);
  auto valueVector = literalsToVector(valueTypeCase, childSize, literal, valueAtFunc);
  return makeMapVector(keyVector, valueVector);
}

VectorPtr SubstraitVeloxExprConverter::literalsToVector(
    ::substrait::Expression_Literal::LiteralTypeCase childTypeCase,
    vector_size_t childSize,
    const ::substrait::Expression::Literal& literal,
    std::function<::substrait::Expression::Literal(vector_size_t /* idx */)> elementAtFunc) {
  switch (childTypeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
      return constructFlatVector<TypeKind::BOOLEAN>(elementAtFunc, childSize, BOOLEAN(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kI8:
      return constructFlatVector<TypeKind::TINYINT>(elementAtFunc, childSize, TINYINT(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kI16:
      return constructFlatVector<TypeKind::SMALLINT>(elementAtFunc, childSize, SMALLINT(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
      return constructFlatVector<TypeKind::INTEGER>(elementAtFunc, childSize, INTEGER(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp32:
      return constructFlatVector<TypeKind::REAL>(elementAtFunc, childSize, REAL(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
      return constructFlatVector<TypeKind::BIGINT>(elementAtFunc, childSize, BIGINT(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
      return constructFlatVector<TypeKind::DOUBLE>(elementAtFunc, childSize, DOUBLE(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kString:
    case ::substrait::Expression_Literal::LiteralTypeCase::kVarChar:
      return constructFlatVector<TypeKind::VARCHAR>(elementAtFunc, childSize, VARCHAR(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
      auto veloxType = substraitTypeToVeloxType(literal.null());
      auto kind = veloxType->kind();
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(constructFlatVector, kind, elementAtFunc, childSize, veloxType, pool_);
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kDate:
      return constructFlatVector<TypeKind::INTEGER>(elementAtFunc, childSize, DATE(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kTimestamp:
      return constructFlatVector<TypeKind::TIMESTAMP>(elementAtFunc, childSize, TIMESTAMP(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kIntervalDayToSecond:
      return constructFlatVector<TypeKind::BIGINT>(elementAtFunc, childSize, INTERVAL_DAY_TIME(), pool_);
    case ::substrait::Expression_Literal::LiteralTypeCase::kList: {
      ArrayVectorPtr elements;
      for (int i = 0; i < childSize; i++) {
        auto element = elementAtFunc(i);
        ArrayVectorPtr grandVector = literalsToArrayVector(element);
        if (!elements) {
          elements = grandVector;
        } else {
          elements->append(grandVector.get());
        }
      }
      return elements;
    }
    case ::substrait::Expression_Literal::LiteralTypeCase::kMap: {
      MapVectorPtr mapVector;
      for (int i = 0; i < childSize; i++) {
        auto element = elementAtFunc(i);
        MapVectorPtr grandVector = literalsToMapVector(element);
        if (!mapVector) {
          mapVector = grandVector;
        } else {
          mapVector->append(grandVector.get());
        }
      }
      return mapVector;
    }
    default:
      VELOX_NYI("literals not supported for type case '{}'", childTypeCase);
  }
}

RowVectorPtr SubstraitVeloxExprConverter::literalsToRowVector(const ::substrait::Expression::Literal& structLiteral) {
  auto childSize = structLiteral.struct_().fields().size();
  if (childSize == 0) {
    return makeEmptyRowVector(pool_);
  }
  auto typeCase = structLiteral.struct_().fields(0).literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kBinary: {
      std::vector<VectorPtr> vectors;
      vectors.reserve(structLiteral.struct_().fields().size());
      for (auto& child : structLiteral.struct_().fields()) {
        vectors.emplace_back(constructFlatVectorForStruct<TypeKind::VARBINARY>(child, 1, VARBINARY(), pool_));
      }
      return makeRowVector(vectors);
    }
    default:
      VELOX_NYI("literalsToRowVector not supported for type case '{}'", typeCase);
  }
}

core::TypedExprPtr SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::Cast& castExpr,
    const RowTypePtr& inputType) {
  auto type = substraitTypeToVeloxType(castExpr.type());
  bool nullOnFailure = isNullOnFailure(castExpr.failure_behavior());

  std::vector<core::TypedExprPtr> inputs{toVeloxExpr(castExpr.input(), inputType)};
  return std::make_shared<core::CastTypedExpr>(type, inputs, nullOnFailure);
}

core::TypedExprPtr SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::IfThen& ifThenExpr,
    const RowTypePtr& inputType) {
  VELOX_CHECK(ifThenExpr.ifs().size() > 0, "If clause expected.");

  // Params are concatenated conditions and results with an optional "else" at
  // the end, e.g. {condition1, result1, condition2, result2,..else}
  std::vector<core::TypedExprPtr> params;
  // If and then expressions are in pairs.
  params.reserve(ifThenExpr.ifs().size() * 2);
  std::optional<TypePtr> outputType = std::nullopt;
  for (const auto& ifThen : ifThenExpr.ifs()) {
    params.emplace_back(toVeloxExpr(ifThen.if_(), inputType));
    const auto& thenExpr = toVeloxExpr(ifThen.then(), inputType);
    // Get output type from the first then expression.
    if (!outputType.has_value()) {
      outputType = thenExpr->type();
    }
    params.emplace_back(thenExpr);
  }

  if (ifThenExpr.has_else_()) {
    params.reserve(1);
    params.emplace_back(toVeloxExpr(ifThenExpr.else_(), inputType));
  }

  VELOX_CHECK(outputType.has_value(), "Output type should be set.");
  if (ifThenExpr.ifs().size() == 1) {
    // If there is only one if-then clause, use if expression.
    return std::make_shared<const core::CallTypedExpr>(outputType.value(), std::move(params), "if");
  }
  return std::make_shared<const core::CallTypedExpr>(outputType.value(), std::move(params), "switch");
}

core::TypedExprPtr SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression& substraitExpr,
    const RowTypePtr& inputType) {
  core::TypedExprPtr veloxExpr;
  auto typeCase = substraitExpr.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kLiteral:
      return toVeloxExpr(substraitExpr.literal());
    case ::substrait::Expression::RexTypeCase::kScalarFunction:
      return toVeloxExpr(substraitExpr.scalar_function(), inputType);
    case ::substrait::Expression::RexTypeCase::kSelection:
      return toVeloxExpr(substraitExpr.selection(), inputType);
    case ::substrait::Expression::RexTypeCase::kCast:
      return toVeloxExpr(substraitExpr.cast(), inputType);
    case ::substrait::Expression::RexTypeCase::kIfThen:
      return toVeloxExpr(substraitExpr.if_then(), inputType);
    case ::substrait::Expression::RexTypeCase::kSingularOrList:
      return toVeloxExpr(substraitExpr.singular_or_list(), inputType);
    default:
      VELOX_NYI("Substrait conversion not supported for Expression '{}'", typeCase);
  }
}

std::unordered_map<std::string, std::string> SubstraitVeloxExprConverter::extractDatetimeFunctionMap_ = {
    {"MILLISECOND", "millisecond"},
    {"SECOND", "second"},
    {"MINUTE", "minute"},
    {"HOUR", "hour"},
    {"DAY", "day"},
    {"DAY_OF_WEEK", "dayofweek"},
    {"DAY_OF_YEAR", "dayofyear"},
    {"MONTH", "month"},
    {"QUARTER", "quarter"},
    {"YEAR", "year"},
    {"YEAR_OF_WEEK", "week_of_year"}};

} // namespace gluten
