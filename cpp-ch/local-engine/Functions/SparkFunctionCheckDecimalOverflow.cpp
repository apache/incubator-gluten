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
#include "SparkFunctionCheckDecimalOverflow.h"

#include <typeinfo>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int TYPE_MISMATCH;
extern const int NOT_IMPLEMENTED;
}
}


namespace local_engine
{
using namespace DB;

struct CheckDecimalOverflowSpark
{
    static constexpr auto name = "checkDecimalOverflowSpark";
};
struct CheckDecimalOverflowSparkOrNull
{
    static constexpr auto name = "checkDecimalOverflowSparkOrNull";
};

enum class CheckExceptionMode
{
    Throw, /// Throw exception if value cannot be parsed.
    Null /// Return ColumnNullable with NULLs when value cannot be parsed.
};

namespace
{
/// Returns received decimal value if and Decimal value has less digits then it's Precision allow, 0 otherwise.
/// Precision could be set as second argument or omitted. If omitted function uses Decimal precision of the first argument.
template <typename Name, CheckExceptionMode mode>
class FunctionCheckDecimalOverflow : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static constexpr auto exception_mode = mode;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCheckDecimalOverflow>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if ((!isDecimal(arguments[0].type) && !isNativeNumber(arguments[0].type)) || !isInteger(arguments[1].type) || !isInteger(arguments[2].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} {} {} of argument of function {}",
                arguments[0].type->getName(),
                arguments[1].type->getName(),
                arguments[2].type->getName(),
                getName());
        UInt32 precision = extractArgument(arguments[1]);
        UInt32 scale = extractArgument(arguments[2]);
        auto return_type = createDecimal<DataTypeDecimal>(precision, scale);
        if constexpr (exception_mode == CheckExceptionMode::Null)
        {
            if (!arguments[0].type->isNullable())
                return std::make_shared<DataTypeNullable>(return_type);
        }
        return return_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        UInt32 to_precision = extractArgument(arguments[1]);
        UInt32 to_scale = extractArgument(arguments[2]);

        const auto & src_col = arguments[0];
        ColumnPtr dst_col;

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using FromDataType = typename Types::LeftType;
            using ToDataType = typename Types::RightType;

            if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeNumber<FromDataType>)
            {
                using FromFieldType = typename FromDataType::FieldType;
                if (const ColumnVectorOrDecimal<FromFieldType> * col_vec = checkAndGetColumn<ColumnVectorOrDecimal<FromFieldType>>(src_col.column.get()))
                {
                    executeInternal<FromDataType, ToDataType>(*col_vec, dst_col, input_rows_count, to_precision, to_scale);
                    return true;
                }
            }

            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());
        };

        if (to_precision <= DecimalUtils::max_precision<Decimal32>)
            callOnIndexAndDataType<DataTypeDecimal<Decimal32>>(src_col.type->getTypeId(), call);
        else if (to_precision <= DecimalUtils::max_precision<Decimal64>)
            callOnIndexAndDataType<DataTypeDecimal<Decimal64>>(src_col.type->getTypeId(), call);
        else if (to_precision <= DecimalUtils::max_precision<Decimal128>)
            callOnIndexAndDataType<DataTypeDecimal<Decimal128>>(src_col.type->getTypeId(), call);
        else
            callOnIndexAndDataType<DataTypeDecimal<Decimal256>>(src_col.type->getTypeId(), call);

        if (!dst_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Wrong call for {} with {}", getName(), src_col.type->getName());

        return dst_col;
    }

private:
    template <typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimal<ToDataType> && (IsDataTypeDecimal<FromDataType> || IsDataTypeNumber<FromDataType>))
    static void
    executeInternal(const FromDataType::ColumnType & src_col, ColumnPtr & dst_col, size_t rows, UInt32 to_precision, UInt32 to_scale)
    {
        using ToFieldType = typename ToDataType::FieldType;
        using ToNativeType = typename ToFieldType::NativeType;
        using ToColumnType = typename ToDataType::ColumnType;
        using FromFieldType = typename FromDataType::FieldType;

        using MaxFieldType = std::conditional_t<
            is_decimal<FromFieldType>,
            std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)), FromFieldType, ToFieldType>,
            ToFieldType>;
        using MaxNativeType = typename MaxFieldType::NativeType;

        ColumnUInt8::MutablePtr res_nullmap_col;
        ColumnUInt8::Container * res_nullmap_data [[maybe_unused]] = nullptr;

        /// Calculate const parameters for decimal conversion outside the loop to avoid unnecessary calculations.
        ToNativeType whole_part_max = 0;
        Int8 scale_direction = 0;
        MaxNativeType scale_multiplier = 0;
        MaxNativeType pow10_to_precision = 0;
        UInt32 from_scale = 0;
        if constexpr (IsDataTypeDecimal<FromDataType>)
        {
            from_scale = src_col.getScale();
            pow10_to_precision = DecimalUtils::scaleMultiplier<MaxNativeType>(to_precision);
            if (to_scale > from_scale)
            {
                scale_direction = 1;
                scale_multiplier = DecimalUtils::scaleMultiplier<MaxNativeType>(to_scale - from_scale);
            }
            else if (to_scale < from_scale)
            {
                scale_direction = -1;
                scale_multiplier = DecimalUtils::scaleMultiplier<MaxNativeType>(from_scale - to_scale);
            }
            else
                scale_multiplier = 0;
        }
        else
        {
            whole_part_max = DecimalUtils::scaleMultiplier<ToNativeType>(to_precision - to_scale) - 1;
        }

        if constexpr (exception_mode == CheckExceptionMode::Null)
        {
            res_nullmap_col = ColumnUInt8::create(rows, 0);
            res_nullmap_data = &res_nullmap_col->getData();
        }

        auto & src_data = src_col.getData();

        auto res_data_col = ToColumnType::create(rows, to_scale);
        auto & res_data = res_data_col->getData();
        res_data.resize_exact(rows);

        for (size_t i = 0; i < rows; ++i)
        {
            ToFieldType to;
            bool success = false;
            if constexpr (IsDataTypeDecimal<FromDataType>)
            {
                success = convertDecimalToDecimalImpl<FromDataType, ToDataType, MaxNativeType>(
                    src_data[i], scale_direction, scale_multiplier, pow10_to_precision, to);
            }
            else
            {
                success = convertNumberToDecimalImpl<FromDataType, ToDataType>(
                    src_data[i], to_scale, whole_part_max, to);
            }

            if constexpr (exception_mode == CheckExceptionMode::Null)
            {
                res_data[i] = static_cast<ToFieldType>(to);
                (*res_nullmap_data)[i] = !success;
            }
            else
            {
                if (success)
                    res_data[i] = static_cast<ToFieldType>(to);
                else
                    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal value is overflow.");
            }
        }

        if constexpr (exception_mode == CheckExceptionMode::Null)
            dst_col = ColumnNullable::create(std::move(res_data_col), std::move(res_nullmap_col));
        else
            dst_col = std::move(res_data_col);
    }

    template <typename FromDataType, typename ToDataType>
    requires(IsDataTypeNumber<FromDataType> && IsDataTypeDecimal<ToDataType>)
    static bool convertNumberToDecimalImpl(
        const typename FromDataType::FieldType & from,
        UInt32 to_scale,
        typename ToDataType::FieldType::NativeType whole_part_max,
        typename ToDataType::FieldType & to)
    {
        using FromFieldType = typename FromDataType::FieldType;
        using ToNativeType = typename ToDataType::FieldType::NativeType;

        ToNativeType whole_part = static_cast<ToNativeType>(from);
        return whole_part >= -whole_part_max && whole_part <= whole_part_max
            && tryConvertToDecimal<FromDataType, ToDataType>(from, to_scale, to);
    }

    template <typename FromDataType, typename ToDataType, typename MaxNativeType>
    requires(IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
    static bool convertDecimalToDecimalImpl(
        const typename FromDataType::FieldType & from,
        Int8 scale_direction,
        const MaxNativeType & scale_multiplier,
        const MaxNativeType & pow10_to_precision,
        typename ToDataType::FieldType & to)
    {
        using FromFieldType = typename FromDataType::FieldType;
        using ToFieldType = typename ToDataType::FieldType;
        using ToNativeType = typename ToFieldType::NativeType;


        MaxNativeType converted;
        if (scale_direction > 0)
        {
            if (common::mulOverflow(static_cast<MaxNativeType>(from.value), scale_multiplier, converted))
                return false;
        }
        else if (scale_direction == 0)
            converted = from.value;
        else
            converted = from.value / scale_multiplier;

        if (converted <= -pow10_to_precision || converted >= pow10_to_precision)
            return false;

        to = static_cast<ToNativeType>(converted);
        return true;
    }
};

using FunctionCheckDecimalOverflowThrow = FunctionCheckDecimalOverflow<CheckDecimalOverflowSpark, CheckExceptionMode::Throw>;
using FunctionCheckDecimalOverflowOrNull = FunctionCheckDecimalOverflow<CheckDecimalOverflowSparkOrNull, CheckExceptionMode::Null>;
}

REGISTER_FUNCTION(CheckDecimalOverflowSpark)
{
    factory.registerFunction<FunctionCheckDecimalOverflowThrow>(FunctionDocumentation{.description = R"(
Check decimal precision is overflow. If overflow throws exception.
)"});

    factory.registerFunction<FunctionCheckDecimalOverflowOrNull>(FunctionDocumentation{.description = R"(
Check decimal precision is overflow. If overflow return `NULL`.
)"});
}
}
