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
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
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

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (!isDecimal(arguments[0]) || !isInteger(arguments[1]) || !isInteger(arguments[2]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} {} {} of argument of function {}",
                    arguments[0]->getName(),
                    arguments[1]->getName(),
                    arguments[2]->getName(),
                    getName());

            if constexpr (exception_mode == CheckExceptionMode::Null)
            {
                if (!arguments[0]->isNullable())
                    return std::make_shared<DataTypeNullable>(arguments[0]);
            }

            return arguments[0];
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const auto & src_column = arguments[0];
            UInt32 precision = extractArgument(arguments[1]);
            UInt32 scale = extractArgument(arguments[2]);

            ColumnPtr result_column;

            auto call = [&](const auto & types) -> bool
            {
                using Types = std::decay_t<decltype(types)>;
                using Type = typename Types::RightType;
                using ColVecType = ColumnDecimal<Type>;

                if (const ColVecType * col_vec = checkAndGetColumn<ColVecType>(src_column.column.get()))
                {
                    executeInternal<Type>(*col_vec, result_column, input_rows_count, precision, scale);
                    return true;
                }

                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());
            };

            callOnBasicType<void, false, false, true, false>(src_column.type->getTypeId(), call);
            if (!result_column)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Wrong call for {} with {}", getName(), src_column.type->getName());

            return result_column;
        }

    private:
        template <typename T>
        static void executeInternal(
            const ColumnDecimal<T> & col_source, ColumnPtr & result_column, size_t input_rows_count, UInt32 precision, UInt32 scale_to)
        {
            ColumnUInt8::MutablePtr col_null_map_to;
            ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
            auto scale_from = col_source.getScale();

            if constexpr (exception_mode == CheckExceptionMode::Null)
            {
                col_null_map_to = ColumnUInt8::create(input_rows_count, false);
                vec_null_map_to = &col_null_map_to->getData();
            }

            auto & datas = col_source.getData();
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                bool overflow = outOfDigits<T>(datas[i], precision, scale_from, scale_to);
                if (overflow)
                {
                    if constexpr (exception_mode == CheckExceptionMode::Null)
                        (*vec_null_map_to)[i] = overflow;
                    else
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal value is overflow.");
                }
            }

            typename ColumnDecimal<T>::MutablePtr col_to = ColumnDecimal<T>::create(std::move(col_source));
            if constexpr (exception_mode == CheckExceptionMode::Null)
                result_column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                result_column = std::move(col_to);
        }

        template <is_decimal T>
        static bool outOfDigits(T decimal, UInt32 precision_to, UInt32 scale_from, UInt32 scale_to)
        {
            using NativeT = typename T::NativeType;

            NativeT converted_value;
            if (scale_to > scale_from)
            {
                converted_value = DecimalUtils::scaleMultiplier<T>(scale_to - scale_from);
                if (common::mulOverflow(static_cast<NativeT>(decimal.value), converted_value, converted_value))
                {
                    if constexpr (exception_mode == CheckExceptionMode::Null)
                        return false;
                    else
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal value is overflow.");
                }
            }
            else
                converted_value = decimal.value / DecimalUtils::scaleMultiplier<NativeT>(scale_from - scale_to);

            NativeT pow10 = intExp10OfSize<NativeT>(precision_to);
            if (converted_value < 0)
                return converted_value <= -pow10;
            return converted_value >= pow10;
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
