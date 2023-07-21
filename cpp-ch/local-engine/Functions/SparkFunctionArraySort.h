#pragma once

#include <Functions/array/FunctionArrayMapped.h>
#include <base/sort.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}


/** Sort arrays, by values of its elements, or by values of corresponding elements of calculated expression (known as "schwartzsort").
  */
template <bool positive>
struct SparkArraySortImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(array_element);
    }

    static void checkArguments(
        const String & name,
        const ColumnWithTypeAndName * fixed_arguments)
    {
        if (!fixed_arguments)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected fixed arguments to get the limit for partial array sort");

        WhichDataType which(fixed_arguments[0].type.get());
        if (!which.isUInt() && !which.isInt())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of limit argument of function {} (must be UInt or Int)",
                fixed_arguments[0].type->getName(),
                name);
    }

    static ColumnPtr execute(
        const ColumnArray & array,
        ColumnPtr mapped,
        const ColumnWithTypeAndName * fixed_arguments [[maybe_unused]] = nullptr);
};

struct NameArraySort
{
    static constexpr auto name = "arraySortSpark";
};
struct NameArrayReverseSort
{
    static constexpr auto name = "arrayReverseSortSpark";
};

using SparkFunctionArraySort = FunctionArrayMapped<SparkArraySortImpl<true>, NameArraySort>;
using SparkFunctionArrayReverseSort = FunctionArrayMapped<SparkArraySortImpl<false>, NameArrayReverseSort>;

}
