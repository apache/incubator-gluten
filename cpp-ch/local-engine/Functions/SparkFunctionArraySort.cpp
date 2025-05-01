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
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <base/sort.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TYPE_MISMATCH;
    extern const int ILLEGAL_COLUMN;
}
}

/// The usage of `arraySort` in CH is different from Spark's `sort_array` function.
/// We need to implement a custom function to sort arrays.
namespace local_engine
{

using namespace DB;
struct LambdaLess
{
    const IColumn & column;
    DataTypePtr type;
    const ColumnFunction & lambda;
    explicit LambdaLess(const IColumn & column_, DataTypePtr type_, const ColumnFunction & lambda_)
        : column(column_), type(type_), lambda(lambda_) {}

    /// May not efficient
    bool operator()(size_t lhs, size_t rhs) const
    {
        /// The column name seems not matter.
        auto left_value_col = ColumnWithTypeAndName(oneRowColumn(lhs), type, "left");
        auto right_value_col = ColumnWithTypeAndName(oneRowColumn(rhs), type, "right");
        auto cloned_lambda = lambda.cloneResized(1);
        auto * lambda_ = typeid_cast<ColumnFunction *>(cloned_lambda.get());
        lambda_->appendArguments({std::move(left_value_col), std::move(right_value_col)});
        auto compare_res_col = lambda_->reduce();
        Field field;
        compare_res_col.column->get(0, field);
        return field.safeGet<Int32>() < 0;
    }
private:
    ALWAYS_INLINE ColumnPtr oneRowColumn(size_t i) const
    {
        auto res = column.cloneEmpty();
        res->insertFrom(column, i);
        return std::move(res);
    }
};

struct GenericLess
{
    const IColumn & column;

    explicit GenericLess(const IColumn & column_) : column(column_) { }

    bool operator()(size_t lhs, size_t rhs) const
    {
        return column.compareAt(lhs, rhs, column, 1) < 0;
    }
};


template <typename ColumnType>
struct Less
{
    const ColumnType & column;

    explicit Less(const IColumn & column_)
        : column(assert_cast<const ColumnType &>(column_))
    {
    }

    bool operator()(size_t lhs, size_t rhs) const { return column.compareAt(lhs, rhs, column, 1) < 0; }
};

template <typename ColumnType>
struct NullableLess
{
    const ColumnType & nested_column;
    const NullMap & null_map;

    explicit NullableLess(const IColumn & nested_column_, const NullMap & null_map_)
        : nested_column(assert_cast<const ColumnType &>(nested_column_))
        , null_map(null_map_)
    {
    }

    bool operator()(size_t lhs, size_t rhs) const
    {
        bool lhs_is_null = null_map[lhs];
        bool rhs_is_null = null_map[rhs];

        if (lhs_is_null) [[unlikely]]
            return false;

        if (rhs_is_null) [[unlikely]]
            return true;

        return nested_column.compareAt(lhs, rhs, nested_column, 1) < 0;
    }
};

class FunctionSparkArraySort : public IFunction
{
public:
    static constexpr auto name = "arraySortSpark";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionSparkArraySort>(); }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForConstants() const { return true; }

    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} requires as arguments a lambda function and an array", getName());

        const auto * lambda_function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!lambda_function_type || lambda_function_type->getArgumentTypes().size() != 2)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a lambda function with 2 arguments, found {} instead.",
                getName(),
                arguments[0]->getName());

        auto array_nesteed_type = checkAndGetDataType<DataTypeArray>(arguments.back().get())->getNestedType();
        DataTypes lambda_args = {array_nesteed_type, array_nesteed_type};
        arguments[0] = std::make_shared<DataTypeFunction>(lambda_args);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 1 or 2 arguments", getName());

        const auto * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arguments.back().type).get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Last argument for function {} must be an array", getName());

        if (arguments.size() > 1)
        {
            const auto * lambda_function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());
            if (!lambda_function_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function", getName());
        }

        return arguments.back().type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto array_column = arguments.back().column;
        auto array_type = arguments.back().type;
        ColumnPtr nullmap_column = nullptr;
        if (const auto * nullable_array_column = checkAndGetColumn<ColumnNullable>(array_column.get()))
        {
            array_column = nullable_array_column->getNestedColumnPtr();
            array_type = assert_cast<const DataTypeNullable *>(array_type.get())->getNestedType();
            nullmap_column = nullable_array_column->getNullMapColumnPtr();
        }

        auto array_nested_type = assert_cast<const DataTypeArray *>(array_type.get())->getNestedType();
        const auto * concrete_array_column = checkAndGetColumn<ColumnArray>(array_column.get());
        if (!concrete_array_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Last argument for function {} must be an array or nullable array column", getName());

        ColumnPtr result = nullptr;
        if (arguments.size() > 1)
            result
                = executeWithLambda(*concrete_array_column, array_nested_type, assert_cast<const ColumnFunction &>(*arguments[0].column));
        else
            result = executeWithoutLambda(*concrete_array_column);

        if (nullmap_column)
            result = ColumnNullable::create(std::move(result), std::move(nullmap_column));

        return result;
    }
private:
    static ColumnPtr executeWithLambda(const ColumnArray & array_column, DataTypePtr array_nested_type, const ColumnFunction & lambda)
    {
        const auto & offsets = array_column.getOffsets();
        auto rows = array_column.size();

        size_t nested_size = array_column.getData().size();
        IColumn::Permutation permutation(nested_size);
        for (size_t i = 0; i < nested_size; ++i)
            permutation[i] = i;

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            auto next_offset = offsets[i];
            ::sort(&permutation[current_offset], &permutation[next_offset], LambdaLess(array_column.getData(), array_nested_type, lambda));
            current_offset = next_offset;
        }
        auto res = ColumnArray::create(array_column.getData().permute(permutation, 0), array_column.getOffsetsPtr());
        return res;
    }

    static ColumnPtr executeWithoutLambda(const ColumnArray & array_column)
    {
        const auto & offsets = array_column.getOffsets();
        auto rows = array_column.size();

        size_t nested_size = array_column.getData().size();
        IColumn::Permutation permutation(nested_size);
        for (size_t i = 0; i < nested_size; ++i)
            permutation[i] = i;

        const auto & data_column = array_column.getData();
        ColumnArray::Offset current_offset = 0;

#define APPLY_COMPARATOR(cmp) \
    for (size_t i = 0; i < rows; ++i) \
    { \
        auto next_offset = offsets[i]; \
        ::sort(&permutation[current_offset], &permutation[next_offset], cmp); \
        current_offset = next_offset; \
    }

#define DISPATCH_FOR_NONNULLABLE_COLUMN(TYPE) \
    else if (checkAndGetColumn<TYPE>(&data_column)) \
    { \
        Less<TYPE> cmp(data_column); \
        APPLY_COMPARATOR(cmp) \
    }

        if (false)
            ;
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnUInt8)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnUInt16)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnUInt32)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnUInt64)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnInt8)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnInt16)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnInt32)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnInt64)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnFloat32)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnFloat64)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnDateTime64)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnDecimal<Decimal32>)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnDecimal<Decimal64>)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnDecimal<Decimal128>)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnDecimal<Decimal256>)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnString)
        DISPATCH_FOR_NONNULLABLE_COLUMN(ColumnFixedString)
#undef DISPATCH_FOR_NONNULLABLE_COLUMN

        else if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&data_column))
        {
            const auto & null_map = nullable->getNullMapData();

#define DISPATCH_FOR_NULLABLE_COLUMN(TYPE) \
    else if (checkAndGetColumn<TYPE>(&nullable->getNestedColumn())) \
    { \
        NullableLess<TYPE> cmp(nullable->getNestedColumn(), null_map); \
        APPLY_COMPARATOR(cmp) \
    }

            if (false)
                ;
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnUInt8)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnUInt16)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnUInt32)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnUInt64)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnInt8)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnInt16)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnInt32)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnInt64)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnFloat32)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnFloat64)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnDateTime64)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnDecimal<Decimal32>)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnDecimal<Decimal64>)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnDecimal<Decimal128>)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnDecimal<Decimal256>)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnString)
            DISPATCH_FOR_NULLABLE_COLUMN(ColumnFixedString)
            else
            {
                GenericLess cmp(data_column);
                APPLY_COMPARATOR(cmp)
            }
#undef DISPATCH_FOR_NULLABLE_COLUMN
        }
        else
        {
            GenericLess cmp(data_column);
            APPLY_COMPARATOR(cmp)
        }
#undef APPLY_COMPARATOR

        return ColumnArray::create(array_column.getData().permute(permutation, 0), array_column.getOffsetsPtr());
    }

    String getName() const override
    {
        return name;
    }
};

REGISTER_FUNCTION(ArraySortSpark)
{
    factory.registerFunction<FunctionSparkArraySort>();
}
}
