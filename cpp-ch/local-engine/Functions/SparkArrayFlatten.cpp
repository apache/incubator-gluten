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
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

/// arrayFlatten([[1, 2, 3], [4, 5]]) = [1, 2, 3, 4, 5] - flatten array.
class SparkArrayFlatten : public IFunction
{
public:
    static constexpr auto name = "sparkArrayFlatten";

    static FunctionPtr create(ContextPtr) { return std::make_shared<SparkArrayFlatten>(); }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isArray(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}, expected Array",
                            arguments[0]->getName(), getName());

        DataTypePtr nested_type = arguments[0];
        nested_type = checkAndGetDataType<DataTypeArray>(removeNullable(nested_type).get())->getNestedType();
        return nested_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /** We create an array column with array elements as the most deep elements of nested arrays,
          * and construct offsets by selecting elements of most deep offsets by values of ancestor offsets.
          *
Example 1:

Source column: Array(Array(UInt8)):
Row 1: [[1, 2, 3], [4, 5]], Row 2: [[6], [7, 8]]
data: [1, 2, 3], [4, 5], [6], [7, 8]
offsets: 2, 4
data.data: 1 2 3 4 5 6 7 8
data.offsets: 3 5 6 8

Result column: Array(UInt8):
Row 1: [1, 2, 3, 4, 5], Row 2: [6, 7, 8]
data: 1 2 3 4 5 6 7 8
offsets: 5 8

Result offsets are selected from the most deep (data.offsets) by previous deep (offsets) (and values are decremented by one):
3 5 6 8
  ^   ^

Example 2:

Source column: Array(Array(Array(UInt8))):
Row 1: [[], [[1], [], [2, 3]]], Row 2: [[[4]]]

most deep data: 1 2 3 4

offsets1: 2 3
offsets2: 0 3 4
-           ^ ^ - select by prev offsets
offsets3: 1 1 3 4
-             ^ ^ - select by prev offsets

result offsets: 3, 4
result: Row 1: [1, 2, 3], Row2: [4]
          */

        const ColumnArray * src_col = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

        if (!src_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} in argument of function 'arrayFlatten'",
                arguments[0].column->getName());

        const IColumn::Offsets & src_offsets = src_col->getOffsets();

        ColumnArray::ColumnOffsets::MutablePtr result_offsets_column;
        const IColumn::Offsets * prev_offsets = &src_offsets;
        const IColumn * prev_data = &src_col->getData();
        bool nullable = prev_data->isNullable();
        // when array has null element, return null
        if (nullable)
        {
            const ColumnNullable *  nullable_column = checkAndGetColumn<ColumnNullable>(prev_data);
            prev_data = nullable_column->getNestedColumnPtr().get();
            for (size_t i = 0; i < nullable_column->size(); i++)
            {
                if (nullable_column->isNullAt(i))
                {
                    auto res= nullable_column->cloneEmpty();
                    res->insertManyDefaults(input_rows_count);
                    return res;
                }
            }
        }
        if (isNothing(prev_data->getDataType()))
            return prev_data->cloneResized(input_rows_count);
        // only flatten one dimension
        if (const ColumnArray * next_col = checkAndGetColumn<ColumnArray>(prev_data))
        {
            result_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count);

            IColumn::Offsets & result_offsets = result_offsets_column->getData();

            const IColumn::Offsets * next_offsets = &next_col->getOffsets();

            for (size_t i = 0; i < input_rows_count; ++i)
                result_offsets[i] = (*next_offsets)[(*prev_offsets)[i] - 1];    /// -1 array subscript is Ok, see PaddedPODArray
            prev_data = &next_col->getData();
        }

        auto res = ColumnArray::create(
            prev_data->getPtr(),
            result_offsets_column ? std::move(result_offsets_column) : src_col->getOffsetsPtr());
        if (nullable)
            return  makeNullable(res);
        return res;
    }

private:
    String getName() const override
    {
        return name;
    }
};

REGISTER_FUNCTION(SparkArrayFlatten)
{
    factory.registerFunction<SparkArrayFlatten>();
}

}
