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
#include <Functions/LeastGreatestGeneric.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}
namespace local_engine
{
template <DB::LeastGreatest kind>
class FunctionGreatestestLeast : public DB::FunctionLeastGreatestGeneric<kind>
{
public:
    bool useDefaultImplementationForNulls() const override { return false; }
    virtual String getName() const = 0;

private:
    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & types) const override
    {
        if (types.empty())
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} cannot be called without arguments", getName());
        return makeNullable(getLeastSupertype(types));
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
    {
        size_t num_arguments = arguments.size();
        DB::Columns converted_columns(num_arguments);
        for (size_t arg = 0; arg < num_arguments; ++arg)
            converted_columns[arg] = castColumn(arguments[arg], result_type)->convertToFullColumnIfConst();
        auto result_column = result_type->createColumn();
        result_column->reserve(input_rows_count);
        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t best_arg = 0;
            for (size_t arg = 1; arg < num_arguments; ++arg)
            {
                if constexpr (kind == DB::LeastGreatest::Greatest)
                {
                    auto cmp_result = converted_columns[arg]->compareAt(row_num, row_num, *converted_columns[best_arg], -1);
                    if (cmp_result > 0)
                        best_arg = arg;
                }
                else
                {
                    auto cmp_result = converted_columns[arg]->compareAt(row_num, row_num, *converted_columns[best_arg], 1);
                    if (cmp_result <  0)
                        best_arg = arg;
                }
            }
            result_column->insertFrom(*converted_columns[best_arg], row_num);
        }
        return result_column;
    }
};

}
