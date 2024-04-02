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
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>

#include <memory>
#include <string>

using namespace DB;

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
class SparkFunctionOverlay : public IFunction
{
public:
    static constexpr auto name = "sparkOverlay";

    static FunctionPtr create(const DB::ContextPtr & context) { return std::make_shared<SparkFunctionOverlay>(context); }

    explicit SparkFunctionOverlay(DB::ContextPtr context_) : context(context_) { }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    /// in spark overlay(input, replace, pos[, len]), gluten will set len = -1 if not provided
    size_t getNumberOfArguments() const override { return 4; };

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }


    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 4)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 4 arguments: overlay(input, replace, pos , len)",
                getName());
        }
        return std::make_shared<DataTypeString>();
    }

    /// see calculate logic at org.apache.spark.sql.catalyst.expressions.Overlay#calculate
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & input_col = arguments[0];
        const ColumnWithTypeAndName & replace_col = arguments[1];
        const ColumnWithTypeAndName & pos_col = arguments[2];
        const ColumnWithTypeAndName & len_col = arguments[3];
        const ColumnConst * replace_col_const = checkAndGetColumn<ColumnConst>(replace_col.column.get());
        const ColumnConst * pos_col_const = checkAndGetColumn<ColumnConst>(pos_col.column.get());

        const DataTypePtr & int64_type = DB::DataTypeFactory::instance().get("Int64");
        const DataTypePtr & uint64_type = DB::DataTypeFactory::instance().get("UInt64");
        const FunctionOverloadResolverPtr & substr_function_resolver = FunctionFactory::instance().get("substringUTF8", context);
        const FunctionOverloadResolverPtr & length_function_resolver = FunctionFactory::instance().get("lengthUTF8", context);
        const FunctionOverloadResolverPtr & minus_function_resolver = FunctionFactory::instance().get("minus", context);
        const FunctionOverloadResolverPtr & plus_function_resolver = FunctionFactory::instance().get("plus", context);

        // first part of the string
        ColumnsWithTypeAndName substr1_arguments(3);
        substr1_arguments[0] = input_col;
        substr1_arguments[1] = {int64_type->createColumnConst(input_rows_count, 1), int64_type, "part1_start"};
        // special case when pos < 1, ignore the first part of the string
        if (pos_col.column->getInt(0) < 1)
        {
            substr1_arguments[2] = {int64_type->createColumnConst(input_rows_count, 0), int64_type, "part1_length"};
        }
        else if (pos_col_const)
        {
            Int64 pos = pos_col_const->getInt(0);
            substr1_arguments[2] = {int64_type->createColumnConst(input_rows_count, pos - 1), int64_type, "part1_length"};
        }
        else
        {
            ColumnsWithTypeAndName minus_args = {pos_col, {int64_type->createColumnConst(input_rows_count, 1), int64_type, "minus_one"}};
            FunctionBasePtr minus = minus_function_resolver->build(minus_args);
            substr1_arguments[2] = {minus->execute(minus_args, int64_type, input_rows_count), int64_type, "part1_length"};
        }
        FunctionBasePtr substr = substr_function_resolver->build(substr1_arguments);
        ColumnPtr part1 = substr->execute(substr1_arguments, result_type, input_rows_count);

        // second part of the string is the replace string
        ColumnPtr part2 = replace_col.column;

        // third part of the string
        // 1. calculate pos + len
        ColumnsWithTypeAndName plus_args(2);
        plus_args[0] = pos_col;
        if (len_col.column->getInt(0) >= 0)
        {
            plus_args[1] = len_col;
        }
        else
        {
            FunctionBasePtr length = length_function_resolver->build({replace_col});
            plus_args[1] = {length->execute({replace_col}, uint64_type, input_rows_count), uint64_type, "pos_length"};
        }
        FunctionBasePtr plus = plus_function_resolver->build(plus_args);
        ColumnPtr pos_plus_len = plus->execute(plus_args, int64_type, input_rows_count);

        // 2. calculate rest part of the input string
        ColumnsWithTypeAndName substr3_arguments(2);
        substr3_arguments[0] = input_col;
        substr3_arguments[1] = {pos_plus_len, int64_type, "part3_begin"};
        FunctionBasePtr substr3 = substr_function_resolver->build(substr3_arguments);
        ColumnPtr part3 = substr3->execute(substr3_arguments, result_type, input_rows_count);

        // concat all parts
        auto res_col = ColumnString::create();
        res_col->reserve(input_rows_count);

        const ColumnString * part1_str = checkAndGetColumn<ColumnString>(part1.get());
        const ColumnString * part3_str = checkAndGetColumn<ColumnString>(part3.get());

        DB::GatherUtils::StringSources sources(3);
        sources[0] = std::make_unique<GatherUtils::DynamicStringSource<GatherUtils::StringSource>>(*part1_str);
        if (replace_col_const)
        {
            const ColumnConst * part2_str = checkAndGetColumnConst<ColumnString>(part2.get());
            sources[1] = std::make_unique<GatherUtils::DynamicStringSource<GatherUtils::ConstSource<GatherUtils::StringSource>>>(*part2_str);
        }
        else
        {
            const ColumnString * part2_str = checkAndGetColumn<ColumnString>(part2.get());
            sources[1] = std::make_unique<GatherUtils::DynamicStringSource<GatherUtils::StringSource>>(*part2_str);
        }
        sources[2] = std::make_unique<GatherUtils::DynamicStringSource<GatherUtils::StringSource>>(*part3_str);
        GatherUtils::concat<GatherUtils::StringSink>(sources, GatherUtils::StringSink(*res_col, input_rows_count));

        return std::move(res_col);
    }

private:
    DB::ContextPtr context;
};

REGISTER_FUNCTION(OverlaySpark)
{
    factory.registerFunction<SparkFunctionOverlay>();
}

}
