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

#include <Parser/FunctionParser.h>
#include <Common/Exception.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include <Common/CHUtil.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/Types.h>
#include <Parser/TypeParser.h>
#include <Parser/scalar_function_parser/lambdaFunction.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace local_engine
{
class ArrayFilter : public FunctionParser
{
public:
    static constexpr auto name = "filter";
    explicit ArrayFilter(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~ArrayFilter() override = default;

    String getName() const override { return name; }

    String getCHFunctionName(const substrait::Expression_ScalarFunction & scalar_function) const override
    {
        return "arrayFilter";
    }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAGPtr & actions_dag) const
    {
        auto ch_func_name = getCHFunctionName(substrait_func);
        auto parsed_args = parseFunctionArguments(substrait_func, ch_func_name, actions_dag);
        assert(parsed_args.size() == 2);
        if (collectLambdaArguments(*plan_parser, substrait_func.arguments()[1].value().scalar_function()).size() == 1)
            return toFunctionNode(actions_dag, ch_func_name, {parsed_args[1], parsed_args[0]});

        /// filter with index argument.
        const auto * range_end_node = toFunctionNode(actions_dag, "length", {toFunctionNode(actions_dag, "assumeNotNull", {parsed_args[0]})});
        range_end_node = ActionsDAGUtil::convertNodeType(
            actions_dag, range_end_node, "Nullable(Int32)", range_end_node->result_name);
        const auto * index_array_node = toFunctionNode(
            actions_dag,
            "range",
            {addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeInt32>(), 0), range_end_node});
        return toFunctionNode(actions_dag, ch_func_name, {parsed_args[1], parsed_args[0], index_array_node});
    }
};
static FunctionParserRegister<ArrayFilter> register_array_filter;

class ArrayTransform : public FunctionParser
{
public:
    static constexpr auto name = "transform";
    explicit ArrayTransform(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~ArrayTransform() override = default;
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & scalar_function) const override
    {
        return "arrayMap";
    }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAGPtr & actions_dag) const
    {
        auto ch_func_name = getCHFunctionName(substrait_func);
        auto lambda_args = collectLambdaArguments(*plan_parser, substrait_func.arguments()[1].value().scalar_function());
        auto parsed_args = parseFunctionArguments(substrait_func, ch_func_name, actions_dag);
        assert(parsed_args.size() == 2);
        if (lambda_args.size() == 1)
        {
            return toFunctionNode(actions_dag, ch_func_name, {parsed_args[1], parsed_args[0]});
        }

        /// transform with index argument.
        const auto * range_end_node = toFunctionNode(actions_dag, "length", {toFunctionNode(actions_dag, "assumeNotNull", {parsed_args[0]})});
        range_end_node = ActionsDAGUtil::convertNodeType(
            actions_dag, range_end_node, "Nullable(Int32)", range_end_node->result_name);
        const auto * index_array_node = toFunctionNode(
            actions_dag,
            "range",
            {addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeInt32>(), 0), range_end_node});
        return toFunctionNode(actions_dag, ch_func_name, {parsed_args[1], parsed_args[0], index_array_node});
    }
};
static FunctionParserRegister<ArrayTransform> register_array_map;

class ArrayAggregate : public FunctionParser
{
public:
    static constexpr auto name = "aggregate";
    explicit ArrayAggregate(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~ArrayAggregate() override = default;
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & scalar_function) const override
    {
        return "arrayFold";
    }
    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAGPtr & actions_dag) const
    {
        auto ch_func_name = getCHFunctionName(substrait_func);
        auto parsed_args = parseFunctionArguments(substrait_func, ch_func_name, actions_dag);
        assert(parsed_args.size() == 3);
        const auto * function_type = typeid_cast<const DataTypeFunction *>(parsed_args[2]->result_type.get());
        if (!function_type)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "The third argument of aggregate function must be a lambda function");
        if (!parsed_args[1]->result_type->equals(*(function_type->getReturnType())))
        {
            parsed_args[1] = ActionsDAGUtil::convertNodeType(
                actions_dag,
                parsed_args[1],
                function_type->getReturnType()->getName(),
                parsed_args[1]->result_name);
        }

        /// arrayFold cannot accept nullable(array)
        const auto * array_col_node = parsed_args[0];
        if (parsed_args[0]->result_type->isNullable())
        {
            array_col_node = toFunctionNode(actions_dag, "assumeNotNull", {parsed_args[0]});
        }
        const auto * func_node = toFunctionNode(actions_dag, ch_func_name, {parsed_args[2], array_col_node, parsed_args[1]});
        /// For null array, result is null.
        /// TODO: make a new version of arrayFold that can handle nullable array.
        const auto * is_null_node = toFunctionNode(actions_dag, "isNull", {parsed_args[0]});
        const auto * null_node = addColumnToActionsDAG(actions_dag, DB::makeNullable(func_node->result_type), DB::Null());
        return toFunctionNode(actions_dag, "if", {is_null_node, null_node, func_node});
    }
};
static FunctionParserRegister<ArrayAggregate> register_array_aggregate;

}
