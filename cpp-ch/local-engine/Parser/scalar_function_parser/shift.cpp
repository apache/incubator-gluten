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
#include <DataTypes/IDataType.h>
#include <Common/CHUtil.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionParserShiftBase : public FunctionParser
{
public:
    explicit FunctionParserShiftBase(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionParserShiftBase() override = default;

    virtual String getCHFunctionName() const = 0;

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAG & actions_dag) const override
    {
        /// parse spark shiftxxx(expr, n) as
        /// If expr has long type -> CH bitShiftxxx(expr, pmod(n, 64))
        /// Otherwise             -> CH bitShiftxxx(expr, pmod(n, 32))
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());


        auto input_type = removeNullable(parsed_args[0]->result_type);
        WhichDataType which(input_type);
        const ActionsDAG::Node * base_node = nullptr;
        if (which.isInt64())
        {
            base_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeInt32>(), 64);
        }
        else if (which.isInt32())
        {
            base_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeInt32>(), 32);
        }
        else
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "First argument for function {} must be an long or integer", getName());

        const auto * pmod_node = toFunctionNode(actions_dag, "pmod", {parsed_args[1], base_node});
        auto ch_function_name = getCHFunctionName();
        const auto * shift_node = toFunctionNode(actions_dag, ch_function_name, {parsed_args[0], pmod_node});
        return convertNodeTypeIfNeeded(substrait_func, shift_node, actions_dag);
    }
};

class FunctionParserShiftLeft : public FunctionParserShiftBase
{
public:
    explicit FunctionParserShiftLeft(SerializedPlanParser * plan_parser_) : FunctionParserShiftBase(plan_parser_) { }
    ~FunctionParserShiftLeft() override = default;

    static constexpr auto name = "shiftleft";
    String getName() const override { return name; }

    String getCHFunctionName() const override { return "bitShiftLeft"; }
};
static FunctionParserRegister<FunctionParserShiftLeft> register_shiftleft;

class FunctionParserShiftRight: public FunctionParserShiftBase
{
public:
    explicit FunctionParserShiftRight(SerializedPlanParser * plan_parser_) : FunctionParserShiftBase(plan_parser_) { }
    ~FunctionParserShiftRight() override = default;

    static constexpr auto name = "shiftright";
    String getName() const override { return name; }

    String getCHFunctionName() const override { return "bitShiftRight"; }
};
static FunctionParserRegister<FunctionParserShiftRight> register_shiftright;


}
