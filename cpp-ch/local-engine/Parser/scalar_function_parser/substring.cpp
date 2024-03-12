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
class FunctionParserSubstring : public FunctionParser
{
public:
    explicit FunctionParserSubstring(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionParserSubstring() override = default;
    static constexpr auto name = "substring";
    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
    const substrait::Expression_ScalarFunction & substrait_func,
    ActionsDAGPtr & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 2 && parsed_args.size() != 3)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires two or three arguments", getName());
        DB::DataTypePtr start_index_data_type = removeNullable(parsed_args[1]->result_type);
        if (!isInteger(start_index_data_type))
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Function {}'s second arguments must be int type", getName());
         /**
            parse substring(str, start_index, length) as
            if (start_index == 0)
                substring(str, 1, length)
            else
                substring(str, start_index, length)
        */
        auto * const_zero_node = addColumnToActionsDAG(actions_dag, start_index_data_type, Field(0));
        auto * const_one_node = addColumnToActionsDAG(actions_dag, start_index_data_type, Field(1));
        auto * equals_zero_node = toFunctionNode(actions_dag, "equals", {parsed_args[1], const_zero_node});
        auto * if_node = toFunctionNode(actions_dag, "if", {equals_zero_node, const_one_node, parsed_args[1]});
        const DB::ActionsDAG::Node * substring_func_node;
        if (parsed_args.size() == 2)
            substring_func_node = toFunctionNode(actions_dag, "substringUTF8", {parsed_args[0], if_node});
        else
            substring_func_node = toFunctionNode(actions_dag, "substringUTF8", {parsed_args[0], if_node, parsed_args[2]});
        return convertNodeTypeIfNeeded(substrait_func, substring_func_node, actions_dag);
    }
protected:
    String getCHFunctionName(const substrait::Expression_ScalarFunction & /*substrait_func*/) const override
    {
        return "substringUTF8";
    }
};

static FunctionParserRegister<FunctionParserSubstring> register_substring;
}
