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
#include <Common/CHUtil.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionParserLog1p : public FunctionParser
{
public:
    explicit FunctionParserLog1p(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~FunctionParserLog1p() override = default;

    static constexpr auto name = "log1p";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        /*
            parse log1p(x) as
            if (x <= -1.0)
                null
            else
                log1p(x)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one arguments", getName());

        const auto * arg_node = parsed_args[0];
        const auto * log1p_node = toFunctionNode(actions_dag, "log1p", {arg_node});

        auto result_type = log1p_node->result_type;
        auto nullable_result_type = makeNullable(result_type);

        const auto * null_const_node = addColumnToActionsDAG(actions_dag, nullable_result_type, Field());
        const auto * nullable_log1p_node = ActionsDAGUtil::convertNodeType(actions_dag, log1p_node, nullable_result_type->getName(), log1p_node->result_name);

        const auto * le_node = toFunctionNode(actions_dag, "lessOrEquals", {arg_node, addColumnToActionsDAG(actions_dag, result_type, -1.0)});
        const auto * result_node = toFunctionNode(actions_dag, "if", {le_node, null_const_node, nullable_log1p_node});

        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserLog1p> register_log1p;
}
