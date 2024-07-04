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

#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <Parser/FunctionParser.h>
#include <Common/CHUtil.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionParserNaNvl : public FunctionParser
{
public:
    explicit FunctionParserNaNvl(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~FunctionParserNaNvl() override = default;

    static constexpr auto name = "nanvl";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        /*
            parse nanvl(e1, e2) as
            if (isNull(e1))
                null
            else if (isNaN(e1))
                e2
            else
                e1
        */
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least two arguments", getName());

        const auto * e1 = parsed_args[0];
        const auto * e2 = parsed_args[1];

        auto result_type = e1->result_type;

        const auto * e1_is_null_node = toFunctionNode(actions_dag, "isNull", {e1});
        const auto * e1_is_nan_node = toFunctionNode(actions_dag, "isNaN", {e1});

        const auto * null_const_node = addColumnToActionsDAG(actions_dag, makeNullable(result_type), Field());
        const auto * result_node = toFunctionNode(actions_dag, "multiIf", {
            e1_is_null_node,
            null_const_node,
            e1_is_nan_node,
            e2,
            e1
        });
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserNaNvl> register_nanvl;
}
