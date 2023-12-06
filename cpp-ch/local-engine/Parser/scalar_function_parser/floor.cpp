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

class FunctionParserFloor : public FunctionParser
{
public:
    explicit FunctionParserFloor(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~FunctionParserFloor() override = default;

    static constexpr auto name = "floor";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        /*
            parse floor(x) as
            if (isNaN(x))
                null
            else if (isInfinite(x))
                null
            else if (isNull(x))
                null
            else
                floor(x)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 1 && parsed_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one arguments", getName());

        const auto * first_arg = parsed_args[0];
        const ActionsDAG::Node * floor_node;
        if (parsed_args.size() == 1)
            floor_node = toFunctionNode(actions_dag, "floor", {first_arg});
        else
        {
            const auto * second_arg = parsed_args[1];
            floor_node = toFunctionNode(actions_dag, "floor", {first_arg, second_arg});
        }
        if (!isNativeNumber(removeNullable(first_arg->result_type)))
            return convertNodeTypeIfNeeded(substrait_func, floor_node, actions_dag);
        
        auto nullable_result_type = makeNullable(floor_node->result_type);
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, nullable_result_type, Field());
        const auto * is_nan_node = toFunctionNode(actions_dag, "isNaN", {first_arg});
        const auto * is_inf_node = toFunctionNode(actions_dag, "isInfinite", {first_arg});
        const auto * is_null_node = toFunctionNode(actions_dag, "isNull", {first_arg});
        const auto * result_node = toFunctionNode(actions_dag, "multiIf", {
            is_nan_node,
            null_const_node,
            is_inf_node,
            null_const_node,
            is_null_node,
            null_const_node,
            floor_node
        });
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserFloor> register_floor;
}
