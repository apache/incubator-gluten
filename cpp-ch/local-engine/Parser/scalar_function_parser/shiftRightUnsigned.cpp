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

#include <DataTypes/DataTypeNullable.h>
#include <Parser/FunctionParser.h>

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
class FunctionParserShiftRightUnsigned : public FunctionParser
{
public:
    explicit FunctionParserShiftRightUnsigned(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionParserShiftRightUnsigned() override = default;

    static constexpr auto name = "shiftrightunsigned";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAGPtr & actions_dag) const override
    {
        /// parse shiftrightunsigned(a, b) as
        /// if (isInteger(a))
        ///   bitShiftRight(a::UInt32, b::UInt32)
        /// else if (isLong(a))
        ///   bitShiftRight(a::UInt64, b::UInt64)
        /// else
        ///   throw Exception

        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * a = parsed_args[0];
        const auto * b = parsed_args[1];
        const auto * new_a = a;
        const auto * new_b = b;

        WhichDataType which(removeNullable(a->result_type));
        if (which.isInt32())
        {
            const auto * uint32_type_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), "Nullable(UInt32)");
            new_a = toFunctionNode(actions_dag, "CAST", {a, uint32_type_node});
            new_b = toFunctionNode(actions_dag, "CAST", {b, uint32_type_node});
        }
        else if (which.isInt64())
        {
            const auto * uint64_type_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), "Nullable(UInt64)");
            new_a = toFunctionNode(actions_dag, "CAST", {a, uint64_type_node});
            new_b = toFunctionNode(actions_dag, "CAST", {b, uint64_type_node});
        }
        else
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Function {} requires integer or long as first argument", getName());

        const auto * result = toFunctionNode(actions_dag, "bitShiftRight", {new_a, new_b});
        return convertNodeTypeIfNeeded(substrait_func, result, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserShiftRightUnsigned> register_shift_right_unsigned;
}
