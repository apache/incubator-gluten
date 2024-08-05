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

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
class FunctionParserOctetLength : public FunctionParser
{
public:
    explicit FunctionParserOctetLength(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionParserOctetLength() override = default;

    static constexpr auto name = "octet_length";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 1)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one arguments", getName());

        const auto * arg = parsed_args[0];
        const auto * new_arg = arg;
        if (isInt(DB::removeNullable(arg->result_type)))
        {
            const auto * string_type_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), "Nullable(String)");
            new_arg = toFunctionNode(actions_dag, "CAST", {arg, string_type_node});
        }
        const auto * octet_length_node = toFunctionNode(actions_dag, "octet_length", {new_arg});
        return convertNodeTypeIfNeeded(substrait_func, octet_length_node, actions_dag);;
    }
};

static FunctionParserRegister<FunctionParserOctetLength> register_octet_length;
}
