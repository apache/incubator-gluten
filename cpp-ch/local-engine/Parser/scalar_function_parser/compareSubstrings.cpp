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

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
class FunctionParserCompareSubstrings : public FunctionParser
{
public:
    static constexpr auto name = "compare_substrings";
    explicit FunctionParserCompareSubstrings(ParserContextPtr parser_context_)
        : FunctionParser(parser_context_)
    {
    }
    ~FunctionParserCompareSubstrings() override = default;

    String getName() const override { return name; }

    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override { return "compareSubstrings"; }

    DB::ActionsDAG::NodeRawConstPtrs
    parseFunctionArguments(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        DB::ActionsDAG::NodeRawConstPtrs parsed_args = expression_parser->parseFunctionArguments(actions_dag, substrait_func);
        if (parsed_args.size() != 5)
            throw DB::Exception(
                DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly three arguments", getName());

        const auto * datatype_node = expression_parser->addConstColumn(actions_dag, std::make_shared<DB::DataTypeString>(), "UInt32");
        parsed_args[2] = expression_parser->toFunctionNode(actions_dag, "CAST", {parsed_args[2], datatype_node});
        parsed_args[3] = expression_parser->toFunctionNode(actions_dag, "CAST", {parsed_args[3], datatype_node});
        parsed_args[4] = expression_parser->toFunctionNode(actions_dag, "CAST", {parsed_args[4], datatype_node});
        return parsed_args;
    }
};

static FunctionParserRegister<FunctionParserCompareSubstrings> register_compare_substrings_parser;
}
