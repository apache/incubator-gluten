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
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionMapConcat : public FunctionParser
{
public:
    explicit FunctionMapConcat(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~FunctionMapConcat() override = default;

    static constexpr auto name = "map_concat";

    String getName() const override { return name; }

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        const DB::ActionsDAG::Node * result_node = nullptr;
        if (!parsed_args.size())
            result_node = toFunctionNode(actions_dag, "map", {});
        else
            result_node = toFunctionNode(actions_dag, "mapConcat", parsed_args);

        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};
static FunctionParserRegister<FunctionMapConcat> register_map_concat;

}