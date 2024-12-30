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

namespace local_engine
{
class SparkFunctionAliasParser : public FunctionParser
{
public:
    SparkFunctionAliasParser(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~SparkFunctionAliasParser() override = default;
    static constexpr auto name = "alias";
    String getName() const { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return name; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        const auto & args = substrait_func.arguments();
        for (const auto & arg : args)
        {
            if (arg.value().has_scalar_function())
            {
                String empty_result_name;// no meaning
                parsed_args.emplace_back(parseFunctionWithDAG(arg.value(), empty_result_name, actions_dag, true));
            }
            else
                parsed_args.emplace_back(parseExpression(actions_dag, arg.value()));
        }

        String result_name = parsed_args[0]->result_name;
        actions_dag.addOrReplaceInOutputs(*parsed_args[0]);
        /// Skip alias because alias name is equal to the name of first argument.
        return parsed_args[0];
    }

};
static FunctionParserRegister<SparkFunctionAliasParser> register_alias;
}

