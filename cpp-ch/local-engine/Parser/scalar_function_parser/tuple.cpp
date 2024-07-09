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
#include <Parser/TypeParser.h>
#include <Common/Exception.h>

namespace local_engine
{
class SparkFunctionNamedStructParser : public FunctionParser
{
public:
    SparkFunctionNamedStructParser(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~SparkFunctionNamedStructParser() override = default;

    static constexpr auto name = "named_struct";
    String getName () const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "tuple"; }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAGPtr & actions_dag) const override
    {
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        const auto & args = substrait_func.arguments();
        auto ch_function_name = getCHFunctionName(substrait_func);
        // Arguments in the format, (<field name>, <value expression>[, <field name>, <value expression> ...])
        // We don't need to care the field names here.
        for (int i = 1; i < args.size(); i += 2)
        {
            parsed_args.emplace_back(parseExpression(actions_dag, args[i].value()));
        }
        const auto * func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};

static FunctionParserRegister<SparkFunctionNamedStructParser> register_named_struct;
}

