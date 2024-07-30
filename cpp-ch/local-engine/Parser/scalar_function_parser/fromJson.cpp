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
#include <DataTypes/DataTypeString.h>

namespace local_engine
{
class SparkFunctionFromJsonParser : public FunctionParser
{
public:
    SparkFunctionFromJsonParser(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~SparkFunctionFromJsonParser() override = default;

    static constexpr auto name = "from_json";
    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction & /*func*/) const override
    {
        return "JSONExtract";
    }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAG & actions_dag) const override
    {
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        auto ch_function_name = getCHFunctionName(substrait_func);
        parsed_args.emplace_back(parseExpression(actions_dag, substrait_func.arguments()[0].value()));
        auto data_type = TypeParser::parseType(substrait_func.output_type());
        parsed_args.emplace_back(addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), data_type->getName()));
        const auto * func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};
static FunctionParserRegister<SparkFunctionFromJsonParser> register_from_json;
}

