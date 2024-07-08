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
class SparkFunctionTrimParser : public FunctionParser
{
public:
    SparkFunctionTrimParser(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~SparkFunctionTrimParser() override = default;

    static constexpr auto name = "trim";
    String getName() const override { return name; }
    
    String getCHFunctionName(const substrait::Expression_ScalarFunction & func) const override
    {
        return func.arguments().size() == 1 ? "trimBoth" : "trimBothSpark";
    }
    
    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAGPtr & actions_dag) const override
    {
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        auto ch_function_name = getCHFunctionName(substrait_func);
        const auto & args = substrait_func.arguments();

        /// In substrait, the first arg is srcStr, the second arg is trimStr
        /// But in CH, the first arg is trimStr, the second arg is srcStr
        if (args.size() > 1)
        {
            parsed_args.emplace_back(parseFunctionArgument(actions_dag, ch_function_name, args[1]));
            parsed_args.emplace_back(parseFunctionArgument(actions_dag, ch_function_name, args[0]));
        }
        else
            parsed_args.emplace_back(parseFunctionArgument(actions_dag, ch_function_name, args[0]));

        const auto * func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};

static FunctionParserRegister<SparkFunctionTrimParser> register_trim;

class SparkFunctionLtrimParser : public FunctionParser
{
public:
    SparkFunctionLtrimParser(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~SparkFunctionLtrimParser() override = default;

    static constexpr auto name = "ltrim";
    String getName() const override { return name; }
    
    String getCHFunctionName(const substrait::Expression_ScalarFunction & func) const override
    {
        return func.arguments().size() == 1 ? "trimLeft" : "trimLeftSpark";
    }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAGPtr & actions_dag) const override
    {
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        auto ch_function_name = getCHFunctionName(substrait_func);
        const auto & args = substrait_func.arguments();

        /// In substrait, the first arg is srcStr, the second arg is trimStr
        /// But in CH, the first arg is trimStr, the second arg is srcStr
        if (args.size() > 1)
        {
            parsed_args.emplace_back(parseFunctionArgument(actions_dag, ch_function_name, args[1]));
            parsed_args.emplace_back(parseFunctionArgument(actions_dag, ch_function_name, args[0]));
        }
        else
            parsed_args.emplace_back(parseFunctionArgument(actions_dag, ch_function_name, args[0]));

        const auto * func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};
static FunctionParserRegister<SparkFunctionLtrimParser> register_ltrim;

class SparkFunctionRtrimParser : public FunctionParser
{
public:
    SparkFunctionRtrimParser(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~SparkFunctionRtrimParser() override = default;

    static constexpr auto name = "rtrim";
    String getName() const override { return name; }
    
    String getCHFunctionName(const substrait::Expression_ScalarFunction & func) const override
    {
        return func.arguments().size() == 1 ? "trimRight" : "trimRightSpark";
    }

    const DB::ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAGPtr & actions_dag) const override
    {
        DB::ActionsDAG::NodeRawConstPtrs parsed_args;
        auto ch_function_name = getCHFunctionName(substrait_func);
        const auto & args = substrait_func.arguments();

        /// In substrait, the first arg is srcStr, the second arg is trimStr
        /// But in CH, the first arg is trimStr, the second arg is srcStr
        if (args.size() > 1)
        {
            parsed_args.emplace_back(parseFunctionArgument(actions_dag, ch_function_name, args[1]));
            parsed_args.emplace_back(parseFunctionArgument(actions_dag, ch_function_name, args[0]));
        }
        else
            parsed_args.emplace_back(parseFunctionArgument(actions_dag, ch_function_name, args[0]));

        const auto * func_node = toFunctionNode(actions_dag, ch_function_name, parsed_args);
        return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
    }
};
static FunctionParserRegister<SparkFunctionRtrimParser> register_rtrim;
}

