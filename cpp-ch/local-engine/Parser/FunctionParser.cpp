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

#include "FunctionParser.h"
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/TypeParser.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
    extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{
using namespace DB;

String FunctionParser::getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const
{
    // no meaning
    /// There is no any simple equivalent ch function.
    return "";
}

ActionsDAG::NodeRawConstPtrs FunctionParser::parseFunctionArguments(
    const substrait::Expression_ScalarFunction & substrait_func, ActionsDAGPtr & actions_dag) const
{
    ActionsDAG::NodeRawConstPtrs parsed_args;
    const auto & args = substrait_func.arguments();
    parsed_args.reserve(args.size());
    plan_parser->parseFunctionArguments(actions_dag, parsed_args, substrait_func);
    return parsed_args;
}



const ActionsDAG::Node *
FunctionParser::parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAGPtr & actions_dag) const
{
    auto ch_func_name = getCHFunctionName(substrait_func);
    auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
    const auto * func_node = toFunctionNode(actions_dag, ch_func_name, parsed_args);
    return convertNodeTypeIfNeeded(substrait_func, func_node, actions_dag);
}

const ActionsDAG::Node * FunctionParser::convertNodeTypeIfNeeded(
    const substrait::Expression_ScalarFunction & substrait_func, const ActionsDAG::Node * func_node, ActionsDAGPtr & actions_dag) const
{
    const auto & output_type = substrait_func.output_type();
    if (!TypeParser::isTypeMatched(output_type, func_node->result_type))
    {
        auto result_type = TypeParser::parseType(substrait_func.output_type());
        if (DB::isDecimalOrNullableDecimal(result_type))
        {
            return ActionsDAGUtil::convertNodeType(
                actions_dag,
                func_node,
                // as stated in isTypeMatched， currently we don't change nullability of the result type
                func_node->result_type->isNullable() ? local_engine::wrapNullableType(true, result_type)->getName()
                                                         : local_engine::removeNullable(result_type)->getName(),
                func_node->result_name,
                CastType::accurateOrNull);
        }
        else
        {
            return ActionsDAGUtil::convertNodeType(
                actions_dag,
                func_node,
                // as stated in isTypeMatched， currently we don't change nullability of the result type
                func_node->result_type->isNullable() ? local_engine::wrapNullableType(true, TypeParser::parseType(output_type))->getName()
                                                     : DB::removeNullable(TypeParser::parseType(output_type))->getName(),
                func_node->result_name);
        }
    }
    else
        return func_node;
}

void FunctionParserFactory::registerFunctionParser(const String & name, Value value)
{
    if (!parsers.emplace(name, value).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FunctionParserFactory: function parser name '{}' is not unique", name);
}

FunctionParserPtr FunctionParserFactory::get(const String & name, SerializedPlanParser * plan_parser)
{
    auto res = tryGet(name, plan_parser);
    if (!res)
    {
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unknown function parser {}", name);
    }

    return res;
}

FunctionParserPtr FunctionParserFactory::tryGet(const String & name, SerializedPlanParser * plan_parser)
{
    auto it = parsers.find(name);
    if (it != parsers.end())
    {
        auto creator = it->second;
        return creator(plan_parser);
    }
    else
        return {};
}

FunctionParserFactory & FunctionParserFactory::instance()
{
    static FunctionParserFactory factory;
    return factory;
}

}
