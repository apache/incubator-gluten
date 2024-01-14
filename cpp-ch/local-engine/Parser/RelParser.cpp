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
#include "RelParser.h"
#include <string>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>
#include <google/protobuf/wrappers.pb.h>
#include <Poco/StringTokenizer.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
AggregateFunctionPtr RelParser::getAggregateFunction(
    const String & name, DB::DataTypes arg_types, DB::AggregateFunctionProperties & properties, const DB::Array & parameters)
{
    auto & factory = AggregateFunctionFactory::instance();
    auto action = NullsAction::EMPTY;
    return factory.get(name, action, arg_types, parameters, properties);
}

std::optional<String> RelParser::parseSignatureFunctionName(UInt32 function_ref)
{
    const auto & function_mapping = getFunctionMapping();
    auto it = function_mapping.find(std::to_string(function_ref));
    if (it == function_mapping.end())
    {
        return {};
    }
    auto function_signature = it->second;
    auto function_name = function_signature.substr(0, function_signature.find(':'));
    return function_name;
}

std::optional<String> RelParser::parseFunctionName(UInt32 function_ref, const substrait::Expression_ScalarFunction & function)
{
    auto sigature_name = parseSignatureFunctionName(function_ref);
    if (!sigature_name)
    {
        return {};
    }
    return plan_parser->getFunctionName(*sigature_name, function);
}
DB::QueryPlanPtr RelParser::parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    SerializedPlanParser & planParser = *getPlanParser();
    rel_stack.push_back(&rel);
    auto query_plan = planParser.parseOp(getSingleInput(rel), rel_stack);
    rel_stack.pop_back();
    return parse(std::move(query_plan), rel, rel_stack);
}

std::map<std::string, std::string> RelParser::parseFormattedRelAdvancedOptimization(const substrait::extensions::AdvancedExtension &advanced_extension)
{
    std::map<std::string, std::string> configs;
    if (advanced_extension.has_optimization())
    {
        google::protobuf::StringValue msg;
        advanced_extension.optimization().UnpackTo(&msg);
        Poco::StringTokenizer kvs( msg.value(), "\n");
        for (auto & kv : kvs)
        {
            if (kv.empty())
                continue;
            auto pos = kv.find('=');
            if (pos == std::string::npos)
            {
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid optimization config:{}.", kv);
            }
            auto key = kv.substr(0, pos);
            auto value = kv.substr(pos + 1);
            configs[key] = value;
        }
    }
    return configs;
}

std::string RelParser::getStringConfig(const std::map<std::string, std::string> & configs, const std::string & key, const std::string & default_value)
{
    auto it = configs.find(key);
    if (it == configs.end())
        return default_value;
    return it->second;
}


RelParserFactory & RelParserFactory::instance()
{
    static RelParserFactory factory;
    return factory;
}

void RelParserFactory::registerBuilder(UInt32 k, RelParserBuilder builder)
{
    auto it = builders.find(k);
    if (it != builders.end())
    {
        throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Duplicated builder key:{}", k);
    }
    builders[k] = builder;
}

RelParserFactory::RelParserBuilder RelParserFactory::getBuilder(UInt32 k)
{
    auto it = builders.find(k);
    if (it == builders.end())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found builder for key:{}", k);
    }
    return it->second;
}

void registerWindowRelParser(RelParserFactory & factory);
void registerSortRelParser(RelParserFactory & factory);
void registerExpandRelParser(RelParserFactory & factory);
void registerAggregateParser(RelParserFactory & factory);
void registerProjectRelParser(RelParserFactory & factory);
void registerJoinRelParser(RelParserFactory & factory);
void registerFilterRelParser(RelParserFactory & factory);

void registerRelParsers()
{
    auto & factory = RelParserFactory::instance();
    registerWindowRelParser(factory);
    registerSortRelParser(factory);
    registerExpandRelParser(factory);
    registerAggregateParser(factory);
    registerProjectRelParser(factory);
    registerJoinRelParser(factory);
    registerFilterRelParser(factory);
}
}
