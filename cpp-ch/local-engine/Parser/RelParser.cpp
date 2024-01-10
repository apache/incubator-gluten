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
#include <google/protobuf/wrappers.pb.h>
#include <Poco/StringTokenizer.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
AggregateFunctionPtr RelParser::getAggregateFunction(
    const DB::String & name,
    DB::DataTypes arg_types,
    DB::AggregateFunctionProperties & properties,
    const DB::Array & parameters)
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
        return {};
    auto function_signature = it->second;
    auto function_name = function_signature.substr(0, function_signature.find(':'));
    return function_name;
}

std::optional<String> RelParser::parseFunctionName(UInt32 function_ref, const substrait::Expression_ScalarFunction & function)
{
    auto sigature_name = parseSignatureFunctionName(function_ref);
    if (!sigature_name)
        return {};
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

std::map<std::string, std::string>
RelParser::parseFormattedRelAdvancedOptimization(const substrait::extensions::AdvancedExtension & advanced_extension)
{
    std::map<std::string, std::string> configs;
    if (advanced_extension.has_optimization())
    {
        google::protobuf::StringValue msg;
        advanced_extension.optimization().UnpackTo(&msg);
        Poco::StringTokenizer kvs(msg.value(), "\n");
        for (auto & kv : kvs)
        {
            if (kv.empty())
                continue;
            auto pos = kv.find('=');
            if (pos == std::string::npos)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid optimization config:{}.", kv);
            auto key = kv.substr(0, pos);
            auto value = kv.substr(pos + 1);
            configs[key] = value;
        }
    }
    return configs;
}

std::string
RelParser::getStringConfig(const std::map<std::string, std::string> & configs, const std::string & key, const std::string & default_value)
{
    auto it = configs.find(key);
    if (it == configs.end())
        return default_value;
    return it->second;
}


void RelParser::getUsedColumnsInBaseSchema(
    std::set<int> & ret,
    const substrait::Expression_ScalarFunction & scalar_function,
    int num_of_base_columns)
{
    for (const auto & arg : scalar_function.arguments())
        getUsedColumnsInBaseSchema(ret, arg.value(), num_of_base_columns);
}

void RelParser::getUsedColumnsInBaseSchema(std::set<int> & ret, const substrait::Expression & rel, int num_of_base_columns)
{
    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            break;
        }
        case substrait::Expression::RexTypeCase::kSelection: {
            if (!rel.selection().has_direct_reference() || !rel.selection().direct_reference().has_struct_field())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can only have direct struct references in selections");

            ret.insert(rel.selection().direct_reference().struct_field().field());
            break;
        }
        case substrait::Expression::RexTypeCase::kCast: {
            if (!rel.cast().has_type() || !rel.cast().has_input())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Doesn't have type or input in cast node.");

            const auto & input = rel.cast().input();
            getUsedColumnsInBaseSchema(ret, input, num_of_base_columns);
            break;
        }

        case substrait::Expression::RexTypeCase::kIfThen: {
            const auto & if_then = rel.if_then();
            auto condition_nums = if_then.ifs_size();

            for (int i = 0; i < condition_nums; ++i)
            {
                const auto & ifs = if_then.ifs(i);
                getUsedColumnsInBaseSchema(ret, ifs.if_(), num_of_base_columns);
                getUsedColumnsInBaseSchema(ret, ifs.then(), num_of_base_columns);
            }
            getUsedColumnsInBaseSchema(ret, if_then.else_(), num_of_base_columns);
            break;
        }
        case substrait::Expression::RexTypeCase::kScalarFunction: {
            if (!rel.has_scalar_function())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Doesn't have scala function in function node.");
            getUsedColumnsInBaseSchema(ret, rel.scalar_function(), num_of_base_columns);
            break;
        }
        case substrait::Expression::RexTypeCase::kSingularOrList: {
            getUsedColumnsInBaseSchema(ret, rel.singular_or_list().value(), num_of_base_columns);

            const auto & options = rel.singular_or_list().options();
            int options_len = static_cast<int>(options.size());
            for (int i = 0; i < options_len; ++i)
                if (!options[i].has_literal())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "in expression values must be the literal!");
            break;
        }
        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported spark expression type {} : {}",
                magic_enum::enum_name(rel.rex_type_case()),
                rel.DebugString());
    }
}

void RelParser::getUsedColumnsInBaseSchema(std::set<int> & ret, const substrait::Rel & rel, int num_of_base_columns)
{
    switch (rel.rel_type_case())
    {
        case substrait::Rel::RelTypeCase::kRead: {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Should not meet another Read rel");
        }
        case substrait::Rel::RelTypeCase::kFilter: {
            getUsedColumnsInBaseSchema(ret, rel.filter().condition(), num_of_base_columns);
            break;
        }
        case substrait::Rel::RelTypeCase::kProject: {
            for (int i = 0; i < rel.project().expressions_size(); ++i)
                getUsedColumnsInBaseSchema(ret, rel.project().expressions(i), num_of_base_columns);
            break;
        }
        case substrait::Rel::RelTypeCase::kGenerate: {
            if (!rel.generate().generator().has_scalar_function())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Doesn't have scala function in generate node.");
            getUsedColumnsInBaseSchema(ret, rel.generate().generator().scalar_function(), num_of_base_columns);

            for (size_t i = 0; i < rel.generate().child_output_size(); ++i)
            {
                getUsedColumnsInBaseSchema(ret, rel.generate().child_output(i), num_of_base_columns);
            }
            break;
        }
        case substrait::Rel::RelTypeCase::kAggregate: {
            auto & aggregate_rel = rel.aggregate();
            for (const auto & measure : aggregate_rel.measures())
            {
                for (size_t i = 0; i < measure.measure().arguments_size(); i++)
                    getUsedColumnsInBaseSchema(ret, measure.measure().arguments().at(i).value(), num_of_base_columns);
                for (size_t i = 0; i < measure.measure().sorts_size(); i++)
                    getUsedColumnsInBaseSchema(ret, measure.measure().sorts().at(i).expr(), num_of_base_columns);

                getUsedColumnsInBaseSchema(ret, measure.filter(), num_of_base_columns);
            }

            if (aggregate_rel.groupings_size() == 1)
            {
                for (const auto & expr : aggregate_rel.groupings(0).grouping_expressions())
                {
                    getUsedColumnsInBaseSchema(ret, expr, num_of_base_columns);
                }
            }
            else if (aggregate_rel.groupings_size() != 0)
            {
                throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupport multible groupings");
            }
            break;
        }
        case substrait::Rel::RelTypeCase::kJoin:
        case substrait::Rel::RelTypeCase::kFetch:
        case substrait::Rel::RelTypeCase::kSort: {
            // all input columns are useful
            for (size_t i = 0; i < num_of_base_columns; i++)
            {
                ret.insert(i);
            }
            break;
        }
        case substrait::Rel::RelTypeCase::kWindow: {
            auto & window_rel = rel.window();
            for (const auto & measure : window_rel.measures())
            {
                for (size_t i = 0; i < measure.measure().arguments_size(); i++)
                    getUsedColumnsInBaseSchema(ret, measure.measure().arguments().at(i).value(), num_of_base_columns);
                for (size_t i = 0; i < measure.measure().sorts_size(); i++)
                    getUsedColumnsInBaseSchema(ret, measure.measure().sorts().at(i).expr(), num_of_base_columns);
            }

            for (size_t i = 0; i < window_rel.sorts_size(); i++)
            {
                getUsedColumnsInBaseSchema(ret, window_rel.sorts().at(i).expr(), num_of_base_columns);
            }
            for (size_t i = 0; i < window_rel.partition_expressions_size(); i++)
            {
                getUsedColumnsInBaseSchema(ret, window_rel.partition_expressions().at(i), num_of_base_columns);
            }
            break;
        }
        case substrait::Rel::RelTypeCase::kExpand: {
            auto & expand_rel = rel.expand();
            for (size_t i = 0; i < expand_rel.fields_size(); i++)
            {
                if (expand_rel.fields().at(i).has_consistent_field())
                {
                    getUsedColumnsInBaseSchema(ret, expand_rel.fields().at(i).consistent_field(), num_of_base_columns);
                }
                else if (expand_rel.fields().at(i).has_switching_field())
                {
                    auto & s = expand_rel.fields().at(i).switching_field();
                    for (size_t j = 0; j < s.duplicates_size(); j++)
                    {
                        getUsedColumnsInBaseSchema(ret, s.duplicates(j), num_of_base_columns);
                    }
                }
                else
                {
                    throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupport expand field type");
                }
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "doesn't support relation type: {}.\n{}", rel.rel_type_case(), rel.DebugString());
    }
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
        throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Duplicated builder key:{}", k);
    builders[k] = builder;
}

RelParserFactory::RelParserBuilder RelParserFactory::getBuilder(DB::UInt32 k)
{
    auto it = builders.find(k);
    if (it == builders.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found builder for key:{}", k);
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
