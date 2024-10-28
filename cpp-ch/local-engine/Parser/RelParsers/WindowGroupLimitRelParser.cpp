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

#include "WindowGroupLimitRelParser.h"
#include <Operator/WindowGroupLimitStep.h>
#include <Parser/AdvancedParametersParseUtil.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/wrappers.pb.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace local_engine
{
WindowGroupLimitRelParser::WindowGroupLimitRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}

DB::QueryPlanPtr
WindowGroupLimitRelParser::parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    const auto win_rel_def = rel.windowgrouplimit();
    google::protobuf::StringValue optimize_info_str;
    optimize_info_str.ParseFromString(win_rel_def.advanced_extension().optimization().value());
    auto optimization_info = WindowGroupOptimizationInfo::parse(optimize_info_str.value());
    window_function_name = optimization_info.window_function;

    current_plan = std::move(current_plan_);

    auto partition_fields = parsePartitoinFields(win_rel_def.partition_expressions());
    auto sort_fields = parseSortFields(win_rel_def.sorts());
    size_t limit = static_cast<size_t>(win_rel_def.limit());

    auto window_group_limit_step = std::make_unique<WindowGroupLimitStep>(
        current_plan->getCurrentHeader(), window_function_name, partition_fields, sort_fields, limit);
    window_group_limit_step->setStepDescription("Window group limit");
    steps.emplace_back(window_group_limit_step.get());
    current_plan->addStep(std::move(window_group_limit_step));

    return std::move(current_plan);
}

std::vector<size_t>
WindowGroupLimitRelParser::parsePartitoinFields(const google::protobuf::RepeatedPtrField<substrait::Expression> & expressions)
{
    std::vector<size_t> fields;
    for (const auto & expr : expressions)
        if (expr.has_selection())
            fields.push_back(static_cast<size_t>(expr.selection().direct_reference().struct_field().field()));
        else if (expr.has_literal())
            continue;
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknow expression: {}", expr.DebugString());
    return fields;
}

std::vector<size_t> WindowGroupLimitRelParser::parseSortFields(const google::protobuf::RepeatedPtrField<substrait::SortField> & sort_fields)
{
    std::vector<size_t> fields;
    for (const auto sort_field : sort_fields)
        if (sort_field.expr().has_literal())
            continue;
        else if (sort_field.expr().has_selection())
            fields.push_back(static_cast<size_t>(sort_field.expr().selection().direct_reference().struct_field().field()));
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown expression: {}", sort_field.expr().DebugString());
    return fields;
}

void registerWindowGroupLimitRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_shared<WindowGroupLimitRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kWindowGroupLimit, builder);
}
}
