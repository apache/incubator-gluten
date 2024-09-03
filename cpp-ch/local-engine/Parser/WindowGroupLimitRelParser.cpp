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

#include <Interpreters/ActionsDAG.h>
#include <Parser/SortRelParser.h>
#include <Parser/WindowGroupLimitRelParser.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <google/protobuf/repeated_field.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

const static String FUNCTION_ROW_NUM = "row_number";
const static String FUNCTION_RANK = "top_rank";
const static String FUNCTION_DENSE_RANK = "top_dense_rank";

namespace local_engine
{
WindowGroupLimitRelParser::WindowGroupLimitRelParser(SerializedPlanParser * plan_parser_) : RelParser(plan_parser_)
{
    LOG_ERROR(getLogger("WindowGroupLimitRelParser"), "xxx new parrser");
}
DB::QueryPlanPtr
WindowGroupLimitRelParser::parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    const auto win_rel_def = rel.windowgrouplimit();
    current_plan = std::move(current_plan_);

    DB::Block output_header = current_plan->getCurrentDataStream().header;

    window_function_name = FUNCTION_ROW_NUM;
    LOG_ERROR(getLogger("WindowGroupLimitRelParser"), "xxx input header: {}", current_plan->getCurrentDataStream().header.dumpStructure());

    /// Only one window function in one window group limit
    auto win_desc = buildWindowDescription(win_rel_def);

    auto win_step = std::make_unique<DB::WindowStep>(current_plan->getCurrentDataStream(), win_desc, win_desc.window_functions, false);
    win_step->setStepDescription("Window Group Limit " + win_desc.window_name);
    steps.emplace_back(win_step.get());
    current_plan->addStep(std::move(win_step));

    /// remove the window function result column which is not needed in later steps
    DB::ActionsDAG post_project_actions_dag = DB::ActionsDAG::makeConvertingActions(
        current_plan->getCurrentDataStream().header.getColumnsWithTypeAndName(),
        output_header.getColumnsWithTypeAndName(),
        DB::ActionsDAG::MatchColumnsMode::Name);
    auto post_project_step
        = std::make_unique<DB::ExpressionStep>(current_plan->getCurrentDataStream(), std::move(post_project_actions_dag));
    post_project_step->setStepDescription("Window group limit: drop window function result column");
    steps.emplace_back(post_project_step.get());
    current_plan->addStep(std::move(post_project_step));

    LOG_ERROR(getLogger("WindowGroupLimitRelParser"), "xxx output header: {}", current_plan->getCurrentDataStream().header.dumpStructure());
    bool x = true;
    if (x)
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalide rel");
    }
    return std::move(current_plan);
}

DB::WindowFrame WindowGroupLimitRelParser::buildWindowFrame(const String & function_name)
{
    DB::WindowFrame frame;
    if (function_name == FUNCTION_ROW_NUM)
    {
        frame.type = DB::WindowFrame::FrameType::ROWS;
        frame.begin_type = DB::WindowFrame::BoundaryType::Offset;
        frame.begin_offset = 1;
        frame.begin_preceding = true;
        frame.end_type = DB::WindowFrame::BoundaryType::Current;
        frame.end_offset = 0;
        frame.end_preceding = true;
    }
    else if (function_name == FUNCTION_RANK || function_name == FUNCTION_DENSE_RANK)
    {
        // rank and dense_rank can only work on range mode
        frame.type = DB::WindowFrame::FrameType::RANGE;
        frame.begin_type = DB::WindowFrame::BoundaryType::Unbounded;
        frame.begin_offset = 0;
        frame.begin_preceding = true;
        frame.end_type = DB::WindowFrame::BoundaryType::Current;
        frame.end_offset = 0;
        frame.end_preceding = true;
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown function {} for window group limit", function_name);
    }

    return frame;
}

DB::WindowDescription WindowGroupLimitRelParser::buildWindowDescription(const substrait::WindowGroupLimitRel & win_rel_def)
{
    DB::WindowDescription win_desc;
    win_desc.frame = buildWindowFrame(window_function_name);
    win_desc.partition_by = parsePartitionBy(win_rel_def.partition_expressions());
    win_desc.order_by = SortRelParser::parseSortDescription(win_rel_def.sorts(), current_plan->getCurrentDataStream().header);
    win_desc.full_sort_description = win_desc.partition_by;
    win_desc.full_sort_description.insert(win_desc.full_sort_description.end(), win_desc.order_by.begin(), win_desc.order_by.end());

    DB::WriteBufferFromOwnString ss;
    ss << "partition by " << DB::dumpSortDescription(win_desc.partition_by);
    ss << "order by " << DB::dumpSortDescription(win_desc.order_by);
    ss << win_desc.frame.toString();
    win_desc.window_name = ss.str();

    win_desc.window_functions.emplace_back(buildWindowFunctionDescription(window_function_name));

    return win_desc;
}

DB::SortDescription
WindowGroupLimitRelParser::parsePartitionBy(const google::protobuf::RepeatedPtrField<substrait::Expression> & expressions)
{
    DB::Block header = current_plan->getCurrentDataStream().header;
    DB::SortDescription sort_desc;
    for (const auto & expr : expressions)
    {
        if (expr.has_selection())
        {
            auto pos = expr.selection().direct_reference().struct_field().field();
            auto col_name = header.getByPosition(pos).name;
            sort_desc.push_back(DB::SortColumnDescription(col_name, 1, 1));
        }
        else if (expr.has_literal())
        {
            continue;
        }
        else
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknow partition argument: {}", expr.DebugString());
        }
    }
    return sort_desc;
}

DB::WindowFunctionDescription WindowGroupLimitRelParser::buildWindowFunctionDescription(const String & function_name)
{
    DB::WindowFunctionDescription desc;
    desc.column_name = function_name;
    desc.function_node = nullptr;
    DB::AggregateFunctionProperties func_properties;
    DB::Names func_args;
    DB::DataTypes func_args_types;
    DB::Array func_params;
    auto func_ptr = RelParser::getAggregateFunction(function_name, func_args_types, func_properties, func_params);
    desc.argument_names = func_args;
    desc.argument_types = func_args_types;
    desc.aggregate_function = func_ptr;
    return desc;
}
void registerWindowGroupLimitRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_parser) { return std::make_shared<WindowGroupLimitRelParser>(plan_parser); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kWindowGroupLimit, builder);
}
}
