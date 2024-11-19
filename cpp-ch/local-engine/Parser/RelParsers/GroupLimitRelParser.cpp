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

#include "GroupLimitRelParser.h"
#include <unordered_map>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/AggregateDescription.h>
#include <Operator/GraceMergingAggregatedStep.h>
#include <Operator/WindowGroupLimitStep.h>
#include <Parser/AdvancedParametersParseUtil.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/wrappers.pb.h>
#include <Common/AggregateUtil.h>
#include <Common/ArrayJoinHelper.h>
#include <Common/CHUtil.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace local_engine
{
GroupLimitRelParser::GroupLimitRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}

DB::QueryPlanPtr
GroupLimitRelParser::parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    const auto win_rel_def = rel.windowgrouplimit();
    google::protobuf::StringValue optimize_info_str;
    optimize_info_str.ParseFromString(win_rel_def.advanced_extension().optimization().value());
    auto optimization_info = WindowGroupOptimizationInfo::parse(optimize_info_str.value());
    if (optimization_info.is_aggregate_group_limit)
    {
        AggregateGroupLimitRelParser aggregate_group_limit_parser(parser_context);
        auto plan = aggregate_group_limit_parser.parse(std::move(current_plan_), rel, rel_stack_);
        steps = aggregate_group_limit_parser.getSteps();
        return std::move(plan);
    }
    else
    {
        WindowGroupLimitRelParser window_parser(parser_context);
        auto plan = window_parser.parse(std::move(current_plan_), rel, rel_stack_);
        steps = window_parser.getSteps();
        return std::move(plan);
    }
}

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

AggregateGroupLimitRelParser::AggregateGroupLimitRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}

DB::QueryPlanPtr AggregateGroupLimitRelParser::parse(
    DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    // 1. add a pre-projecttion. Make two tuple arguments for the aggregation function. One is the required columns for the output, the other
    //   is the required columns for sorting.
    // 2. Collect the sorting directions for each sorting field, Let them as the aggregation function's parameters.
    // 3. Add a aggregation step.
    // 4. Add a post-projecttion. Explode the aggregation function's result, since the result is an array.

    current_plan = std::move(current_plan_);
    input_header = current_plan->getCurrentHeader();
    win_rel_def = &rel.windowgrouplimit();

    const auto win_rel_def = rel.windowgrouplimit();
    google::protobuf::StringValue optimize_info_str;
    optimize_info_str.ParseFromString(win_rel_def.advanced_extension().optimization().value());
    auto optimization_info = WindowGroupOptimizationInfo::parse(optimize_info_str.value());
    limit = static_cast<size_t>(win_rel_def.limit());
    aggregate_function_name = getAggregateFunctionName(optimization_info.window_function);

    if (limit < 1)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid limit: {}", limit);

    prePrejectionForAggregateArguments();
    addGroupLmitAggregationStep();
    postProjectionForExplodingArrays();

    LOG_DEBUG(getLogger("AggregateGroupLimitRelParser"), "Final group limit plan:\n{}", PlanUtil::explainPlan(*current_plan));
    return std::move(current_plan);
}

String AggregateGroupLimitRelParser::getAggregateFunctionName(const String & window_function_name)
{
    if (window_function_name == "row_number")
        return "rowNumGroupArraySorted";
#if 0
    else if (window_function_name == "rank")
        return "groupArrayRankSorted";
    else if (window_function_name == "dense_rank")
        return "groupArrayDenseRankSorted";
#endif
    else
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported window function: {}", window_function_name);
}

static std::set<size_t> collectPartitionFields(const google::protobuf::RepeatedPtrField<substrait::Expression> & expressions)
{
    std::set<size_t> fields;
    for (const auto & expr : expressions)
        if (expr.has_selection())
            fields.insert(static_cast<size_t>(expr.selection().direct_reference().struct_field().field()));
        else if (expr.has_literal())
            continue;
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknow expression: {}", expr.DebugString());
    return fields;
}

// Build two tuple columns as the aggregate function's arguments
void AggregateGroupLimitRelParser::prePrejectionForAggregateArguments()
{
    auto projection_actions = std::make_shared<DB::ActionsDAG>(input_header.getColumnsWithTypeAndName());


    auto partition_fields = collectPartitionFields(win_rel_def->partition_expressions());
    DB::NameSet required_column_names;
    auto build_tuple = [&](const DB::DataTypes & data_types,
                           const Strings & names,
                           const DB::ActionsDAG::NodeRawConstPtrs & elements,
                           const String & name_prefix,
                           String & result_name)
    {
        result_name = expression_parser->getUniqueName(name_prefix);
        auto tuple = expression_parser->toFunctionNode(*projection_actions, "tuple", elements, result_name);
        auto tuple_type = std::make_shared<DB::DataTypeTuple>(data_types, names);
        DB::ActionsDAG::NodeRawConstPtrs cast_args;
        cast_args.push_back(tuple);
        cast_args.push_back(
            expression_parser->addConstColumn(*projection_actions, std::make_shared<DataTypeString>(), tuple_type->getName()));
        tuple = expression_parser->toFunctionNode(*projection_actions, "CAST", cast_args, result_name);
        projection_actions->addOrReplaceInOutputs(*tuple);
        required_column_names.insert(tuple->result_name);
    };

    DB::DataTypes aggregate_data_tuple_types;
    Strings aggregate_data_tuple_names;
    DB::ActionsDAG::NodeRawConstPtrs aggregate_data_tuple_nodes;
    for (size_t i = 0; i < input_header.columns(); ++i)
    {
        const auto & col = input_header.getByPosition(i);
        if (partition_fields.count(i))
        {
            required_column_names.insert(col.name);
            aggregate_grouping_keys.push_back(col.name);
        }
        else
        {
            aggregate_data_tuple_types.push_back(col.type);
            aggregate_data_tuple_names.push_back(col.name);
            aggregate_data_tuple_nodes.push_back(projection_actions->getInputs()[i]);
        }
    }
    build_tuple(
        aggregate_data_tuple_types,
        aggregate_data_tuple_names,
        aggregate_data_tuple_nodes,
        "aggregate_data_tuple",
        aggregate_tuple_column_name);

    projection_actions->removeUnusedActions(required_column_names);
    LOG_DEBUG(
        getLogger("AggregateGroupLimitRelParser"),
        "Projection for building tuples for aggregate function:\n{}",
        projection_actions->dumpDAG());

    auto expression_step = std::make_unique<DB::ExpressionStep>(input_header, std::move(*projection_actions));
    expression_step->setStepDescription("Pre-projection for aggregate group limit arguments");
    steps.push_back(expression_step.get());
    current_plan->addStep(std::move(expression_step));
}


String AggregateGroupLimitRelParser::parseSortDirections(const google::protobuf::RepeatedPtrField<substrait::SortField> & sort_fields)
{
    DB::Array directions;
    static const std::unordered_map<int, std::string> order_directions
        = {{1, " asc nulls first"}, {2, " asc nulls last"}, {3, " desc nulls first"}, {4, " desc nulls last"}};
    size_t n = 0;
    DB::WriteBufferFromOwnString ostr;
    for (const auto & sort_field : sort_fields)
    {
        auto it = order_directions.find(sort_field.direction());
        if (it == order_directions.end())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknow sort direction: {}", sort_field.direction());
        if (!sort_field.expr().has_selection())
        {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "Sort field must be a column reference. but got {}", sort_field.DebugString());
        }
        auto ref = sort_field.expr().selection().direct_reference().struct_field().field();
        const auto & col_name = input_header.getByPosition(ref).name;
        if (n)
            ostr << String(",");
        // the col_name may contain '#' which can may ch fail to parse.
        ostr << "`" << col_name << "`" << it->second;
        n += 1;
    }
    LOG_DEBUG(getLogger("AggregateGroupLimitRelParser"), "Order by clasue: {}", ostr.str());
    return ostr.str();
}

DB::AggregateDescription AggregateGroupLimitRelParser::buildAggregateDescription()
{
    DB::AggregateDescription agg_desc;
    agg_desc.column_name = aggregate_tuple_column_name;
    agg_desc.argument_names = {aggregate_tuple_column_name};
    DB::Array parameters;
    parameters.push_back(static_cast<UInt32>(limit));
    auto sort_directions = parseSortDirections(win_rel_def->sorts());
    parameters.push_back(sort_directions);

    auto header = current_plan->getCurrentHeader();
    DB::DataTypes arg_types;
    arg_types.push_back(header.getByName(aggregate_tuple_column_name).type);

    DB::AggregateFunctionProperties properties;
    agg_desc.function = getAggregateFunction(aggregate_function_name, arg_types, properties, parameters);
    return agg_desc;
}

void AggregateGroupLimitRelParser::addGroupLmitAggregationStep()
{
    const auto & settings = getContext()->getSettingsRef();
    DB::AggregateDescriptions agg_descs = {buildAggregateDescription()};
    auto params = AggregatorParamsHelper::buildParams(
        getContext(), aggregate_grouping_keys, agg_descs, AggregatorParamsHelper::Mode::INIT_TO_COMPLETED);
    auto agg_step = std::make_unique<GraceMergingAggregatedStep>(getContext(), current_plan->getCurrentHeader(), params, true);
    steps.push_back(agg_step.get());
    current_plan->addStep(std::move(agg_step));
    LOG_DEBUG(getLogger("AggregateGroupLimitRelParser"), "Plan after add group limit:\n{}", PlanUtil::explainPlan(*current_plan));
}

void AggregateGroupLimitRelParser::postProjectionForExplodingArrays()
{
    auto header = current_plan->getCurrentHeader();

    /// flatten the array column.
    auto agg_result_index = header.columns() - 1;
    auto array_join_actions_dag = ArrayJoinHelper::applyArrayJoinOnOneColumn(header, agg_result_index);
    auto new_steps = ArrayJoinHelper::addArrayJoinStep(getContext(), *current_plan, array_join_actions_dag, false);
    steps.insert(steps.end(), new_steps.begin(), new_steps.end());

    auto array_join_output_header = current_plan->getCurrentHeader();
    DB::ActionsDAG flatten_actions_dag(array_join_output_header.getColumnsWithTypeAndName());
    DB::Names flatten_output_column_names;
    for (size_t i = 0; i < array_join_output_header.columns() - 1; ++i)
    {
        const auto & col = array_join_output_header.getByPosition(i);
        flatten_output_column_names.push_back(col.name);
    }
    auto last_column = array_join_output_header.getByPosition(array_join_output_header.columns() - 1);
    const auto * tuple_column = typeid_cast<const DB::ColumnTuple *>(last_column.column.get());
    const auto * tuple_datatype = typeid_cast<const DB::DataTypeTuple *>(last_column.type.get());
    const auto & field_names = tuple_datatype->getElementNames();
    DB::DataTypePtr tuple_index_type = std::make_shared<DB::DataTypeUInt32>();
    const auto * tuple_node = flatten_actions_dag.getInputs().back();
    for (size_t i = 0; i < field_names.size(); ++i)
    {
        DB::ActionsDAG::NodeRawConstPtrs tuple_index_args;
        tuple_index_args.push_back(tuple_node);
        tuple_index_args.push_back(expression_parser->addConstColumn(flatten_actions_dag, tuple_index_type, i + 1));
        const auto * field_node = expression_parser->toFunctionNode(flatten_actions_dag, "tupleElement", tuple_index_args, field_names[i]);
        flatten_actions_dag.addOrReplaceInOutputs(*field_node);
        flatten_output_column_names.push_back(field_node->result_name);
    }
    flatten_actions_dag.removeUnusedActions(flatten_output_column_names);
    LOG_DEBUG(getLogger("AggregateGroupLimitRelParser"), "Actions dag for untupling aggregate result:\n{}", flatten_actions_dag.dumpDAG());
    auto flatten_expression_step = std::make_unique<DB::ExpressionStep>(current_plan->getCurrentHeader(), std::move(flatten_actions_dag));
    flatten_expression_step->setStepDescription("Untuple the aggregation result");
    steps.push_back(flatten_expression_step.get());
    current_plan->addStep(std::move(flatten_expression_step));

    auto flatten_tuple_output_header = current_plan->getCurrentHeader();
    auto window_result_column = flatten_tuple_output_header.getByPosition(flatten_tuple_output_header.columns() - 1);
    /// The result column is put at the end of the header.
    auto output_header = input_header;
    output_header.insert(window_result_column);
    auto adjust_pos_actions_dag = DB::ActionsDAG::makeConvertingActions(
        flatten_tuple_output_header.getColumnsWithTypeAndName(),
        output_header.getColumnsWithTypeAndName(),
        DB::ActionsDAG::MatchColumnsMode::Name);
    LOG_DEBUG(getLogger("AggregateGroupLimitRelParser"), "Actions dag for replacing columns:\n{}", adjust_pos_actions_dag.dumpDAG());
    auto adjust_pos_expression_step = std::make_unique<DB::ExpressionStep>(flatten_tuple_output_header, std::move(adjust_pos_actions_dag));
    adjust_pos_expression_step->setStepDescription("Adjust position of the output columns");
    steps.push_back(adjust_pos_expression_step.get());
    current_plan->addStep(std::move(adjust_pos_expression_step));
}

void registerWindowGroupLimitRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context) { return std::make_shared<GroupLimitRelParser>(parser_context); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kWindowGroupLimit, builder);
}
}
