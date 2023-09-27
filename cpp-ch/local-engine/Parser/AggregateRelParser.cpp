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
#include "AggregateRelParser.h"
#include <memory>
#include <AggregateFunctions/AggregateFunctionIf.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Parser/AggregateFunctionParser.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include "Common/PODArray.h"
#include <Common/StringUtils/StringUtils.h>
#include "DataTypes/IDataType.h"

#include <Operator/EmptyHashAggregate.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TYPE;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}
namespace local_engine
{

AggregateRelParser::AggregateRelParser(SerializedPlanParser * plan_paser_) : RelParser(plan_paser_)
{
}

DB::QueryPlanPtr AggregateRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> &)
{
    setup(std::move(query_plan), rel);
    LOG_TRACE(logger, "original header is: {}", plan->getCurrentDataStream().header.dumpStructure());
    if (rel.aggregate().measures().empty()
        && (rel.aggregate().groupings().empty() || rel.aggregate().groupings()[0].grouping_expressions().empty()))
    {
        LOG_TRACE(&Poco::Logger::get("AggregateRelParser"), "Empty aggregate step");
        auto empty_agg = std::make_unique<EmptyHashAggregateStep>(plan->getCurrentDataStream());
        empty_agg->setStepDescription("Empty aggregate");
        steps.push_back(empty_agg.get());
        plan->addStep(std::move(empty_agg));
        return std::move(plan);
    }
    addPreProjection();
    LOG_TRACE(logger, "header after pre-projection is: {}", plan->getCurrentDataStream().header.dumpStructure());
    if (has_final_stage)
    {
        addMergingAggregatedStep();
        LOG_TRACE(logger, "header after merging is: {}", plan->getCurrentDataStream().header.dumpStructure());
        addPostProjection();
        LOG_TRACE(logger, "header after post-projection is: {}", plan->getCurrentDataStream().header.dumpStructure());
    }
    else
    {
        addAggregatingStep();
        LOG_TRACE(logger, "header after aggregating is: {}", plan->getCurrentDataStream().header.dumpStructure());
    }
    return std::move(plan);
}

void AggregateRelParser::setup(DB::QueryPlanPtr query_plan, const substrait::Rel & rel)
{
    plan = std::move(query_plan);
    aggregate_rel = &rel.aggregate();

    std::set<substrait::AggregationPhase> phase_set;
    for (const auto & measure : aggregate_rel->measures())
    {
        auto phase = measure.measure().phase();
        phase_set.insert(phase);
    }
    has_first_stage = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE);
    has_inter_stage = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE);
    has_final_stage = phase_set.contains(substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT);

    if (phase_set.size() > 1 && has_final_stage)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "AggregateRelParser: multiple aggregation phases with final stage are not supported");
    }

    auto input_header = plan->getCurrentDataStream().header;
    for (const auto & measure : aggregate_rel->measures())
    {
        AggregateInfo agg_info;
        auto arg = measure.measure().arguments(0).value();
        agg_info.signature_function_name = *parseSignatureFunctionName(measure.measure().function_reference());
        auto function_parser = AggregateFunctionParserFactory::instance().get(agg_info.signature_function_name, getPlanParser());
        if (!function_parser)
        {
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported aggregate function: {}", agg_info.signature_function_name);
        }
        /// Put function_parser, parser_func_info and function_name into agg_info for reducing repeated builds.
        agg_info.function_parser = function_parser;
        agg_info.parser_func_info = AggregateFunctionParser::CommonFunctionInfo(measure);
        agg_info.function_name = function_parser->getCHFunctionName(agg_info.parser_func_info);
        agg_info.measure = &measure;
        aggregates.push_back(agg_info);
    }

    if (aggregate_rel->groupings_size() == 1)
    {
        for (const auto & expr : aggregate_rel->groupings(0).grouping_expressions())
        {
            if (expr.has_selection() && expr.selection().has_direct_reference())
            {
                grouping_keys.push_back(input_header.getByPosition(expr.selection().direct_reference().struct_field().field()).name);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported group expression: {}", expr.DebugString());
            }
        }
    }
    else if (aggregate_rel->groupings_size() != 0)
    {
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupport multible groupings");
    }
}

/// Projections for function arguments.
/// The projections are built by the function parsers.
void AggregateRelParser::addPreProjection()
{
    auto input_header = plan->getCurrentDataStream().header;
    ActionsDAGPtr projection_action = std::make_shared<ActionsDAG>(input_header.getColumnsWithTypeAndName());
    std::string dag_footprint = projection_action->dumpDAG();
    for (auto & agg_info : aggregates)
    {
        auto arg_nodes = agg_info.function_parser->parseFunctionArguments(agg_info.parser_func_info, projection_action);
        // This may remove elements from arg_nodes, because some of them are converted to CH func parameters.
        agg_info.params = agg_info.function_parser->parseFunctionParameters(agg_info.parser_func_info, arg_nodes);
        for (auto & arg_node : arg_nodes)
        {
            agg_info.arg_column_names.emplace_back(arg_node->result_name);
            agg_info.arg_column_types.emplace_back(arg_node->result_type);
            projection_action->addOrReplaceInOutputs(*arg_node);
        }
    }
    if (projection_action->dumpDAG() != dag_footprint)
    {
        auto projection_step = std::make_unique<DB::ExpressionStep>(plan->getCurrentDataStream(), projection_action);
        projection_step->setStepDescription("Projection before aggregate");
        steps.emplace_back(projection_step.get());
        plan->addStep(std::move(projection_step));
    }
}

void AggregateRelParser::buildAggregateDescriptions(AggregateDescriptions & descriptions)
{
    auto build_result_column_name = [](const String & function_name, const Strings & arg_column_names, substrait::AggregationPhase phase)
    {
        if (phase != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
        {
            assert(arg_column_names.size() == 1);
            return arg_column_names[0];
        }
        String arg_list_str = boost::algorithm::join(arg_column_names, ",");
        return function_name + "(" + arg_list_str + ")";
    };
    for (auto & agg_info : aggregates)
    {
        AggregateDescription description;
        const auto & measure = agg_info.measure->measure();
        description.column_name = build_result_column_name(agg_info.function_name, agg_info.arg_column_names, measure.phase());
        description.argument_names = agg_info.arg_column_names;
        DB::AggregateFunctionProperties properties;

        if (!agg_info.function_name.ends_with("State"))
        {
            // May apply `PartialMerge` and `If` on the original function.
            auto [combinator_function_name, combinator_function_arg_types] = agg_info.function_parser->tryApplyCHCombinator(
                agg_info.parser_func_info, agg_info.function_name, agg_info.arg_column_types);
            description.function
                = getAggregateFunction(combinator_function_name, combinator_function_arg_types, properties, agg_info.params);
        }
        else
        {
            // If the function is a state function, we don't need to apply `PartialMerge`.
            // In INITIAL_TO_INTERMEDIATE phase, we do arguments -> xxState.
            // In INTERMEDIATE_TO_RESULT phase, we do xxState -> xxState.
            if (agg_info.parser_func_info.phase == substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
            {
                description.function = getAggregateFunction(agg_info.function_name, agg_info.arg_column_types, properties, agg_info.params);
            }
            else if (agg_info.parser_func_info.phase == substrait::AggregationPhase::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT)
            {
                if (agg_info.arg_column_types.size() != 1)
                    throw Exception(
                        DB::ErrorCodes::BAD_ARGUMENTS,
                        "Only support one argument aggregate function in phase {}",
                        agg_info.parser_func_info.phase);
                const auto * type = checkAndGetDataType<DataTypeAggregateFunction>(agg_info.arg_column_types[0].get());
                if (!type)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Argument for function {} must have type AggregateFunction as arguments",
                        agg_info.function_name);
                auto nested_types = type->getArgumentsDataTypes();
                description.function = getAggregateFunction(agg_info.function_name, nested_types, properties, agg_info.params);
            }
            else
            {
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported phase for state function: {}", agg_info.function_name);
            }
        }
        description.parameters = agg_info.params;
        descriptions.emplace_back(description);
    }
}

void AggregateRelParser::addMergingAggregatedStep()
{
    AggregateDescriptions aggregate_descriptions;
    buildAggregateDescriptions(aggregate_descriptions);
    auto settings = getContext()->getSettingsRef();
    Aggregator::Params params(grouping_keys, aggregate_descriptions, false, settings.max_threads, settings.max_block_size);
    auto merging_step = std::make_unique<DB::MergingAggregatedStep>(
        plan->getCurrentDataStream(),
        params,
        true,
        false,
        1,
        1,
        false,
        settings.max_block_size,
        settings.aggregation_in_order_max_block_bytes,
        SortDescription(),
        settings.enable_memory_bound_merging_of_aggregation_results);
    steps.emplace_back(merging_step.get());
    plan->addStep(std::move(merging_step));
}

void AggregateRelParser::addAggregatingStep()
{
    AggregateDescriptions aggregate_descriptions;
    buildAggregateDescriptions(aggregate_descriptions);
    auto settings = getContext()->getSettingsRef();
    Aggregator::Params params(
        grouping_keys,
        aggregate_descriptions,
        false,
        settings.max_rows_to_group_by,
        settings.group_by_overflow_mode,
        settings.group_by_two_level_threshold,
        settings.group_by_two_level_threshold_bytes,
        settings.max_bytes_before_external_group_by,
        settings.empty_result_for_aggregation_by_empty_set,
        getContext()->getTempDataOnDisk(),
        settings.max_threads,
        settings.min_free_disk_space_for_temporary_data,
        true,
        3,
        settings.max_block_size,
        /*enable_prefetch*/ true,
        /*only_merge*/ false,
        settings.optimize_group_by_constant_keys);

    auto aggregating_step = std::make_unique<AggregatingStep>(
        plan->getCurrentDataStream(),
        params,
        GroupingSetsParamsList(),
        false,
        settings.max_block_size,
        settings.aggregation_in_order_max_block_bytes,
        1,
        1,
        false,
        false,
        SortDescription(),
        SortDescription(),
        false,
        false,
        false);
    steps.emplace_back(aggregating_step.get());
    plan->addStep(std::move(aggregating_step));
}

// Only be called in final stage.
void AggregateRelParser::addPostProjection()
{
    auto input_header = plan->getCurrentDataStream().header;
    ActionsDAGPtr project_actions_dag = std::make_shared<ActionsDAG>(input_header.getColumnsWithTypeAndName());
    auto dag_footprint = project_actions_dag->dumpDAG();
    for (const auto & agg_info : aggregates)
    {
        /// For final stage, the aggregate function's input is only one intermediate result columns.
        /// The final result columm's position is the same as the intermediate result column's position.
        auto pos = agg_info.measure->measure().arguments(0).value().selection().direct_reference().struct_field().field();
        const auto * agg_result_node = project_actions_dag->getInputs()[pos];
        agg_info.function_parser->convertNodeTypeIfNeeded(agg_info.parser_func_info, agg_result_node, project_actions_dag);
    }
    if (project_actions_dag->dumpDAG() != dag_footprint)
    {
        QueryPlanStepPtr convert_step = std::make_unique<ExpressionStep>(plan->getCurrentDataStream(), project_actions_dag);
        convert_step->setStepDescription("Post-projection for aggregate");
        steps.emplace_back(convert_step.get());
        plan->addStep(std::move(convert_step));
    }
}

void registerAggregateParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_parser) { return std::make_shared<AggregateRelParser>(plan_parser); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kAggregate, builder);
}
}
