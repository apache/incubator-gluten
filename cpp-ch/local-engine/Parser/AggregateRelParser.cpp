#include "AggregateRelParser.h"
#include <memory>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TYPE;
}
}
namespace local_engine
{

AggregateRelParser::AggregateRelParser(SerializedPlanParser * plan_paser_)
    : RelParser(plan_paser_)
{}

DB::QueryPlanPtr AggregateRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    setup(std::move(query_plan), rel);
    LOG_TRACE(logger, "original header is: {}", plan->getCurrentDataStream().header.dumpStructure());
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
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "AggregateRelParser: multiple aggregation phases with final stage are not supported");
    }

    auto input_header = plan->getCurrentDataStream().header;
    for (const auto & measure : aggregate_rel->measures())
    {
        if (measure.measure().arguments_size() != 1)
        {
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "only support one argument aggregate function");
        }
        AggregateInfo agg_info;
        auto arg = measure.measure().arguments(0).value();
        auto function_name = parseFunctionName(measure.measure().function_reference(), {});
        if (!function_name)
        {
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported aggregate function");
        }
        agg_info.measure = &measure;
        agg_info.function_name = *function_name;
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

// projections for function arguments.
// 1. add literal columns
// 2. change argument columns' nullabitlity.
void AggregateRelParser::addPreProjection()
{
    auto input_header = plan->getCurrentDataStream().header;
    bool need_projection = false;
    ActionsDAGPtr projection_action = std::make_shared<ActionsDAG>(input_header.getColumnsWithTypeAndName());
    for (auto & agg_info : aggregates)
    {
        auto arg = agg_info.measure->measure().arguments(0).value();
        if (arg.has_selection())
        {
            agg_info.arg_column_name = input_header.getByPosition(arg.selection().direct_reference().struct_field().field()).name;
        }
        else if (arg.has_literal())
        {
            const auto * node = parseArgument(projection_action, arg);
            projection_action->addOrReplaceInOutputs(*node);
            agg_info.arg_column_name = node->result_name;
            need_projection = true;
        }
        else
        {
            throw Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unsupported aggregate argument: {}", arg.DebugString());
        }

        auto required_output_which_type = WhichDataType(parseType(agg_info.measure->measure().output_type()));
        if (required_output_which_type.isNullable()
            && agg_info.measure->measure().phase() == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
            && !projection_action->findInOutputs(agg_info.arg_column_name).result_type->isNullable())
        {
            ActionsDAG::NodeRawConstPtrs args;
            agg_info.has_mismatch_nullablity = true;
            args.emplace_back(&projection_action->findInOutputs(agg_info.arg_column_name));
            const auto * node = buildFunctionNode(projection_action, "toNullable", args);
            projection_action->addOrReplaceInOutputs(*node);
            agg_info.arg_column_name = node->result_name;
            need_projection = true;
        }
    }
    if (need_projection)
    {
        auto projection_step = std::make_unique<DB::ExpressionStep>(plan->getCurrentDataStream(), projection_action);
        projection_step->setStepDescription("Projection before aggregate");
        steps.emplace_back(projection_step.get());
        plan->addStep(std::move(projection_step));
    }
}

void AggregateRelParser::buildAggregateDescriptions(AggregateDescriptions & descriptions)
{
    for (auto & agg_info : aggregates)
    {
        AggregateDescription description;
        const auto & measure = agg_info.measure->measure();
        if (measure.phase() != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
        {
            description.column_name = agg_info.arg_column_name;
        }
        else
        {
            description.column_name = agg_info.function_name + "(" + agg_info.arg_column_name + ")";
        }
        description.argument_names = {agg_info.arg_column_name};
        auto arg_type = plan->getCurrentDataStream().header.getByName(agg_info.arg_column_name).type;
        if (const auto * function_type = DB::checkAndGetDataType<DB::DataTypeAggregateFunction>(arg_type.get()))
        {
            const auto * suffix = "PartialMerge";
            description.function = getAggregateFunction(agg_info.function_name + suffix, {arg_type});
        }
        else
        {
            auto function_name = agg_info.function_name;
            auto final_arg_type = arg_type;
            if (measure.phase() != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
            {
                auto first = getAggregateFunction(agg_info.function_name, {arg_type});
                final_arg_type = first->getStateType();
                const auto * suffix = "PartialMerge";
                function_name = function_name + suffix;
            }
            description.function = getAggregateFunction(function_name, {final_arg_type});
        }
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
        false,
        false);

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
    addPostProjectionForAggregatingResult();   
    addPostProjectionForTypeMismatch();
}

// Handle special cases for aggregate result.
void AggregateRelParser::addPostProjectionForAggregatingResult()
{
    auto input_header = plan->getCurrentDataStream().header;
    auto current_cols = plan->getCurrentDataStream().header.getColumnsWithTypeAndName();
    ActionsDAGPtr projection_action = std::make_shared<ActionsDAG>(input_header.getColumnsWithTypeAndName());

    auto build_function_node = [&](ActionsDAGPtr action_dag,
                                   const String & function_name,
                                   const String & result_name,
                                   const DB::ActionsDAG::NodeRawConstPtrs & args)
    {
        auto function_builder = DB::FunctionFactory::instance().get(function_name, getContext());
        return &action_dag->addFunction(function_builder, args, result_name);
    };

    bool need_projection = false;
    for (const auto & agg_info : aggregates)
    {
        // groupArray is used to implement collect_list in spark. But there is a difference between them.
        // If all elements are null, collect_list will return [], but groupArray will return null. And clickhosue
        // has backward compatibility issue, we cannot modify the inner implementation of groupArray
        if (agg_info.function_name == "groupArray" || agg_info.function_name == "groupUniqArray")
        {
            auto pos = agg_info.measure->measure().arguments(0).value().selection().direct_reference().struct_field().field();
            if (current_cols[pos].type->isNullable())
            {
                ActionsDAG::NodeRawConstPtrs args;
                args.push_back(&projection_action->findInOutputs(agg_info.arg_column_name));
                auto nested_type = typeid_cast<const DB::DataTypeNullable *>(current_cols[pos].type.get())->getNestedType();
                DB::Field empty_field = nested_type->getDefault();
                const auto * default_value_node = &projection_action->addColumn(
                    ColumnWithTypeAndName(nested_type->createColumnConst(1, empty_field), nested_type, getUniqueName("[]")));
                args.push_back(default_value_node);
                const auto * if_null_node = build_function_node(projection_action, "ifNull", current_cols[pos].name, args);
                projection_action->addOrReplaceInOutputs(*if_null_node);
                need_projection = true;
            }
        }
    }

    if (need_projection)
    {
        auto projection_step = std::make_unique<DB::ExpressionStep>(plan->getCurrentDataStream(), projection_action);
        projection_step->setStepDescription("Post-projection For Special Aggregate Functions");
        steps.emplace_back(projection_step.get());
        plan->addStep(std::move(projection_step));
    }
}

void AggregateRelParser::addPostProjectionForTypeMismatch()
{
    auto current_cols = plan->getCurrentDataStream().header.getColumnsWithTypeAndName();
    auto target_cols = current_cols;
    bool need_convert = false;
    for (const auto & agg_info : aggregates)
    {
        // For final stage, the argument refers to a intermediate resutl.
        auto pos = agg_info.measure->measure().arguments(0).value().selection().direct_reference().struct_field().field();
        auto output_type = parseType(agg_info.measure->measure().output_type());
        if (!output_type->equals(*current_cols[pos].type))
        {
            target_cols[pos].type = output_type;
            target_cols[pos].column = output_type->createColumn();
            need_convert = true;
        }
    }
    if (need_convert)
    {
        ActionsDAGPtr convert_action = ActionsDAG::makeConvertingActions(current_cols, target_cols, DB::ActionsDAG::MatchColumnsMode::Position);
        if (convert_action)
        {
            QueryPlanStepPtr convert_step = std::make_unique<ExpressionStep>(plan->getCurrentDataStream(), convert_action);
            convert_step->setStepDescription("Post-projection For Type Mismatch");
            steps.emplace_back(convert_step.get());
            plan->addStep(std::move(convert_step));
        }
    }
}

void registerAggregateParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_parser) { return std::make_shared<AggregateRelParser>(plan_parser); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kAggregate, builder);
}

}
