#include "AggregateRelParser.h"

#include <memory>
#include <AggregateFunctions/AggregateFunctionIf.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Common/StringUtils/StringUtils.h>

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
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "AggregateRelParser: multiple aggregation phases with final stage are not supported");

    auto input_header = plan->getCurrentDataStream().header;
    for (const auto & measure : aggregate_rel->measures())
    {
        const auto & agg_func = measure.measure();
        auto function_name = parseFunctionName(agg_func.function_reference(), agg_func);
        if (!function_name)
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported aggregate function");

        AggregateInfo agg_info;
        agg_info.function_name = *function_name;
        agg_info.measure.reset(measure.New());

        if (agg_info.function_name == "quantileGK" || agg_info.function_name == "quantilesGK")
        {
            /// Special setup for approx_percentile because:
            /// Spark approx_percentile(col, array(0.5, 0.4, 0.1), 100) => CH quantilesGK(100, 0.5. 0.4, 0.1)(col)
            /// Spark approx_percentile(col, 0.5, 100) => CH quantileGK(100, 0.5)(col)
            const auto & args = agg_func.arguments();
            const auto & col_arg = args[0].value();
            const auto & percentile_arg = args[1].value();
            const auto & accurary_arg = args[2].value();
            if (!accurary_arg.has_literal() || !percentile_arg.has_literal())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Spark function approx_percentile requires the second and third argument to be literal, but got {} and {}",
                    percentile_arg.ShortDebugString(),
                    accurary_arg.ShortDebugString());

            auto accuracy = parseLiteral(accurary_arg.literal());
            auto percentile = parseLiteral(percentile_arg.literal());
            std::cout << "function_name:" << agg_info.function_name << ", accuracy:" << toString(accuracy.second)
                      << ", percentile:" << toString(percentile.second) << std::endl;

            Array & params = agg_info.parameters;
            params.emplace_back(std::move(accuracy.second));
            if (function_name == "quantileGK")
                params.emplace_back(std::move(percentile.second));
            else
            {
                Array & percentile_array = percentile.second.safeGet<Array>();
                for (const auto & percentile_field : percentile_array)
                {
                    params.emplace_back(std::move(percentile_field));
                }
            }
            std::cout << "original params:" << toString(params) << std::endl;

            /// Remove the second and third column from the substrait arguments.
            agg_info.measure->CopyFrom(measure);
            agg_info.measure->mutable_measure()->mutable_arguments()->DeleteSubrange(1, 2);
        }
        else
            agg_info.measure->CopyFrom(measure);

        aggregates.emplace_back(std::move(agg_info));
    }

    if (aggregate_rel->groupings_size() == 1)
    {
        for (const auto & expr : aggregate_rel->groupings(0).grouping_expressions())
        {
            if (expr.has_selection() && expr.selection().has_direct_reference())
                grouping_keys.push_back(input_header.getByPosition(expr.selection().direct_reference().struct_field().field()).name);
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported group expression: {}", expr.DebugString());
        }
    }
    else if (aggregate_rel->groupings_size() != 0)
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupport multible groupings");
}

// projections for function arguments.
// 1. add literal columns
// 2. change argument columns' nullabitlity.
// 3. if the aggregate expression has filter, add a filter column.
void AggregateRelParser::addPreProjection()
{
    auto input_header = plan->getCurrentDataStream().header;
    bool need_projection = false;
    ActionsDAGPtr projection_action = std::make_shared<ActionsDAG>(input_header.getColumnsWithTypeAndName());
    for (auto & agg_info : aggregates)
    {
        for (const auto & arg : agg_info.measure->measure().arguments())
        {
            String arg_column_name;
            DB::DataTypePtr arg_column_type = nullptr;
            auto arg_value = arg.value();
            if (arg_value.has_selection())
            {
                const auto & col = input_header.getByPosition(arg_value.selection().direct_reference().struct_field().field());
                arg_column_name = col.name;
                arg_column_type = col.type;
            }
            else if (arg_value.has_literal())
            {
                const auto * node = parseArgument(projection_action, arg_value);
                projection_action->addOrReplaceInOutputs(*node);
                arg_column_name = node->result_name;
                arg_column_type = node->result_type;
                need_projection = true;
            }
            else
                throw Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unsupported aggregate argument: {}", arg_value.DebugString());

            // If the aggregate result is required to be nullable, make all inputs be nullable at the first stage.
            auto required_output_which_type = WhichDataType(parseType(agg_info.measure->measure().output_type()));
            if (required_output_which_type.isNullable()
                && agg_info.measure->measure().phase() == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
                && !projection_action->findInOutputs(arg_column_name).result_type->isNullable())
            {
                ActionsDAG::NodeRawConstPtrs args;
                agg_info.has_mismatch_nullablity = true;
                args.emplace_back(&projection_action->findInOutputs(arg_column_name));
                const auto * node = buildFunctionNode(projection_action, "toNullable", args);
                projection_action->addOrReplaceInOutputs(*node);
                arg_column_name = node->result_name;
                arg_column_type = node->result_type;
                need_projection = true;
            }

            agg_info.arg_column_names.emplace_back(arg_column_name);
            agg_info.arg_column_types.emplace_back(arg_column_type);
        }

        if (agg_info.measure->has_filter())
        {
            // With `If` combinator, the function take one more argument which refers to the condition.
            const auto * action_node = parseExpression(projection_action, agg_info.measure->filter());
            agg_info.filter_column_name = action_node->result_name;
            agg_info.arg_column_names.emplace_back(agg_info.filter_column_name);
            agg_info.arg_column_types.emplace_back(action_node->result_type);
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
        // The suffix of "PartialMerge" is used in AggregateFunctionCombinatorPartialMerge
        const auto * partial_merge_suffix = "PartialMerge";
        auto function_name = agg_info.function_name;
        auto arg_column_types = agg_info.arg_column_types;
        if (measure.phase() != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
        {
            if (agg_info.arg_column_types.size() != 1)
                throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Only support one argument aggregate function in phase {}", measure.phase());

            // Add a check here for safty.
            if (!agg_info.filter_column_name.empty())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unspport apply filter in phase {}", measure.phase());

            const auto * agg_function_data = DB::checkAndGetDataType<DB::DataTypeAggregateFunction>(agg_info.arg_column_types[0].get());
            if (!agg_function_data)
            {
                // FIXME. This is should be fixed. It's the case that count(distinct(xxx)) with other aggregate functions.
                // Gluten breaks the rule that intermediate result should have a special format name here.
                if (measure.phase() != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
                {
                    LOG_INFO(logger, "Intermediate aggregate function data is expected in phase {} for {}", measure.phase(), function_name);
                    auto arg_type = DB::removeNullable(agg_info.arg_column_types[0]);
                    if (auto * tupe_type = typeid_cast<const DB::DataTypeTuple*>(arg_type.get()))
                    {
                        arg_column_types = tupe_type->getElements();
                    }
                    auto agg_function = getAggregateFunction(function_name, arg_column_types, agg_info.parameters);
                    auto agg_intermediate_result_type = agg_function->getStateType();
                    arg_column_types = {agg_intermediate_result_type};
                }
            }
            else
            {
                // Special case for handling the intermedidate result from aggregate functions with filter.
                // It's safe to use AggregateFunctionxxx to parse intermediate result from AggregateFunctionxxxIf,
                // since they have the same binary representation
                // reproduce this case by
                //  select
                //    count(a),count(b), count(1), count(distinct(a)), count(distinct(b))
                //  from values (1, null), (2,2) as data(a,b)
                // with `first_value` enable
                if (measure.phase() != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
                {
                    if (endsWith(agg_function_data->getFunction()->getName(), "If")
                        && function_name != agg_function_data->getFunction()->getName())
                    {
                        auto original_args_types = agg_function_data->getArgumentsDataTypes();
                        arg_column_types = DataTypes(original_args_types.begin(), std::prev(original_args_types.end()));
                        auto agg_function = getAggregateFunction(function_name, arg_column_types, agg_info.parameters);
                        arg_column_types = {agg_function->getStateType()};
                    }
                }
            }

            function_name = function_name + partial_merge_suffix;
        }
        else if (!agg_info.filter_column_name.empty())
        {
            // Apply `If` aggregate function combinator on the original aggregate function.
            function_name += "If";
        }

        description.function = getAggregateFunction(function_name, arg_column_types, agg_info.parameters);
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
                args.push_back(&projection_action->findInOutputs(agg_info.arg_column_names[0]));
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
