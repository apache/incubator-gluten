#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/aggregate_function_parser/CommonAggregateFunctionParser.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_FUNCTION;
}
}

namespace local_engine
{

DB::ActionsDAG::NodeRawConstPtrs BaseAggregateFunctionParser::parseFunctionArguments(
    const CommonFunctionInfo & func_info, const String &, DB::ActionsDAGPtr & actions_dag) const
{
    DB::ActionsDAG::NodeRawConstPtrs collected_args;
    for (const auto & arg : func_info.arguments)
    {
        auto arg_value = arg.value();
        const DB::ActionsDAG::Node * arg_node = parseExpression(actions_dag, arg_value);

        // If the aggregate result is required to be nullable, make all inputs be nullable at the first stage.
        auto required_output_type = DB::WhichDataType(SerializedPlanParser::parseType(func_info.output_type));
        if (required_output_type.isNullable()
            && func_info.phase == substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
            && !arg_node->result_type->isNullable())
        {
            ActionsDAG::NodeRawConstPtrs args;
            args.emplace_back(arg_node);
            const auto * node = toFunctionNode(actions_dag, "toNullable", args);
            actions_dag->addOrReplaceInOutputs(*node);
            arg_node = node;
        }

        collected_args.push_back(arg_node);
    }
    if (func_info.has_filter)
    {
        // With `If` combinator, the function take one more argument which refers to the condition.
        const auto * action_node = parseExpression(actions_dag, func_info.filter);
        collected_args.emplace_back(action_node);
    }
    return collected_args;
}

const DB::ActionsDAG::Node * BaseAggregateFunctionParser::convertNodeTypeIfNeeded(
    const CommonFunctionInfo & func_info, const DB::ActionsDAG::Node * func_node, DB::ActionsDAGPtr & actions_dag) const
{
    auto ret_node = func_node;
    auto output_type = SerializedPlanParser::parseType(func_info.output_type);
    if (!output_type->equals(*func_node->result_type))
    {
        ret_node = ActionsDAGUtil::convertNodeType(actions_dag, func_node, output_type->getName(), func_node->result_name);
        actions_dag->addOrReplaceInOutputs(*ret_node);
    }
    return ret_node;
}

std::pair<String, DB::DataTypes> BaseAggregateFunctionParser::tryApplyCHCombinator(
    const CommonFunctionInfo & func_info, const String & ch_func_name, const DB::DataTypes & arg_column_types) const
{
    auto get_aggregate_function = [](const String & name, const DB::DataTypes & arg_types) -> DB::AggregateFunctionPtr
    {
        DB::AggregateFunctionProperties properties;
        auto func = DB::AggregateFunctionFactory::instance().get(name, arg_types, DB::Array{}, properties);
        if (!func)
        {
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown aggregate function {}", name);
        }
        return func;
    };
    String combinator_function_name = ch_func_name;
    DB::DataTypes combinator_arg_column_types = arg_column_types;
    if (func_info.phase != substrait::AggregationPhase::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
    {
        if (arg_column_types.size() != 1)
        {
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Only support one argument aggregate function in phase {}", func_info.phase);
        }
        // Add a check here for safty.
        if (func_info.has_filter)
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unspport apply filter in phase {}", func_info.phase);
        }

        const auto * agg_function_data = DB::checkAndGetDataType<DB::DataTypeAggregateFunction>(arg_column_types[0].get());
        if (!agg_function_data)
        {
            // FIXME. This is should be fixed. It's the case that count(distinct(xxx)) with other aggregate functions.
            // Gluten breaks the rule that intermediate result should have a special format name here.
            LOG_INFO(logger, "Intermediate aggregate function data is expected in phase {} for {}", func_info.phase, ch_func_name);
            auto arg_type = DB::removeNullable(arg_column_types[0]);
            if (auto * tupe_type = typeid_cast<const DB::DataTypeTuple *>(arg_type.get()))
            {
                combinator_arg_column_types = tupe_type->getElements();
            }
            auto agg_function = get_aggregate_function(ch_func_name, arg_column_types);
            auto agg_intermediate_result_type = agg_function->getStateType();
            combinator_arg_column_types = {agg_intermediate_result_type};
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
            if (endsWith(agg_function_data->getFunction()->getName(), "If") && ch_func_name != agg_function_data->getFunction()->getName())
            {
                auto original_args_types = agg_function_data->getArgumentsDataTypes();
                combinator_arg_column_types = DataTypes(original_args_types.begin(), std::prev(original_args_types.end()));
                auto agg_function = get_aggregate_function(ch_func_name, combinator_arg_column_types);
                combinator_arg_column_types = {agg_function->getStateType()};
            }
        }
        combinator_function_name += "PartialMerge";
    }
    else if (func_info.has_filter)
    {
        // Apply `If` aggregate function combinator on the original aggregate function.
        combinator_function_name += "If";
    }
    return {combinator_function_name, combinator_arg_column_types};
}

#define REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(cls_name, substait_name, ch_name) \
    class AggregateFunctionParser##cls_name : public BaseAggregateFunctionParser \
    { \
    public: \
        AggregateFunctionParser##cls_name(SerializedPlanParser * plan_parser_) : BaseAggregateFunctionParser(plan_parser_) \
        { \
        } \
        ~AggregateFunctionParser##cls_name() override = default; \
        String getName() const override { return  #substait_name; } \
        static constexpr auto name = #substait_name; \
        String getCHFunctionName(const CommonFunctionInfo &) const override { return #ch_name; } \
        String getCHFunctionName(const DB::DataTypes &) const override { return #ch_name; } \
    }; \
    static const FunctionParserRegister<AggregateFunctionParser##cls_name> register_##cls_name = FunctionParserRegister<AggregateFunctionParser##cls_name>();

REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Sum, sum, sum)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Avg, avg, avg)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Min, min, min)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Max, max, max)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(StdDev, stddev, stddev_samp)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(StdDevSamp, stddev_samp, stddev_samp)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(StdDevPop, stddev_pop, stddev_pop)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(BitAnd, bit_and, groupBitAnd)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(BitOr, bit_or, groupBitOr)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(BitXor, bit_xor, groupBitXor)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(CovarPop, covar_pop, covarPop)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(CovarSamp, covar_samp, covarSamp)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(VarSamp, var_samp, varSamp)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(VarPop, var_pop, varPop)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Corr, corr, corr)

REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(First, first, first_value_respect_nulls)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(FirstIgnoreNull, first_ignore_null, first_value)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Last, last, last_value_respect_nulls)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(LastIgnoreNull, last_ignore_null, last_value)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(DenseRank, dense_rank, dense_rank)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Rank, rank, rank)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(RowNumber, row_number, row_number)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(Ntile, ntile, ntile)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(PercentRank, percent_rank, percent_rank)
REGISTER_COMMON_AGGREGATE_FUNCTION_PARSER(CumeDist, cume_dist, cume_dist)
}
