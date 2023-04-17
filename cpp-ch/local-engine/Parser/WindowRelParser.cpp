#include "WindowRelParser.h"
#include <exception>
#include <memory>
#include <valarray>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/WindowDescription.h>
#include <Parser/RelParser.h>
#include <Parser/SortRelParser.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Common/logger_useful.h>
#include <base/sort.h>
#include <base/types.h>
#include <google/protobuf/util/json_util.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Interpreters/ActionsDAG.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}
}
namespace local_engine
{

WindowRelParser::WindowRelParser(SerializedPlanParser * plan_paser_) : RelParser(plan_paser_)
{
}

DB::QueryPlanPtr
WindowRelParser::parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    // rel_stack = rel_stack_;
    const auto & win_rel_pb = rel.window();
    current_plan = std::move(current_plan_);
    auto expected_header = current_plan->getCurrentDataStream().header;
    for (const auto & measure : win_rel_pb.measures())
    {
        const auto & win_function = measure.measure();
        ColumnWithTypeAndName named_col;
        named_col.name = win_function.column_name();
        named_col.type = parseType(win_function.output_type());
        named_col.column = named_col.type->createColumn();
        expected_header.insert(named_col);
    }
    tryAddProjectionBeforeWindow(*current_plan, win_rel_pb);

    auto window_descriptions = parseWindowDescriptions(win_rel_pb);

    /// In spark plan, there is already a sort step before each window, so we don't need to add sort steps here.
    for (auto & it : window_descriptions)
    {
        auto & win = it.second;
        ;
        auto window_step = std::make_unique<DB::WindowStep>(current_plan->getCurrentDataStream(), win, win.window_functions);
        window_step->setStepDescription("Window step for window '" + win.window_name + "'");
        current_plan->addStep(std::move(window_step));
    }


    auto current_header = current_plan->getCurrentDataStream().header;
    if (!DB::blocksHaveEqualStructure(expected_header, current_header))
    {
        ActionsDAGPtr convert_action = ActionsDAG::makeConvertingActions(
            current_header.getColumnsWithTypeAndName(),
            expected_header.getColumnsWithTypeAndName(),
            DB::ActionsDAG::MatchColumnsMode::Name);
        QueryPlanStepPtr convert_step = std::make_unique<DB::ExpressionStep>(current_plan->getCurrentDataStream(), convert_action);
        convert_step->setStepDescription("Convert window Output");
        current_plan->addStep(std::move(convert_step));
    }

    return std::move(current_plan);
}
DB::WindowDescription
WindowRelParser::parseWindowDescrption(const substrait::WindowRel & win_rel, const substrait::Expression::WindowFunction & win_function)
{
    DB::WindowDescription win_descr;
    win_descr.frame = parseWindowFrame(win_function);
    win_descr.partition_by = parsePartitionBy(win_rel.partition_expressions());
    win_descr.order_by = SortRelParser::parseSortDescription(win_rel.sorts(), current_plan->getCurrentDataStream().header);
    win_descr.full_sort_description = win_descr.partition_by;
    win_descr.full_sort_description.insert(win_descr.full_sort_description.end(), win_descr.order_by.begin(), win_descr.order_by.end());

    DB::WriteBufferFromOwnString ss;
    ss << "partition by " << DB::dumpSortDescription(win_descr.partition_by);
    ss << " order by " << DB::dumpSortDescription(win_descr.order_by);
    ss << " " << win_descr.frame.toString();
    win_descr.window_name = ss.str();
    return win_descr;
}

std::unordered_map<DB::String, WindowDescription> WindowRelParser::parseWindowDescriptions(const substrait::WindowRel & win_rel)
{
    std::unordered_map<DB::String, WindowDescription> window_descriptions;
    for (int i = 0; i < win_rel.measures_size(); ++i)
    {
        const auto & measure = win_rel.measures(i);
        const auto & win_function = measure.measure();
        auto win_descr = parseWindowDescrption(win_rel, win_function);
        WindowDescription * description = nullptr;
        const auto win_it = window_descriptions.find(win_descr.window_name);
        if (win_it != window_descriptions.end())
            description = &win_it->second;
        else
        {
            window_descriptions[win_descr.window_name] = win_descr;
            description = &window_descriptions[win_descr.window_name];
        }
        auto win_func = parseWindowFunctionDescription(win_rel, win_function, measures_arg_names[i], measures_arg_types[i]);
        description->window_functions.emplace_back(win_func);
    }
    return window_descriptions;
}

DB::WindowFrame WindowRelParser::parseWindowFrame(const substrait::Expression::WindowFunction & window_function)
{
    auto function_name = parseFunctionName(window_function.function_reference());
    if (!function_name)
        function_name = "";
    DB::WindowFrame win_frame;
    win_frame.type = parseWindowFrameType(*function_name, window_function);
    parseBoundType(
        *function_name, window_function.lower_bound(), true, win_frame.begin_type, win_frame.begin_offset, win_frame.begin_preceding);
    parseBoundType(*function_name, window_function.upper_bound(), false, win_frame.end_type, win_frame.end_offset, win_frame.end_preceding);

    // special cases
    if (*function_name == "lead" || *function_name == "lag")
    {
        win_frame.begin_preceding = true;
        win_frame.end_preceding = false;
    }
    return win_frame;
}

DB::WindowFrame::FrameType WindowRelParser::parseWindowFrameType(const std::string & function_name, const substrait::Expression::WindowFunction & window_function)
{
    // It's weird! The frame type only could be rows in spark for rank(). But in clickhouse
    // it's should be range. If run rank() over rows frame, the result is different. The rank number
    // is different for the same values.
    static const std::unordered_map<std::string, substrait::WindowType> special_function_frame_type
        = {
            {"rank", substrait::RANGE},
            {"dense_rank", substrait::RANGE},
        };

    substrait::WindowType frame_type;
    auto iter = special_function_frame_type.find(function_name);
    if (iter != special_function_frame_type.end())
        frame_type = iter->second;
    else
        frame_type = window_function.window_type();

    if (frame_type == substrait::ROWS)
    {
        return DB::WindowFrame::FrameType::ROWS;
    }
    else if (frame_type == substrait::RANGE)
    {
        return DB::WindowFrame::FrameType::RANGE;
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unknow window frame type:{}", frame_type);
    }
}

void WindowRelParser::parseBoundType(
    const std::string & function_name,
    const substrait::Expression::WindowFunction::Bound & bound,
    bool is_begin_or_end,
    DB::WindowFrame::BoundaryType & bound_type,
    Field & offset,
    bool & preceding_direction)
{
    /// some default settings.
    offset = 0;

    if (bound.has_preceding())
    {
        const auto & preceding = bound.preceding();
        bound_type = DB::WindowFrame::BoundaryType::Offset;
        preceding_direction = preceding.offset() >= 0;
        if (preceding.offset() < 0)
        {
            offset = 0 - preceding.offset();
        }
        else
        {
            offset = preceding.offset();
        }
    }
    else if (bound.has_following())
    {
        const auto & following = bound.following();
        bound_type = DB::WindowFrame::BoundaryType::Offset;
        preceding_direction = following.offset() < 0;
        if (following.offset() < 0)
        {
            offset = 0 - following.offset();
        }
        else
        {
            offset = following.offset();
        }
    }
    else if (bound.has_current_row())
    {
        const auto & current_row = bound.current_row();
        bound_type = DB::WindowFrame::BoundaryType::Current;
        offset = 0;
        preceding_direction = is_begin_or_end;
    }
    else if (bound.has_unbounded_preceding())
    {
        bound_type = DB::WindowFrame::BoundaryType::Unbounded;
        offset = 0;
        preceding_direction = true;
    }
    else if (bound.has_unbounded_following())
    {
        bound_type = DB::WindowFrame::BoundaryType::Unbounded;
        offset = 0;
        preceding_direction = false;
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unknown bound type:{}", bound.DebugString());
    }
}


DB::SortDescription WindowRelParser::parsePartitionBy(const google::protobuf::RepeatedPtrField<substrait::Expression> & expressions)
{
    DB::Block header = current_plan->getCurrentDataStream().header;
    DB::SortDescription sort_descr;
    for (const auto & expr : expressions)
    {
        if (!expr.has_selection())
        {
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Column reference is expected.");
        }
        auto pos = expr.selection().direct_reference().struct_field().field();
        auto col_name = header.getByPosition(pos).name;
        sort_descr.push_back(DB::SortColumnDescription(col_name, 1, 1));
    }
    return sort_descr;
}

WindowFunctionDescription WindowRelParser::parseWindowFunctionDescription(
    const substrait::WindowRel & win_rel,
    const substrait::Expression::WindowFunction & window_function,
    const DB::Names & arg_names,
    const DB::DataTypes & arg_types)
{
    auto header = current_plan->getCurrentDataStream().header;
    WindowFunctionDescription description;
    description.column_name = window_function.column_name();
    description.function_node = nullptr;

    auto function_name = parseFunctionName(window_function.function_reference());
    if (!function_name)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found function for reference: {}", window_function.function_reference());

    DB::AggregateFunctionProperties agg_function_props;
    // Special transform for lead/lag
    if (*function_name == "lead" || *function_name == "lag")
    {
        if (*function_name == "lead")
            function_name = "leadInFrame";
        else
            function_name = "lagInFrame";
        auto agg_function_ptr = getAggregateFunction(*function_name, arg_types, agg_function_props);

        description.argument_names = arg_names;
        description.argument_types = arg_types;
        description.aggregate_function = agg_function_ptr;
    }
    else
    {
        auto agg_function_ptr = getAggregateFunction(*function_name, arg_types, agg_function_props);

        description.argument_names = arg_names;
        description.argument_types = arg_types;
        description.aggregate_function = agg_function_ptr;
    }

    return description;
}

void WindowRelParser::tryAddProjectionBeforeWindow(
    QueryPlan & plan, const substrait::WindowRel & win_rel)
{
    auto header = plan.getCurrentDataStream().header;
    ActionsDAGPtr actions_dag = std::make_shared<ActionsDAG>(header.getColumnsWithTypeAndName());
    bool need_project = false;
    for (const auto & measure : win_rel.measures())
    {
        DB::Names names;
        DB::DataTypes types;
        auto function_name = parseFunctionName(measure.measure().function_reference());
        if (function_name && (*function_name == "lead" || *function_name == "lag"))
        {
            const auto & arg0 = measure.measure().arguments(0).value();
            const auto & col = header.getByPosition(arg0.selection().direct_reference().struct_field().field());
            names.emplace_back(col.name);
            types.emplace_back(col.type);

            auto arg1 = measure.measure().arguments(1).value();
            const DB::ActionsDAG::Node * node = nullptr;
            // lag's offset is negative
            if (*function_name == "lag")
            {
                auto literal_result = parseLiteral(arg1.literal());
                assert(literal_result.second.safeGet<DB::Int32>() < 0);
                auto real_field = 0 - literal_result.second.safeGet<DB::Int32>();
                node = &actions_dag->addColumn(ColumnWithTypeAndName(
                    literal_result.first->createColumnConst(1, real_field), literal_result.first, getUniqueName(toString(real_field))));
            }
            else
            {
                node = parseArgument(actions_dag, arg1);
            }
            node = ActionsDAGUtil::convertNodeType(actions_dag, node, DataTypeInt64().getName());
            actions_dag->addOrReplaceInOutputs(*node);
            names.emplace_back(node->result_name);
            types.emplace_back(node->result_type);

            const auto & arg2 = measure.measure().arguments(2).value();
            if (arg2.has_literal() && !arg2.literal().has_null())
            {
                node = parseArgument(actions_dag, arg2);
                actions_dag->addOrReplaceInOutputs(*node);
                names.emplace_back(node->result_name);
                types.emplace_back(node->result_type);
            }
            need_project = true;
        }
        else
        {
            for (int i = 0, n = measure.measure().arguments().size(); i < n; ++i)
            {
                const auto & arg = measure.measure().arguments(i).value();
                if (arg.has_selection())
                {
                    const auto & col = header.getByPosition(arg.selection().direct_reference().struct_field().field());
                    names.push_back(col.name);
                    types.emplace_back(col.type);
                }
                else if (arg.has_literal())
                {
                    // for example, sum(2) over(...), we need to add new const column for 2, otherwise
                    // an exception of not found column(2) will throw.
                    const auto * node = parseArgument(actions_dag, arg);
                    names.push_back(node->result_name);
                    types.emplace_back(node->result_type);
                    actions_dag->addOrReplaceInOutputs(*node);
                    need_project = true;
                }
                else
                {
                    // There should be a projections ahead to eliminate complex expressions.
                    throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported aggregate argument type {}.", arg.DebugString());
                }
            }
        }
        measures_arg_names.emplace_back(std::move(names));
        measures_arg_types.emplace_back(std::move(types));
    }
    if (need_project)
    {
        auto project_step = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), actions_dag);
        project_step->setStepDescription("Add projections before aggregation");
        plan.addStep(std::move(project_step));
    }
}


void registerWindowRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_paser) { return std::make_shared<WindowRelParser>(plan_paser); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kWindow, builder);

}
}
