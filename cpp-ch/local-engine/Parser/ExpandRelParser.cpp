#include "ExpandRelParser.h"
#include <Operator/ExpandStep.h>
#include <Parser/RelParser.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Processors/QueryPlan/ExpressionStep.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}
namespace local_engine
{

ExpandRelParser::ExpandRelParser(SerializedPlanParser * plan_parser_)
    : RelParser(plan_parser_)
{}

DB::QueryPlanPtr
ExpandRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel  & rel, std::list<const substrait::Rel*> & rel_stack)
{
    const auto & expand_rel = rel.expand();
    std::vector<size_t> aggregating_expressions_columns;
    std::set<size_t> agg_cols_ref;
    const auto & header = query_plan->getCurrentDataStream().header;
    for (int i = 0; i < expand_rel.aggregate_expressions_size(); ++i)
    {
        const auto & expr = expand_rel.aggregate_expressions(i);
        if (expr.has_selection())
        {
            aggregating_expressions_columns.push_back(expr.selection().direct_reference().struct_field().field());
            agg_cols_ref.insert(expr.selection().direct_reference().struct_field().field());
        }
        else
        {
            // FIXEME. see https://github.com/oap-project/gluten/pull/794
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Unsupported aggregating expression in expand node. {}. input header:{}.",
                expr.ShortDebugString(),
                header.dumpNames());
        }
    }
    std::vector<std::set<size_t>> grouping_sets;
    buildGroupingSets(expand_rel, grouping_sets);
    // The input header is : aggregating columns + grouping columns.
    auto expand_step = std::make_unique<ExpandStep>(
        query_plan->getCurrentDataStream(), aggregating_expressions_columns, grouping_sets, expand_rel.group_name());
    expand_step->setStepDescription("Expand step");
    query_plan->addStep(std::move(expand_step));
    return query_plan;
}


void ExpandRelParser::buildGroupingSets(const substrait::ExpandRel & expand_rel, std::vector<std::set<size_t>> & grouping_sets)
{
    for (int i = 0; i < expand_rel.groupings_size(); ++i)
    {
        const auto grouping_set_pb = expand_rel.groupings(i);
        std::set<size_t> grouping_set;
        for (int n = 0; n < grouping_set_pb.groupsets_expressions_size(); ++n)
        {
            const auto & expr = grouping_set_pb.groupsets_expressions(n);
            if (expr.has_selection())
            {
                grouping_set.insert(expr.selection().direct_reference().struct_field().field());
            }
            else
            {
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported expression in grouping sets");
            }
        }
        grouping_sets.emplace_back(std::move(grouping_set));
    }
}

void registerExpandRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_parser)
    {
        return std::make_shared<ExpandRelParser>(plan_parser);
    };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kExpand, builder);
}
}
