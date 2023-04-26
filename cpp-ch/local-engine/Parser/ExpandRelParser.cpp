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
#include <vector>
#include <Parser/ExpandField.h>

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

void updateType(DB::DataTypePtr & type, const DB::DataTypePtr & new_type)
{
    if (type == nullptr || (!type->isNullable() && new_type->isNullable()))
    {
        type = new_type;
    }
}

DB::QueryPlanPtr
ExpandRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel*> & rel_stack)
{
    const auto & expand_rel = rel.expand();
    const auto & header = query_plan->getCurrentDataStream().header;

    std::vector<std::vector<ExpandFieldKind>> expand_kinds;
    std::vector<std::vector<DB::Field>> expand_fields;
    std::vector<DB::DataTypePtr> types;
    std::vector<std::string> names;
    std::set<String> distinct_names;

    expand_kinds.reserve(expand_rel.fields_size());
    expand_fields.reserve(expand_rel.fields_size());

    for (const auto & projections: expand_rel.fields())
    {
        auto expand_col_size = projections.switching_field().duplicates_size();
    
        std::vector<ExpandFieldKind> kinds;
        std::vector<DB::Field> fields;

        kinds.reserve(expand_col_size);
        fields.reserve(expand_col_size);

        if (types.empty()) types.resize(expand_col_size, nullptr);
        if (names.empty()) names.resize(expand_col_size);

        for (int i = 0; i < expand_col_size; ++i)
        {
            const auto & project_expr = projections.switching_field().duplicates(i);
            if (project_expr.has_selection())
            {
                auto field = project_expr.selection().direct_reference().struct_field().field();
                kinds.push_back(ExpandFieldKind::EXPAND_FIELD_KIND_SELECTION);
                fields.push_back(field);
                updateType(types[i], header.getByPosition(field).type);
                const auto & name = header.getByPosition(field).name;
                if (names[i].empty())
                {
                    if (distinct_names.contains(name))
                    {
                        auto unique_name = getUniqueName(name);
                        distinct_names.emplace(unique_name);
                        names[i] = unique_name;
                    }
                    else
                    {
                        distinct_names.emplace(name);
                        names[i] = name;
                    }
                }
            }
            else if (project_expr.has_literal())
            {
                auto [type, field] = parseLiteral(project_expr.literal());
                kinds.push_back(ExpandFieldKind::EXPAND_FIELD_KIND_LITERAL);
                fields.push_back(field);
                updateType(types[i], type);
            }
            else
            {
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported expression in projections");
            }
        }
        expand_kinds.push_back(std::move(kinds));
        expand_fields.push_back(std::move(fields));
    }

    ExpandField expand_field(names, types, expand_kinds, expand_fields);
    auto expand_step = std::make_unique<ExpandStep>(query_plan->getCurrentDataStream(), std::move(expand_field));
    expand_step->setStepDescription("Expand Step");
    query_plan->addStep(std::move(expand_step));
    return query_plan;
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
