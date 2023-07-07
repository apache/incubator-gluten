#include "ProjectRelParser.h"
#include <Operator/EmptyProjectStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
namespace local_engine
{
ProjectRelParser::ProjectRelParser(SerializedPlanParser * plan_paser_)
    : RelParser(plan_paser_)
{
}
DB::QueryPlanPtr
ProjectRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    if (rel.has_project())
    {
        return parseProject(std::move(query_plan), rel, rel_stack_);
    }
    return parseGenerate(std::move(query_plan), rel, rel_stack_);
}

DB::QueryPlanPtr
ProjectRelParser::parseProject(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    const auto & project_rel = rel.project();
    if (project_rel.expressions_size())
    {
        std::vector<substrait::Expression> expressions;
        auto header = query_plan->getCurrentDataStream().header;
        for (int i = 0; i < project_rel.expressions_size(); ++i)
        {
            expressions.emplace_back(project_rel.expressions(i));
        }
        auto actions_dag = expressionsToActionsDAG(expressions, header);
        auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), actions_dag);
        expression_step->setStepDescription("Project");
        steps.emplace_back(expression_step.get());
        query_plan->addStep(std::move(expression_step));
        return query_plan;
    }
    else
    {
        auto empty_project_step = std::make_unique<EmptyProjectStep>(query_plan->getCurrentDataStream());
        empty_project_step->setStepDescription("EmptyProject");
        steps.emplace_back(empty_project_step.get());
        query_plan->addStep(std::move(empty_project_step));
        return query_plan;
    }
}

DB::QueryPlanPtr
ProjectRelParser::parseGenerate(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    const auto & generate_rel = rel.generate();
    std::vector<substrait::Expression> expressions;
    for(int i = 0; i < generate_rel.child_output_size(); ++i)
    {
        expressions.emplace_back(generate_rel.child_output(i));
    }
    expressions.emplace_back(generate_rel.generator());
    auto header = query_plan->getCurrentDataStream().header;
    auto actions_dag = expressionsToActionsDAG(expressions, header);
    auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), actions_dag);
    expression_step->setStepDescription("Generate");
    steps.emplace_back(expression_step.get());
    query_plan->addStep(std::move(expression_step));
    return query_plan;
}

void registerProjectRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_parser) -> std::unique_ptr<RelParser> {
        return std::make_unique<ProjectRelParser>(plan_parser);
    };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kProject, builder);
}
}
