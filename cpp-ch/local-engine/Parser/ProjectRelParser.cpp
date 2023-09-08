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
#include "ProjectRelParser.h"
#include <Operator/EmptyProjectStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Rewriter/ExpressionRewriter.h>
namespace local_engine
{
ProjectRelParser::ProjectRelParser(SerializedPlanParser * plan_paser_) : RelParser(plan_paser_)
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
    ExpressionsRewriter rewriter(getPlanParser());
    substrait::Rel final_rel = rel;
    rewriter.rewrite(final_rel);
    const auto & project_rel = final_rel.project();
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
    for (int i = 0; i < generate_rel.child_output_size(); ++i)
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
    auto builder
        = [](SerializedPlanParser * plan_parser) -> std::unique_ptr<RelParser> { return std::make_unique<ProjectRelParser>(plan_parser); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kProject, builder);
    factory.registerBuilder(substrait::Rel::RelTypeCase::kGenerate, builder);
}
}
