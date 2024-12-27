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

#include <Interpreters/ArrayJoin.h>
#include <Operator/EmptyProjectStep.h>
#include <Operator/ReplicateRowsStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Rewriter/ExpressionRewriter.h>
#include <Common/ArrayJoinHelper.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_block_size;
}
}
using namespace DB;

namespace local_engine
{
ProjectRelParser::ProjectRelParser(ParserContextPtr parser_context_) : RelParser(parser_context_)
{
}
DB::QueryPlanPtr
ProjectRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
{
    if (rel.has_project())
    {
        return parseProject(std::move(query_plan), rel, rel_stack_);
    }

    if (rel.has_generate())
    {
        return parseGenerate(std::move(query_plan), rel, rel_stack_);
    }

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "ProjectRelParser can't parse rel:{}", rel.ShortDebugString());
}

DB::QueryPlanPtr
ProjectRelParser::parseProject(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    ExpressionsRewriter rewriter(parser_context);
    substrait::Rel final_rel = rel;
    rewriter.rewrite(final_rel);
    const auto & project_rel = final_rel.project();
    if (project_rel.expressions_size())
    {
        std::vector<substrait::Expression> expressions;
        auto header = query_plan->getCurrentHeader();
        for (int i = 0; i < project_rel.expressions_size(); ++i)
        {
            expressions.emplace_back(project_rel.expressions(i));
        }
        auto actions_dag = expressionsToActionsDAG(expressions, header);
        auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentHeader(), std::move(actions_dag));
        expression_step->setStepDescription("Project");
        steps.emplace_back(expression_step.get());
        query_plan->addStep(std::move(expression_step));
        return query_plan;
    }
    else
    {
        auto empty_project_step = std::make_unique<EmptyProjectStep>(query_plan->getCurrentHeader());
        empty_project_step->setStepDescription("EmptyProject");
        steps.emplace_back(empty_project_step.get());
        query_plan->addStep(std::move(empty_project_step));
        return query_plan;
    }
}

bool ProjectRelParser::isReplicateRows(substrait::GenerateRel rel) const
{
    auto signature = expression_parser->getFunctionNameInSignature(rel.generator().scalar_function());
    return signature == "replicaterows";
}

DB::QueryPlanPtr ProjectRelParser::parseReplicateRows(DB::QueryPlanPtr query_plan, const substrait::GenerateRel & generate_rel)
{
    std::vector<substrait::Expression> expressions;
    for (int i = 0; i < generate_rel.generator().scalar_function().arguments_size(); ++i)
    {
        expressions.emplace_back(generate_rel.generator().scalar_function().arguments(i).value());
    }
    auto header = query_plan->getCurrentHeader();
    auto actions_dag = expressionsToActionsDAG(expressions, header);
    auto before_replicate_rows = std::make_unique<DB::ExpressionStep>(query_plan->getCurrentHeader(), std::move(actions_dag));
    before_replicate_rows->setStepDescription("Before ReplicateRows");
    steps.emplace_back(before_replicate_rows.get());
    query_plan->addStep(std::move(before_replicate_rows));

    auto replicate_rows_step = std::make_unique<ReplicateRowsStep>(query_plan->getCurrentHeader());
    replicate_rows_step->setStepDescription("ReplicateRows");
    steps.emplace_back(replicate_rows_step.get());
    query_plan->addStep(std::move(replicate_rows_step));
    return query_plan;
}

DB::QueryPlanPtr
ProjectRelParser::parseGenerate(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    const auto & generate_rel = rel.generate();
    if (isReplicateRows(generate_rel))
    {
        return parseReplicateRows(std::move(query_plan), generate_rel);
    }
    std::vector<substrait::Expression> expressions;
    for (int i = 0; i < generate_rel.child_output_size(); ++i)
    {
        expressions.emplace_back(generate_rel.child_output(i));
    }

    expressions.emplace_back(generate_rel.generator());
    auto header = query_plan->getCurrentHeader();
    auto actions_dag = expressionsToActionsDAG(expressions, header);

    if (!ArrayJoinHelper::findArrayJoinNode(actions_dag))
    {
        /// If generator in generate rel is not explode/posexplode, e.g. json_tuple
        auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentHeader(), std::move(actions_dag));
        expression_step->setStepDescription("Generate");
        steps.emplace_back(expression_step.get());
        query_plan->addStep(std::move(expression_step));
    }
    else
    {
        auto new_steps = ArrayJoinHelper::addArrayJoinStep(getContext(), *query_plan, actions_dag, generate_rel.outer());
        steps.insert(steps.end(), new_steps.begin(), new_steps.end());
    }

    return query_plan;
}

void registerProjectRelParser(RelParserFactory & factory)
{
    auto builder = [](ParserContextPtr parser_context_) -> std::unique_ptr<RelParser>
    { return std::make_unique<ProjectRelParser>(parser_context_); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kProject, builder);
    factory.registerBuilder(substrait::Rel::RelTypeCase::kGenerate, builder);
}
}
