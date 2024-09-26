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

    if (rel.has_generate())
    {
        return parseGenerate(std::move(query_plan), rel, rel_stack_);
    }

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "ProjectRelParser can't parse rel:{}", rel.ShortDebugString());
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
        auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), std::move(actions_dag));
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

const DB::ActionsDAG::Node * ProjectRelParser::findArrayJoinNode(const ActionsDAG& actions_dag)
{
    const ActionsDAG::Node * array_join_node = nullptr;
    const auto & nodes = actions_dag.getNodes();
    for (const auto & node : nodes)
    {
        if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
        {
            if (array_join_node)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expect single ARRAY JOIN node in generate rel");
            array_join_node = &node;
        }
    }
    return array_join_node;
}

ProjectRelParser::SplittedActionsDAGs ProjectRelParser::splitActionsDAGInGenerate(const ActionsDAG& actions_dag)
{
    SplittedActionsDAGs res;

    auto array_join_node = findArrayJoinNode(actions_dag);
    std::unordered_set<const ActionsDAG::Node *> first_split_nodes(array_join_node->children.begin(), array_join_node->children.end());
    auto first_split_result = actions_dag.split(first_split_nodes);
    res.before_array_join = std::move(first_split_result.first);

    array_join_node = findArrayJoinNode(first_split_result.second);
    std::unordered_set<const ActionsDAG::Node *> second_split_nodes = {array_join_node};
    auto second_split_result = first_split_result.second.split(second_split_nodes);
    res.array_join = std::move(second_split_result.first);
    second_split_result.second.removeUnusedActions();
    res.after_array_join = std::move(second_split_result.second);
    return res;
}

bool ProjectRelParser::isReplicateRows(substrait::GenerateRel rel)
{
    return plan_parser->isFunction(rel.generator().scalar_function(), "replicaterows");
}

DB::QueryPlanPtr ProjectRelParser::parseReplicateRows(DB::QueryPlanPtr query_plan, substrait::GenerateRel generate_rel)
{
    std::vector<substrait::Expression> expressions;
    for (int i = 0; i < generate_rel.generator().scalar_function().arguments_size(); ++i)
    {
        expressions.emplace_back(generate_rel.generator().scalar_function().arguments(i).value());
    }
    auto header = query_plan->getCurrentDataStream().header;
    auto actions_dag = expressionsToActionsDAG(expressions, header);
    auto before_replicate_rows = std::make_unique<DB::ExpressionStep>(query_plan->getCurrentDataStream(), std::move(actions_dag));
    before_replicate_rows->setStepDescription("Before ReplicateRows");
    steps.emplace_back(before_replicate_rows.get());
    query_plan->addStep(std::move(before_replicate_rows));

    auto replicate_rows_step = std::make_unique<ReplicateRowsStep>(query_plan->getCurrentDataStream());
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
    auto header = query_plan->getCurrentDataStream().header;
    auto actions_dag = expressionsToActionsDAG(expressions, header);

    if (!findArrayJoinNode(actions_dag))
    {
        /// If generator in generate rel is not explode/posexplode, e.g. json_tuple
        auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), std::move(actions_dag));
        expression_step->setStepDescription("Generate");
        steps.emplace_back(expression_step.get());
        query_plan->addStep(std::move(expression_step));
    }
    else
    {
        /// If generator in generate rel is explode/posexplode, transform arrayJoin function to ARRAY JOIN STEP to apply max_block_size
        /// which avoids OOM when several lateral view explode/posexplode is used in spark sqls
        LOG_DEBUG(logger, "original actions_dag:{}", actions_dag.dumpDAG());
        auto splitted_actions_dags = splitActionsDAGInGenerate(actions_dag);
        LOG_DEBUG(logger, "actions_dag before arrayJoin:{}", splitted_actions_dags.before_array_join.dumpDAG());
        LOG_DEBUG(logger, "actions_dag during arrayJoin:{}", splitted_actions_dags.array_join.dumpDAG());
        LOG_DEBUG(logger, "actions_dag after arrayJoin:{}", splitted_actions_dags.after_array_join.dumpDAG());

        auto ignore_actions_dag = [](const ActionsDAG& actions_dag_) -> bool
        {
            /*
            We should ignore actions_dag like:
            0 : INPUT () (no column) String a
            1 : INPUT () (no column) String b
            Output nodes: 0, 1
             */
            return actions_dag_.getOutputs().size() == actions_dag_.getNodes().size()
                && actions_dag_.getInputs().size() == actions_dag_.getNodes().size();
        };

        /// Pre-projection before array join
        if (!ignore_actions_dag(splitted_actions_dags.before_array_join))
        {
            auto step_before_array_join
                = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), std::move(splitted_actions_dags.before_array_join));
            step_before_array_join->setStepDescription("Pre-projection In Generate");
            steps.emplace_back(step_before_array_join.get());
            query_plan->addStep(std::move(step_before_array_join));
            // LOG_DEBUG(logger, "plan1:{}", PlanUtil::explainPlan(*query_plan));
        }

        /// ARRAY JOIN
        Names array_joined_columns{findArrayJoinNode(splitted_actions_dags.array_join)->result_name};
        ArrayJoin array_join;
        array_join.columns = std::move(array_joined_columns);
        array_join.is_left = generate_rel.outer();
        auto array_join_step = std::make_unique<ArrayJoinStep>(
            query_plan->getCurrentDataStream(), std::move(array_join), false, getContext()->getSettingsRef()[Setting::max_block_size]);
        array_join_step->setStepDescription("ARRAY JOIN In Generate");
        steps.emplace_back(array_join_step.get());
        query_plan->addStep(std::move(array_join_step));
        // LOG_DEBUG(logger, "plan2:{}", PlanUtil::explainPlan(*query_plan));

        /// Post-projection after array join(Optional)
        if (!ignore_actions_dag(splitted_actions_dags.after_array_join))
        {
            auto step_after_array_join = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), std::move(splitted_actions_dags.after_array_join));
            step_after_array_join->setStepDescription("Post-projection In Generate");
            steps.emplace_back(step_after_array_join.get());
            query_plan->addStep(std::move(step_after_array_join));
            // LOG_DEBUG(logger, "plan3:{}", PlanUtil::explainPlan(*query_plan));
        }
    }

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
