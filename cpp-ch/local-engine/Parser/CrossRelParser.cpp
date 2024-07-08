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
#include "CrossRelParser.h"

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Join/BroadCastJoinBuilder.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Parser/SerializedPlanParser.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <google/protobuf/wrappers.pb.h>
#include <Common/CHUtil.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int BAD_ARGUMENTS;
}
}

using namespace DB;




namespace local_engine
{
String parseCrossJoinOptimizationInfos(const substrait::CrossRel & join)
{
    google::protobuf::StringValue optimization;
    optimization.ParseFromString(join.advanced_extension().optimization().value());
    String storage_join_key;
    ReadBufferFromString in(optimization.value());
    assertString("JoinParameters:", in);
    assertString("buildHashTableId=", in);
    readString(storage_join_key, in);
    return storage_join_key;
}

std::shared_ptr<DB::TableJoin> createCrossTableJoin(substrait::CrossRel_JoinType join_type)
{
    auto & global_context = SerializedPlanParser::global_context;
    auto table_join = std::make_shared<TableJoin>(
        global_context->getSettings(), global_context->getGlobalTemporaryVolume(), global_context->getTempDataOnDisk());

    std::pair<DB::JoinKind, DB::JoinStrictness> kind_and_strictness = JoinUtil::getCrossJoinKindAndStrictness(join_type);
    table_join->setKind(kind_and_strictness.first);
    table_join->setStrictness(kind_and_strictness.second);
    return table_join;
}

CrossRelParser::CrossRelParser(SerializedPlanParser * plan_paser_)
    : RelParser(plan_paser_)
    , function_mapping(plan_paser_->function_mapping)
    , context(plan_paser_->context)
    , extra_plan_holder(plan_paser_->extra_plan_holder)
{
}

DB::QueryPlanPtr
CrossRelParser::parse(DB::QueryPlanPtr /*query_plan*/, const substrait::Rel & /*rel*/, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "join node has 2 inputs, can't call parse().");
}

const substrait::Rel & CrossRelParser::getSingleInput(const substrait::Rel & /*rel*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "join node has 2 inputs, can't call getSingleInput().");
}

DB::QueryPlanPtr CrossRelParser::parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    const auto & join = rel.cross();
    if (!join.has_left() || !join.has_right())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "left table or right table is missing.");
    }

    rel_stack.push_back(&rel);
    auto left_plan = getPlanParser()->parseOp(join.left(), rel_stack);
    auto right_plan = getPlanParser()->parseOp(join.right(), rel_stack);
    rel_stack.pop_back();

    return parseJoin(join, std::move(left_plan), std::move(right_plan));
}

void CrossRelParser::renamePlanColumns(DB::QueryPlan & left, DB::QueryPlan & right, const StorageJoinFromReadBuffer & storage_join)
{
    ActionsDAGPtr project = nullptr;
    /// To support mixed join conditions, we must make sure that the column names in the right be the same as
    /// storage_join's right sample block.
    auto right_ori_header = right.getCurrentDataStream().header.getColumnsWithTypeAndName();
    if (right_ori_header.size() > 0 && right_ori_header[0].name != BlockUtil::VIRTUAL_ROW_COUNT_COLUMN)
    {
        project = ActionsDAG::makeConvertingActions(
            right_ori_header, storage_join.getRightSampleBlock().getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Position);
        if (project)
        {
            QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right.getCurrentDataStream(), project);
            project_step->setStepDescription("Rename Broadcast Table Name");
            steps.emplace_back(project_step.get());
            right.addStep(std::move(project_step));
        }
    }

    /// If the columns name in right table is duplicated with left table, we need to rename the left table's columns,
    /// avoid the columns name in the right table be changed in `addConvertStep`.
    /// This could happen in tpc-ds q44.
    DB::ColumnsWithTypeAndName new_left_cols;
    const auto & right_header = right.getCurrentDataStream().header;
    auto left_prefix = getUniqueName("left");
    for (const auto & col : left.getCurrentDataStream().header)
        if (right_header.has(col.name))
            new_left_cols.emplace_back(col.column, col.type, left_prefix + col.name);
        else
            new_left_cols.emplace_back(col.column, col.type, col.name);
    auto left_header = left.getCurrentDataStream().header.getColumnsWithTypeAndName();
    if (left_header.size() > 0 && left_header[0].name != BlockUtil::VIRTUAL_ROW_COUNT_COLUMN)
    {
        project = ActionsDAG::makeConvertingActions(left_header, new_left_cols, ActionsDAG::MatchColumnsMode::Position);

        if (project)
        {
            QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(left.getCurrentDataStream(), project);
            project_step->setStepDescription("Rename Left Table Name for broadcast join");
            steps.emplace_back(project_step.get());
            left.addStep(std::move(project_step));
        }
    }
}

DB::QueryPlanPtr CrossRelParser::parseJoin(const substrait::CrossRel & join, DB::QueryPlanPtr left, DB::QueryPlanPtr right)
{
    auto storage_join_key = parseCrossJoinOptimizationInfos(join);
    auto storage_join = BroadCastJoinBuilder::getJoin(storage_join_key) ;
    renamePlanColumns(*left, *right, *storage_join);
    auto table_join = createCrossTableJoin(join.type());
    DB::Block right_header_before_convert_step = right->getCurrentDataStream().header;
    addConvertStep(*table_join, *left, *right);

    // Add a check to find error easily.
    if(!blocksHaveEqualStructure(right_header_before_convert_step, right->getCurrentDataStream().header))
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "For broadcast join, we must not change the columns name in the right table.\nleft header:{},\nright header: {} -> {}",
            left->getCurrentDataStream().header.dumpNames(),
            right_header_before_convert_step.dumpNames(),
            right->getCurrentDataStream().header.dumpNames());
    }

    Names after_join_names;
    auto left_names = left->getCurrentDataStream().header.getNames();
    after_join_names.insert(after_join_names.end(), left_names.begin(), left_names.end());
    auto right_name = table_join->columnsFromJoinedTable().getNames();
    after_join_names.insert(after_join_names.end(), right_name.begin(), right_name.end());

    auto left_header = left->getCurrentDataStream().header;
    auto right_header = right->getCurrentDataStream().header;

    QueryPlanPtr query_plan;
    table_join->addDisjunct();
    auto broadcast_hash_join = storage_join->getJoinLocked(table_join, context);
    // table_join->resetKeys();
    QueryPlanStepPtr join_step = std::make_unique<FilledJoinStep>(left->getCurrentDataStream(), broadcast_hash_join, 8192);

    join_step->setStepDescription("STORAGE_JOIN");
    steps.emplace_back(join_step.get());
    left->addStep(std::move(join_step));
    query_plan = std::move(left);
    /// hold right plan for profile
    extra_plan_holder.emplace_back(std::move(right));

    addPostFilter(*query_plan, join);
    Names cols;
    for (auto after_join_name : after_join_names)
    {
        if (BlockUtil::VIRTUAL_ROW_COUNT_COLUMN == after_join_name)
            continue;

        cols.emplace_back(after_join_name);
    }
    JoinUtil::reorderJoinOutput(*query_plan, cols);

    return query_plan;
}


void CrossRelParser::addPostFilter(DB::QueryPlan & query_plan, const substrait::CrossRel & join_rel)
{
    if (!join_rel.has_expression())
        return;

    auto expression = join_rel.expression();
    std::string filter_name;
    auto actions_dag = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());
    if (!expression.has_scalar_function())
    {
        // It may be singular_or_list
        auto * in_node = getPlanParser()->parseExpression(actions_dag, expression);
        filter_name = in_node->result_name;
    }
    else
    {
        getPlanParser()->parseFunction(query_plan.getCurrentDataStream().header, expression, filter_name, actions_dag, true);
    }
    auto filter_step = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(), actions_dag, filter_name, true);
    filter_step->setStepDescription("Post Join Filter");
    steps.emplace_back(filter_step.get());
    query_plan.addStep(std::move(filter_step));
}

void CrossRelParser::addConvertStep(TableJoin & table_join, DB::QueryPlan & left, DB::QueryPlan & right)
{
    /// If the columns name in right table is duplicated with left table, we need to rename the right table's columns.
    NameSet left_columns_set;
    for (const auto & col : left.getCurrentDataStream().header.getNames())
        left_columns_set.emplace(col);
    table_join.setColumnsFromJoinedTable(
        right.getCurrentDataStream().header.getNamesAndTypesList(), left_columns_set, getUniqueName("right") + ".");

    // fix right table key duplicate
    NamesWithAliases right_table_alias;
    for (size_t idx = 0; idx < table_join.columnsFromJoinedTable().size(); idx++)
    {
        auto origin_name = right.getCurrentDataStream().header.getByPosition(idx).name;
        auto dedup_name = table_join.columnsFromJoinedTable().getNames().at(idx);
        if (origin_name != dedup_name)
        {
            right_table_alias.emplace_back(NameWithAlias(origin_name, dedup_name));
        }
    }
    if (!right_table_alias.empty())
    {
        ActionsDAGPtr rename_dag = std::make_shared<ActionsDAG>(right.getCurrentDataStream().header.getNamesAndTypesList());
        auto original_right_columns = right.getCurrentDataStream().header;
        for (const auto & column_alias : right_table_alias)
        {
            if (original_right_columns.has(column_alias.first))
            {
                auto pos = original_right_columns.getPositionByName(column_alias.first);
                const auto & alias = rename_dag->addAlias(*rename_dag->getInputs()[pos], column_alias.second);
                rename_dag->getOutputs()[pos] = &alias;
            }
        }

        QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right.getCurrentDataStream(), rename_dag);
        project_step->setStepDescription("Right Table Rename");
        steps.emplace_back(project_step.get());
        right.addStep(std::move(project_step));
    }

    for (const auto & column : table_join.columnsFromJoinedTable())
    {
        table_join.addJoinedColumn(column);
    }
    ActionsDAGPtr left_convert_actions = nullptr;
    ActionsDAGPtr right_convert_actions = nullptr;
    std::tie(left_convert_actions, right_convert_actions) = table_join.createConvertingActions(
        left.getCurrentDataStream().header.getColumnsWithTypeAndName(), right.getCurrentDataStream().header.getColumnsWithTypeAndName());

    if (right_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right.getCurrentDataStream(), right_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        steps.emplace_back(converting_step.get());
        right.addStep(std::move(converting_step));
    }

    if (left_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(left.getCurrentDataStream(), left_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        steps.emplace_back(converting_step.get());
        left.addStep(std::move(converting_step));
    }
}


void registerCrossRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_paser) { return std::make_shared<CrossRelParser>(plan_paser); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kCross, builder);
}

}
