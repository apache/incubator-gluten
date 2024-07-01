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
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Join/BroadCastJoinBuilder.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Parser/SerializedPlanParser.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <google/protobuf/wrappers.pb.h>

#include <Poco/Logger.h>
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

struct JoinOptimizationInfo
{
    bool is_broadcast = false;
    bool is_smj = false;
    bool is_null_aware_anti_join = false;
    bool is_existence_join = false;
    std::string storage_join_key;
};

using namespace DB;


String parseJoinOptimizationInfos(const substrait::CrossRel & join)
{
    google::protobuf::StringValue optimization;
    optimization.ParseFromString(join.advanced_extension().optimization().value());
    JoinOptimizationInfo info;
    auto a = optimization.value();
    String storage_join_key;
    ReadBufferFromString in(optimization.value());
    assertString("JoinParameters:", in);
    assertString("buildHashTableId=", in);
    readString(storage_join_key, in);
    return storage_join_key;
}

void reorderJoinOutput2(DB::QueryPlan & plan, DB::Names cols)
{
    ActionsDAGPtr project = std::make_shared<ActionsDAG>(plan.getCurrentDataStream().header.getNamesAndTypesList());
    NamesWithAliases project_cols;
    for (const auto & col : cols)
    {
        project_cols.emplace_back(NameWithAlias(col, col));
    }
    project->project(project_cols);
    QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), project);
    project_step->setStepDescription("Reorder Join Output");
    plan.addStep(std::move(project_step));
}

namespace local_engine
{

std::pair<DB::JoinKind, DB::JoinStrictness> getJoinKindAndStrictness2(substrait::CrossRel_JoinType join_type)
{
    switch (join_type)
    {
        case substrait::CrossRel_JoinType_JOIN_TYPE_INNER:
        case substrait::CrossRel_JoinType_JOIN_TYPE_LEFT:
        case substrait::CrossRel_JoinType_JOIN_TYPE_OUTER:
            return {DB::JoinKind::Cross, DB::JoinStrictness::All};
        // case substrait::CrossRel_JoinType_JOIN_TYPE_LEFT:
        //     return {DB::JoinKind::Left, DB::JoinStrictness::All};
        //
        // case substrait::CrossRel_JoinType_JOIN_TYPE_RIGHT:
        //     return {DB::JoinKind::Right, DB::JoinStrictness::All};
        // case substrait::CrossRel_JoinType_JOIN_TYPE_OUTER:
        //     return {DB::JoinKind::Full, DB::JoinStrictness::All};
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported join type {}.", magic_enum::enum_name(join_type));
    }
}
std::shared_ptr<DB::TableJoin> createDefaultTableJoin2(substrait::CrossRel_JoinType join_type)
{
    auto & global_context = SerializedPlanParser::global_context;
    auto table_join = std::make_shared<TableJoin>(
        global_context->getSettings(), global_context->getGlobalTemporaryVolume(), global_context->getTempDataOnDisk());

    std::pair<DB::JoinKind, DB::JoinStrictness> kind_and_strictness = getJoinKindAndStrictness2(join_type);
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

std::unordered_set<DB::JoinTableSide> CrossRelParser::extractTableSidesFromExpression(const substrait::Expression & expr, const DB::Block & left_header, const DB::Block & right_header)
{
    std::unordered_set<DB::JoinTableSide> table_sides;
    if (expr.has_scalar_function())
    {
        for (const auto & arg : expr.scalar_function().arguments())
        {
            auto table_sides_from_arg = extractTableSidesFromExpression(arg.value(), left_header, right_header);
            table_sides.insert(table_sides_from_arg.begin(), table_sides_from_arg.end());
        }
    }
    else if (expr.has_selection() && expr.selection().has_direct_reference() && expr.selection().direct_reference().has_struct_field())
    {
        auto pos = expr.selection().direct_reference().struct_field().field();
        if (pos < left_header.columns())
        {
            table_sides.insert(DB::JoinTableSide::Left);
        }
        else
        {
            table_sides.insert(DB::JoinTableSide::Right);
        }
    }
    else if (expr.has_singular_or_list())
    {
        auto child_table_sides = extractTableSidesFromExpression(expr.singular_or_list().value(), left_header, right_header);
        table_sides.insert(child_table_sides.begin(), child_table_sides.end());
        for (const auto & option : expr.singular_or_list().options())
        {
            child_table_sides = extractTableSidesFromExpression(option, left_header, right_header);
            table_sides.insert(child_table_sides.begin(), child_table_sides.end());
        }
    }
    else if (expr.has_cast())
    {
        auto child_table_sides = extractTableSidesFromExpression(expr.cast().input(), left_header, right_header);
        table_sides.insert(child_table_sides.begin(), child_table_sides.end());
    }
    else if (expr.has_if_then())
    {
        for (const auto & if_child : expr.if_then().ifs())
        {
            auto child_table_sides = extractTableSidesFromExpression(if_child.if_(), left_header, right_header);
            table_sides.insert(child_table_sides.begin(), child_table_sides.end());
            child_table_sides = extractTableSidesFromExpression(if_child.then(), left_header, right_header);
            table_sides.insert(child_table_sides.begin(), child_table_sides.end());
        }
        auto child_table_sides = extractTableSidesFromExpression(expr.if_then().else_(), left_header, right_header);
        table_sides.insert(child_table_sides.begin(), child_table_sides.end());
    }
    else if (expr.has_literal())
    {
        // nothing
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Illegal expression '{}'", expr.DebugString());
    }
    return table_sides;
}


void CrossRelParser::renamePlanColumns(DB::QueryPlan & left, DB::QueryPlan & right, const StorageJoinFromReadBuffer & storage_join)
{
    /// To support mixed join conditions, we must make sure that the column names in the right be the same as
    /// storage_join's right sample block.
    ActionsDAGPtr project = ActionsDAG::makeConvertingActions(
        right.getCurrentDataStream().header.getColumnsWithTypeAndName(),
        storage_join.getRightSampleBlock().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Position);

    if (project)
    {
        QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right.getCurrentDataStream(), project);
        project_step->setStepDescription("Rename Broadcast Table Name");
        steps.emplace_back(project_step.get());
        right.addStep(std::move(project_step));
    }

    /// If the columns name in right table is duplicated with left table, we need to rename the left table's columns,
    /// avoid the columns name in the right table be changed in `addConvertStep`.
    /// This could happen in tpc-ds q44.
    DB::ColumnsWithTypeAndName new_left_cols;
    const auto & right_header = right.getCurrentDataStream().header;
    auto left_prefix = getUniqueName("left");
    for (const auto & col : left.getCurrentDataStream().header)
    {
        if (right_header.has(col.name))
        {
            new_left_cols.emplace_back(col.column, col.type, left_prefix + col.name);
        }
        else
        {
            new_left_cols.emplace_back(col.column, col.type, col.name);
        }
    }
    project = ActionsDAG::makeConvertingActions(
        left.getCurrentDataStream().header.getColumnsWithTypeAndName(),
        new_left_cols,
        ActionsDAG::MatchColumnsMode::Position);

    if (project)
    {
        QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(left.getCurrentDataStream(), project);
        project_step->setStepDescription("Rename Left Table Name for broadcast join");
        steps.emplace_back(project_step.get());
        left.addStep(std::move(project_step));
    }
}

DB::QueryPlanPtr CrossRelParser::parseJoin(const substrait::CrossRel & join, DB::QueryPlanPtr left, DB::QueryPlanPtr right)
{
    auto storage_join_key = parseJoinOptimizationInfos(join);
    auto storage_join = BroadCastJoinBuilder::getJoin(storage_join_key) ;
    renamePlanColumns(*left, *right, *storage_join);
    auto table_join = createDefaultTableJoin2(join.type());
    DB::Block right_header_before_convert_step = right->getCurrentDataStream().header;
    addConvertStep(*table_join, *left, *right);

    // Add a check to find error easily.
    if (storage_join)
    {
        if(!blocksHaveEqualStructure(right_header_before_convert_step, right->getCurrentDataStream().header))
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "For broadcast join, we must not change the columns name in the right table.\nleft header:{},\nright header: {} -> {}",
                left->getCurrentDataStream().header.dumpNames(),
                right_header_before_convert_step.dumpNames(),
                right->getCurrentDataStream().header.dumpNames());
        }
    }

    Names after_join_names;
    auto left_names = left->getCurrentDataStream().header.getNames();
    after_join_names.insert(after_join_names.end(), left_names.begin(), left_names.end());
    auto right_name = table_join->columnsFromJoinedTable().getNames();
    after_join_names.insert(after_join_names.end(), right_name.begin(), right_name.end());

    auto left_header = left->getCurrentDataStream().header;
    auto right_header = right->getCurrentDataStream().header;

    QueryPlanPtr query_plan;
    // applyJoinFilter(*table_join, join, *left, *right, true);
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
    reorderJoinOutput2(*query_plan, after_join_names);

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

bool CrossRelParser::applyJoinFilter(
    DB::TableJoin & table_join, const substrait::CrossRel & join_rel, DB::QueryPlan & left, DB::QueryPlan & right, bool allow_mixed_condition)
{
    if (!join_rel.has_expression())
        return true;
    const auto & expr = join_rel.expression();

    const auto & left_header = left.getCurrentDataStream().header;
    const auto & right_header = right.getCurrentDataStream().header;
    ColumnsWithTypeAndName mixed_columns;
    std::unordered_set<String> added_column_name;
    for (const auto & col : left_header.getColumnsWithTypeAndName())
    {
        mixed_columns.emplace_back(col);
        added_column_name.insert(col.name);
    }
    for (const auto & col : right_header.getColumnsWithTypeAndName())
    {
        const auto & renamed_col_name = table_join.renamedRightColumnNameWithAlias(col.name);
        if (added_column_name.find(col.name) != added_column_name.end())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Right column's name conflict with left column: {}", col.name);
        mixed_columns.emplace_back(col);
        added_column_name.insert(col.name);
    }
    DB::Block mixed_header(mixed_columns);

    auto table_sides = extractTableSidesFromExpression(expr, left_header, right_header);

    auto get_input_expressions = [](const DB::Block & header)
    {
        std::vector<substrait::Expression> exprs;
        for (size_t i = 0; i < header.columns(); ++i)
        {
            substrait::Expression expr;
            expr.mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(i);
            exprs.emplace_back(expr);
        }
        return exprs;
    };

    /// If the columns in the expression are all from one table, use analyzer_left_filter_condition_column_name
    /// and analyzer_left_filter_condition_column_name to filt the join result data. It requires to build the filter
    /// column at first.
    /// If the columns in the expression are from both tables, use mixed_join_expression to filt the join result data.
    /// the filter columns will be built inner the join step.
    if (table_sides.size() == 1)
    {
        auto table_side = *table_sides.begin();
        if (table_side == DB::JoinTableSide::Left)
        {
            auto input_exprs = get_input_expressions(left_header);
            input_exprs.push_back(expr);
            auto actions_dag = expressionsToActionsDAG(input_exprs, left_header);
            table_join.getClauses().back().analyzer_left_filter_condition_column_name = actions_dag->getOutputs().back()->result_name;
            QueryPlanStepPtr before_join_step = std::make_unique<ExpressionStep>(left.getCurrentDataStream(), actions_dag);
            before_join_step->setStepDescription("Before JOIN LEFT");
            steps.emplace_back(before_join_step.get());
            left.addStep(std::move(before_join_step));
        }
        else
        {
            /// since the field reference in expr is the index of left_header ++ right_header, so we use
            /// mixed_header to build the actions_dag
            auto input_exprs = get_input_expressions(mixed_header);
            input_exprs.push_back(expr);
            auto actions_dag = expressionsToActionsDAG(input_exprs, mixed_header);

            /// clear unused columns in actions_dag
            for (const auto & col : left_header.getColumnsWithTypeAndName())
            {
                actions_dag->removeUnusedResult(col.name);
            }
            actions_dag->removeUnusedActions();

            table_join.getClauses().back().analyzer_right_filter_condition_column_name = actions_dag->getOutputs().back()->result_name;
            QueryPlanStepPtr before_join_step = std::make_unique<ExpressionStep>(right.getCurrentDataStream(), actions_dag);
            before_join_step->setStepDescription("Before JOIN RIGHT");
            steps.emplace_back(before_join_step.get());
            right.addStep(std::move(before_join_step));
        }
    }
    else if (table_sides.size() == 2)
    {
        if (!allow_mixed_condition)
            return false;
        auto mixed_join_expressions_actions = expressionsToActionsDAG({expr}, mixed_header);
        table_join.getMixedJoinExpression()
            = std::make_shared<DB::ExpressionActions>(mixed_join_expressions_actions, ExpressionActionsSettings::fromContext(context));
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Not any table column is used in the join condition.\n{}", join_rel.DebugString());
    }
    return true;
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

/// Join keys are collected from substrait::JoinRel::expression() which only contains the equal join conditions.
void CrossRelParser::collectJoinKeys(
    TableJoin & table_join, const substrait::CrossRel & join_rel, const DB::Block & left_header, const DB::Block & right_header)
{
    if (!join_rel.has_expression())
        return;
    const auto & expr = join_rel.expression();
    auto & join_clause = table_join.getClauses().back();
    std::list<const substrait::Expression *> expressions_stack;
    expressions_stack.push_back(&expr);
    while (!expressions_stack.empty())
    {
        /// Must handle the expressions in DF order. It matters in sort merge join.
        const auto * current_expr = expressions_stack.back();
        expressions_stack.pop_back();
        if (!current_expr->has_scalar_function())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Function expression is expected");
        auto function_name = parseFunctionName(current_expr->scalar_function().function_reference(), current_expr->scalar_function());
        if (!function_name)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid function expression");
        if (*function_name == "equals")
        {
            String left_key, right_key;
            size_t left_pos = 0, right_pos = 0;
            for (const auto & arg : current_expr->scalar_function().arguments())
            {
                if (!arg.value().has_selection() || !arg.value().selection().has_direct_reference()
                    || !arg.value().selection().direct_reference().has_struct_field())
                {
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "A column reference is expected");
                }
                auto col_pos_ref = arg.value().selection().direct_reference().struct_field().field();
                if (col_pos_ref < left_header.columns())
                {
                    left_pos = col_pos_ref;
                    left_key = left_header.getByPosition(col_pos_ref).name;
                }
                else
                {
                    right_pos = col_pos_ref - left_header.columns();
                    right_key = right_header.getByPosition(col_pos_ref - left_header.columns()).name;
                }
            }
            if (left_key.empty() || right_key.empty())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid key equal join condition");
            join_clause.addKey(left_key, right_key, false);
        }
        else if (*function_name == "and")
        {
            expressions_stack.push_back(&current_expr->scalar_function().arguments().at(1).value());
            expressions_stack.push_back(&current_expr->scalar_function().arguments().at(0).value());
        }
        else if (*function_name == "not")
        {
            expressions_stack.push_back(&current_expr->scalar_function().arguments().at(0).value());
        }
        else
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknow function: {}", *function_name);
        }
    }
}

void registerCrossRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_paser) { return std::make_shared<CrossRelParser>(plan_paser); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kCross, builder);
}

}
