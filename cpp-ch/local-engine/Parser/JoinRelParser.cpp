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
#include "JoinRelParser.h"
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

JoinOptimizationInfo parseJoinOptimizationInfo(const substrait::JoinRel & join)
{
    google::protobuf::StringValue optimization;
    optimization.ParseFromString(join.advanced_extension().optimization().value());
    JoinOptimizationInfo info;
    if (optimization.value().contains("isBHJ="))
    {
        ReadBufferFromString in(optimization.value());
        assertString("JoinParameters:", in);
        assertString("isBHJ=", in);
        readBoolText(info.is_broadcast, in);
        assertChar('\n', in);
        if (info.is_broadcast)
        {
            assertString("isNullAwareAntiJoin=", in);
            readBoolText(info.is_null_aware_anti_join, in);
            assertChar('\n', in);
            assertString("buildHashTableId=", in);
            readString(info.storage_join_key, in);
            assertChar('\n', in);
        }
    }
    else
    {
        ReadBufferFromString in(optimization.value());
        assertString("JoinParameters:", in);
        assertString("isSMJ=", in);
        readBoolText(info.is_smj, in);
        assertChar('\n', in);
        if (info.is_smj)
        {
            assertString("isNullAwareAntiJoin=", in);
            readBoolText(info.is_null_aware_anti_join, in);
            assertChar('\n', in);
            assertString("isExistenceJoin=", in);
            readBoolText(info.is_existence_join, in);
            assertChar('\n', in);
        }
    }
    return info;
}


void reorderJoinOutput(DB::QueryPlan & plan, DB::Names cols)
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

std::pair<DB::JoinKind, DB::JoinStrictness> getJoinKindAndStrictness(substrait::JoinRel_JoinType join_type)
{
    switch (join_type)
    {
        case substrait::JoinRel_JoinType_JOIN_TYPE_INNER:
            return {DB::JoinKind::Inner, DB::JoinStrictness::All};
        case substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
            return {DB::JoinKind::Left, DB::JoinStrictness::Semi};
        case substrait::JoinRel_JoinType_JOIN_TYPE_ANTI:
            return {DB::JoinKind::Left, DB::JoinStrictness::Anti};
        case substrait::JoinRel_JoinType_JOIN_TYPE_LEFT:
            return {DB::JoinKind::Left, DB::JoinStrictness::All};
        case substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT:
            return {DB::JoinKind::Right, DB::JoinStrictness::All};
        case substrait::JoinRel_JoinType_JOIN_TYPE_OUTER:
            return {DB::JoinKind::Full, DB::JoinStrictness::All};
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported join type {}.", magic_enum::enum_name(join_type));
    }
}
std::shared_ptr<DB::TableJoin> createDefaultTableJoin(substrait::JoinRel_JoinType join_type)
{
    auto & global_context = SerializedPlanParser::global_context;
    auto table_join = std::make_shared<TableJoin>(
        global_context->getSettings(), global_context->getGlobalTemporaryVolume(), global_context->getTempDataOnDisk());

    std::pair<DB::JoinKind, DB::JoinStrictness> kind_and_strictness = getJoinKindAndStrictness(join_type);
    table_join->setKind(kind_and_strictness.first);
    table_join->setStrictness(kind_and_strictness.second);
    return table_join;
}

JoinRelParser::JoinRelParser(SerializedPlanParser * plan_paser_)
    : RelParser(plan_paser_)
    , function_mapping(plan_paser_->function_mapping)
    , context(plan_paser_->context)
    , extra_plan_holder(plan_paser_->extra_plan_holder)
{
}

DB::QueryPlanPtr
JoinRelParser::parse(DB::QueryPlanPtr /*query_plan*/, const substrait::Rel & /*rel*/, std::list<const substrait::Rel *> & /*rel_stack_*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "join node has 2 inputs, can't call parse().");
}

const substrait::Rel & JoinRelParser::getSingleInput(const substrait::Rel & /*rel*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "join node has 2 inputs, can't call getSingleInput().");
}

DB::QueryPlanPtr JoinRelParser::parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    const auto & join = rel.join();
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

std::unordered_set<DB::JoinTableSide> JoinRelParser::extractTableSidesFromExpression(const substrait::Expression & expr, const DB::Block & left_header, const DB::Block & right_header)
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


void JoinRelParser::renamePlanColumns(DB::QueryPlan & left, DB::QueryPlan & right, const StorageJoinFromReadBuffer & storage_join)
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

DB::QueryPlanPtr JoinRelParser::parseJoin(const substrait::JoinRel & join, DB::QueryPlanPtr left, DB::QueryPlanPtr right)
{
    auto join_opt_info = parseJoinOptimizationInfo(join);
    auto storage_join = join_opt_info.is_broadcast ? BroadCastJoinBuilder::getJoin(join_opt_info.storage_join_key) : nullptr;
    if (storage_join)
    {
        renamePlanColumns(*left, *right, *storage_join);
    }

    auto table_join = createDefaultTableJoin(join.type());
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

    /// Support only one join clause.
    table_join->addDisjunct();
    /// some examples to explain when the post_join_filter is not empty
    /// - on t1.key = t2.key and t1.v1 > 1 and t2.v1 > 1, 't1.v1> 1' is in the  post filter. but 't2.v1 > 1'
    ///   will be pushed down into right table by spark and is not in the post filter. 't1.key = t2.key ' is
    ///   in JoinRel::expression.
    /// - on t1.key = t2. key and t1.v1 > t2.v2, 't1.v1 > t2.v2' is in the post filter.
    collectJoinKeys(*table_join, join, left_header, right_header);

    if (storage_join)
    {

        applyJoinFilter(*table_join, join, *left, *right, true);
        auto broadcast_hash_join = storage_join->getJoinLocked(table_join, context);

        QueryPlanStepPtr join_step = std::make_unique<FilledJoinStep>(left->getCurrentDataStream(), broadcast_hash_join, 8192);

        join_step->setStepDescription("STORAGE_JOIN");
        steps.emplace_back(join_step.get());
        left->addStep(std::move(join_step));
        query_plan = std::move(left);
        /// hold right plan for profile
        extra_plan_holder.emplace_back(std::move(right));
    }
    else if (join_opt_info.is_smj)
    {
        bool need_post_filter = !applyJoinFilter(*table_join, join, *left, *right, false);

        /// If applyJoinFilter returns false, it means there are mixed conditions in the post_join_filter.
        /// It should be a inner join.
        /// TODO: make smj support mixed conditions
        if (need_post_filter && table_join->kind() != DB::JoinKind::Inner)
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Sort merge join doesn't support mixed join conditions, except inner join.");
        }

        JoinPtr smj_join = std::make_shared<FullSortingMergeJoin>(table_join, right->getCurrentDataStream().header.cloneEmpty(), -1);
        MultiEnum<DB::JoinAlgorithm> join_algorithm = context->getSettingsRef().join_algorithm;
        QueryPlanStepPtr join_step
                 = std::make_unique<DB::JoinStep>(left->getCurrentDataStream(), right->getCurrentDataStream(), smj_join, 8192, 1, false);

        join_step->setStepDescription("SORT_MERGE_JOIN");
        steps.emplace_back(join_step.get());
        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::move(left));
        plans.emplace_back(std::move(right));

        query_plan = std::make_unique<QueryPlan>();
        query_plan->unitePlans(std::move(join_step), {std::move(plans)});
        if (need_post_filter)
            addPostFilter(*query_plan, join);
    }
    else
    {
        applyJoinFilter(*table_join, join, *left, *right, true);

        /// Following is some configurations for grace hash join.
        /// - spark.gluten.sql.columnar.backend.ch.runtime_settings.join_algorithm=grace_hash. This will
        ///   enable grace hash join.
        /// - spark.gluten.sql.columnar.backend.ch.runtime_settings.max_bytes_in_join=3145728. This setup
        ///   the memory limitation fro grace hash join. If the memory consumption exceeds the limitation,
        ///   data will be spilled to disk. Don't set the limitation too small, otherwise the buckets number
        ///   will be too large and the performance will be bad.
        JoinPtr hash_join = nullptr;
        MultiEnum<DB::JoinAlgorithm> join_algorithm = context->getSettingsRef().join_algorithm;
        if (join_algorithm.isSet(DB::JoinAlgorithm::GRACE_HASH))
        {
            hash_join = std::make_shared<GraceHashJoin>(
                context,
                table_join,
                left->getCurrentDataStream().header,
                right->getCurrentDataStream().header,
                context->getTempDataOnDisk());
        }
        else
        {
            hash_join = std::make_shared<HashJoin>(table_join, right->getCurrentDataStream().header.cloneEmpty());
        }
        QueryPlanStepPtr join_step
            = std::make_unique<DB::JoinStep>(left->getCurrentDataStream(), right->getCurrentDataStream(), hash_join, 8192, 1, false);

        join_step->setStepDescription("HASH_JOIN");
        steps.emplace_back(join_step.get());
        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::move(left));
        plans.emplace_back(std::move(right));

        query_plan = std::make_unique<QueryPlan>();
        query_plan->unitePlans(std::move(join_step), {std::move(plans)});
    }
    reorderJoinOutput(*query_plan, after_join_names);

    return query_plan;
}

void JoinRelParser::addConvertStep(TableJoin & table_join, DB::QueryPlan & left, DB::QueryPlan & right)
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
        rename_dag->projectInput();
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
void JoinRelParser::collectJoinKeys(
    TableJoin & table_join, const substrait::JoinRel & join_rel, const DB::Block & left_header, const DB::Block & right_header)
{
    if (!join_rel.has_expression())
        return;
    const auto & expr = join_rel.expression();
    auto & join_clause = table_join.getClauses().back();
    std::list<const const substrait::Expression *> expressions_stack;
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
        else
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknow function: {}", *function_name);
        }
    }
}

bool JoinRelParser::applyJoinFilter(
    DB::TableJoin & table_join, const substrait::JoinRel & join_rel, DB::QueryPlan & left, DB::QueryPlan & right, bool allow_mixed_condition)
{
    if (!join_rel.has_post_join_filter())
        return true;
    const auto & expr = join_rel.post_join_filter();

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

void JoinRelParser::addPostFilter(DB::QueryPlan & query_plan, const substrait::JoinRel & join)
{
    std::string filter_name;
    auto actions_dag = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());
    if (!join.post_join_filter().has_scalar_function())
    {
	// It may be singular_or_list
        auto * in_node = getPlanParser()->parseExpression(actions_dag, join.post_join_filter());
        filter_name = in_node->result_name;
    }
    else
    {
        getPlanParser()->parseFunction(query_plan.getCurrentDataStream().header, join.post_join_filter(), filter_name, actions_dag, true);
    }
    auto filter_step = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(), actions_dag, filter_name, true);
    filter_step->setStepDescription("Post Join Filter");
    steps.emplace_back(filter_step.get());
    query_plan.addStep(std::move(filter_step));
}

void registerJoinRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_paser) { return std::make_shared<JoinRelParser>(plan_paser); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kJoin, builder);
}

}
