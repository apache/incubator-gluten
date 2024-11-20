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
#include "SerializedPlanParser.h"
#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryPriorities.h>
#include <Operator/BlocksBufferPoolTransform.h>
#include <Parser/ExpressionParser.h>
#include <Parser/FunctionParser.h>
#include <Parser/LocalExecutor.h>
#include <Parser/ParserContext.h>
#include <Parser/RelParsers/ReadRelParser.h>
#include <Parser/RelParsers/RelParser.h>
#include <Parser/RelParsers/WriteRelParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parser/TypeParser.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/printPipeline.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/wrappers.pb.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>
#include <Common/Exception.h>
#include <Common/GlutenConfig.h>
#include <Common/JNIUtils.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool query_plan_enable_optimizations;
extern const SettingsUInt64 priority;
}
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TYPE;
extern const int BAD_ARGUMENTS;
extern const int NO_SUCH_DATA_PART;
extern const int UNKNOWN_FUNCTION;
extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int INVALID_JOIN_ON_EXPRESSION;
}
}

namespace local_engine
{
using namespace DB;

std::string join(const ActionsDAG::NodeRawConstPtrs & v, char c)
{
    std::string res;
    for (size_t i = 0; i < v.size(); ++i)
    {
        if (i)
            res += c;
        res += v[i]->result_name;
    }
    return res;
}

void SerializedPlanParser::adjustOutput(const DB::QueryPlanPtr & query_plan, const substrait::Plan & plan)
{
    const substrait::PlanRel & root_rel = plan.relations().at(0);
    if (root_rel.root().names_size())
    {
        ActionsDAG actions_dag{blockToNameAndTypeList(query_plan->getCurrentHeader())};
        NamesWithAliases aliases;
        const auto cols = query_plan->getCurrentHeader().getNamesAndTypesList();
        if (cols.getNames().size() != static_cast<size_t>(root_rel.root().names_size()))
        {
            debug::dumpPlan(*query_plan, "clickhouse plan", true);
            debug::dumpMessage(plan, "substrait::Plan", true);
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Missmatch result columns size. plan column size {}, subtrait plan name size {}.",
                cols.getNames().size(),
                root_rel.root().names_size());
        }
        for (int i = 0; i < static_cast<int>(cols.getNames().size()); i++)
            aliases.emplace_back(NameWithAlias(cols.getNames()[i], root_rel.root().names(i)));
        actions_dag.project(aliases);
        auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentHeader(), std::move(actions_dag));
        expression_step->setStepDescription("Rename Output");
        query_plan->addStep(std::move(expression_step));
    }

    // fixes: issue-1874, to keep the nullability as expected.
    const auto & output_schema = root_rel.root().output_schema();
    if (output_schema.types_size())
    {
        auto origin_header = query_plan->getCurrentHeader();
        const auto & origin_columns = origin_header.getColumnsWithTypeAndName();

        if (static_cast<size_t>(output_schema.types_size()) != origin_columns.size())
        {
            debug::dumpPlan(*query_plan, "clickhouse plan", true);
            debug::dumpMessage(plan, "substrait::Plan", true);
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Missmatch result columns size. plan column size {}, subtrait plan output schema size {}, subtrait plan name size {}.",
                origin_columns.size(),
                output_schema.types_size(),
                root_rel.root().names_size());
        }

        bool need_final_project = false;
        ColumnsWithTypeAndName final_columns;
        for (int i = 0; i < output_schema.types_size(); ++i)
        {
            const auto & origin_column = origin_columns[i];
            const auto & origin_type = origin_column.type;
            auto final_type = TypeParser::parseType(output_schema.types(i));

            /// Intermediate aggregate data is special, no check here.
            if (typeid_cast<const DataTypeAggregateFunction *>(origin_column.type.get()) || origin_type->equals(*final_type))
                final_columns.push_back(origin_column);
            else
            {
                need_final_project = true;

                bool need_const = origin_column.column && isColumnConst(*origin_column.column);
                ColumnWithTypeAndName final_column(
                    need_const ? final_type->createColumnConst(0, assert_cast<const ColumnConst &>(*origin_column.column).getField())
                               : final_type->createColumn(),
                    final_type,
                    origin_column.name);
                final_columns.emplace_back(std::move(final_column));
            }
        }

        if (need_final_project)
        {
            ActionsDAG final_project = ActionsDAG::makeConvertingActions(origin_columns, final_columns, ActionsDAG::MatchColumnsMode::Position);
            QueryPlanStepPtr final_project_step
                = std::make_unique<ExpressionStep>(query_plan->getCurrentHeader(), std::move(final_project));
            final_project_step->setStepDescription("Project for output schema");
            query_plan->addStep(std::move(final_project_step));
        }
    }
}

QueryPlanPtr SerializedPlanParser::parse(const substrait::Plan & plan)
{
    debug::dumpMessage(plan, "substrait::Plan");
    //parseExtensions(plan.extensions());
    if (plan.relations_size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "too many relations found");

    const substrait::PlanRel & root_rel = plan.relations().at(0);
    if (!root_rel.has_root())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "must have root rel!");

    const bool writePipeline = root_rel.root().input().has_write();
    const substrait::Rel & first_read_rel = writePipeline ? root_rel.root().input().write().input() : root_rel.root().input();

    std::list<const substrait::Rel *> rel_stack;
    auto query_plan = parseOp(first_read_rel, rel_stack);
    if (!writePipeline)
        adjustOutput(query_plan, plan);

#ifndef NDEBUG
    PlanUtil::checkOuputType(*query_plan);
#endif

    debug::dumpPlan(*query_plan);
    return query_plan;
}

std::unique_ptr<LocalExecutor> SerializedPlanParser::createExecutor(const substrait::Plan & plan)
{
    return createExecutor(parse(plan), plan);
}

QueryPlanPtr SerializedPlanParser::parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    auto rel_parser = RelParserFactory::instance().getBuilder(rel.rel_type_case())(parser_context);

    auto all_input_rels = rel_parser->getInputs(rel);
    assert(all_input_rels.empty() || all_input_rels.size() == 1 || all_input_rels.size() == 2);
    std::vector<DB::QueryPlanPtr> input_query_plans;
    rel_stack.push_back(&rel);
    for (const auto * input_rel : all_input_rels)
    {
        auto input_query_plan = parseOp(*input_rel, rel_stack);
        input_query_plans.push_back(std::move(input_query_plan));
    }

    rel_stack.pop_back();

    // source node is special
    if (rel.rel_type_case() == substrait::Rel::RelTypeCase::kRead)
    {
        assert(all_input_rels.empty());
        auto read_rel_parser = std::dynamic_pointer_cast<ReadRelParser>(rel_parser);
        const auto & read = rel.read();
        if (read.has_local_files())
        {
            if (read_rel_parser->isReadRelFromJava(read))
            {
                auto iter = read.local_files().items().at(0).uri_file();
                auto pos = iter.find(':');
                auto iter_index = std::stoi(iter.substr(pos + 1, iter.size()));
                auto [input_iter, materalize_input] = getInputIter(static_cast<size_t>(iter_index));
                read_rel_parser->setInputIter(input_iter, materalize_input);
            }
        }
        else if (read_rel_parser->isReadFromMergeTree(read))
        {
            if (!read.has_extension_table())
            {
                read_rel_parser->setSplitInfo(nextSplitInfo());
            }
        }
        else if (!read.has_local_files() && !read.has_extension_table())
        {
            // read from split files
            auto split_info = nextSplitInfo();
            read_rel_parser->setSplitInfo(split_info);
        }
    }

    DB::QueryPlanPtr query_plan = rel_parser->parse(input_query_plans, rel, rel_stack);
    for (auto & extra_plan : rel_parser->extraPlans())
    {
        extra_plan_holder.push_back(std::move(extra_plan));
    }

    std::vector<DB::IQueryPlanStep *> steps = rel_parser->getSteps();

    if (!parser_context->queryContext()->getSettingsRef()[Setting::query_plan_enable_optimizations])
    {
        if (rel.rel_type_case() == substrait::Rel::RelTypeCase::kRead)
        {
            size_t id = metrics.empty() ? 0 : metrics.back()->getId() + 1;
            metrics.emplace_back(std::make_shared<RelMetric>(id, String(magic_enum::enum_name(rel.rel_type_case())), steps));
        }
        else
            metrics = {std::make_shared<RelMetric>(String(magic_enum::enum_name(rel.rel_type_case())), metrics, steps)};
    }

    return query_plan;
}

DB::QueryPipelineBuilderPtr SerializedPlanParser::buildQueryPipeline(DB::QueryPlan & query_plan) const
{
    const Settings & settings = parser_context->queryContext()->getSettingsRef();
    QueryPriorities priorities;
    const auto query_status = std::make_shared<QueryStatus>(
        parser_context->queryContext(),
        "",
        parser_context->queryContext()->getClientInfo(),
        priorities.insert(settings[Setting::priority]),
        CurrentThread::getGroup(),
        IAST::QueryKind::Select,
        settings,
        0);
    const QueryPlanOptimizationSettings optimization_settings{.optimize_plan = settings[Setting::query_plan_enable_optimizations]};
    BuildQueryPipelineSettings build_settings = BuildQueryPipelineSettings::fromContext(context);
    build_settings.process_list_element = query_status;
    build_settings.progress_callback = nullptr;
    return query_plan.buildQueryPipeline(optimization_settings,build_settings);
}

std::unique_ptr<LocalExecutor> SerializedPlanParser::createExecutor(const std::string_view plan)
{
    const auto s_plan = BinaryToMessage<substrait::Plan>(plan);
    return createExecutor(parse(s_plan), s_plan);
}

std::unique_ptr<LocalExecutor> SerializedPlanParser::createExecutor(DB::QueryPlanPtr query_plan, const substrait::Plan & s_plan) const
{
    Stopwatch stopwatch;

    DB::QueryPipelineBuilderPtr builder = nullptr;
    try
    {
        builder = buildQueryPipeline(*query_plan);
    }
    catch (...)
    {
        debug::dumpPlan(*query_plan, "Invalid clickhouse plan", true);
        throw;
    }

    assert(s_plan.relations_size() == 1);
    const substrait::PlanRel & root_rel = s_plan.relations().at(0);
    assert(root_rel.has_root());
    if (root_rel.root().input().has_write())
        addSinkTransform(parser_context->queryContext(), root_rel.root().input().write(), builder);
    LOG_INFO(getLogger("SerializedPlanParser"), "build pipeline {} ms", stopwatch.elapsedMicroseconds() / 1000.0);

    auto config = ExecutorConfig::loadFromContext(parser_context->queryContext());
    return std::make_unique<LocalExecutor>(std::move(query_plan), std::move(builder), config.dump_pipeline);
}

SerializedPlanParser::SerializedPlanParser(ParserContextPtr parser_context_) : parser_context(parser_context_)
{
    context = parser_context->queryContext();
}

NonNullableColumnsResolver::NonNullableColumnsResolver(
    const Block & header_, ParserContextPtr parser_context_, const substrait::Expression & cond_rel_)
    : header(header_), parser_context(parser_context_), cond_rel(cond_rel_)
{
    expression_parser = std::make_unique<ExpressionParser>(parser_context);
}

// make it simple at present if the condition contains or, return empty for both side.
std::set<String> NonNullableColumnsResolver::resolve()
{
    collected_columns.clear();
    visit(cond_rel);
    return collected_columns;
}

// TODO: make it the same as spark, it's too simple at present.
void NonNullableColumnsResolver::visit(const substrait::Expression & expr)
{
    if (!expr.has_scalar_function())
        return;

    const auto & scalar_function = expr.scalar_function();
    auto function_name = expression_parser->safeGetFunctionName(scalar_function);

    // Only some special functions are used to judge whether the column is non-nullable.
    if (function_name == "and")
    {
        visit(scalar_function.arguments(0).value());
        visit(scalar_function.arguments(1).value());
    }
    else if (function_name == "greaterOrEquals" || function_name == "greater")
    {
        // If it's the case, a > x, what ever x or a is, a and x are non-nullable.
        // a or x may be a column, or a simple expression like plus etc.
        visitNonNullable(scalar_function.arguments(0).value());
        visitNonNullable(scalar_function.arguments(1).value());
    }
    else if (function_name == "lessOrEquals" || function_name == "less")
    {
        // same as gt, gte.
        visitNonNullable(scalar_function.arguments(0).value());
        visitNonNullable(scalar_function.arguments(1).value());
    }
    else if (function_name == "isNotNull")
    {
        visitNonNullable(scalar_function.arguments(0).value());
    }
    // else do nothing
}

void NonNullableColumnsResolver::visitNonNullable(const substrait::Expression & expr)
{
    if (expr.has_scalar_function())
    {
        const auto & scalar_function = expr.scalar_function();
        auto function_name = expression_parser->safeGetFunctionName(scalar_function);
        if (function_name == "plus" || function_name == "minus" || function_name == "multiply" || function_name == "divide")
        {
            visitNonNullable(scalar_function.arguments(0).value());
            visitNonNullable(scalar_function.arguments(1).value());
        }
    }
    else if (expr.has_selection())
    {
        const auto & selection = expr.selection();
        auto column_pos = selection.direct_reference().struct_field().field();
        auto column_name = header.getByPosition(column_pos).name;
        collected_columns.insert(column_name);
    }
    // else, do nothing.
}

}
