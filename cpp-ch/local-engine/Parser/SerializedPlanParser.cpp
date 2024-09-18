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
#include <Columns/ColumnSet.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryPriorities.h>
#include <Join/StorageJoinFromReadBuffer.h>
#include <Operator/BlocksBufferPoolTransform.h>
#include <Parser/FunctionParser.h>
#include <Parser/InputFileNameParser.h>
#include <Parser/LocalExecutor.h>
#include <Parser/RelParsers/ReadRelParser.h>
#include <Parser/RelParsers/RelParser.h>
#include <Parser/RelParsers/WriteRelParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parser/TypeParser.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/printPipeline.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/Output/FileWriterWrappers.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/wrappers.pb.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>
#include <Common/Exception.h>
#include <Common/GlutenConfig.h>
#include <Common/JNIUtils.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{
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

const ActionsDAG::Node * SerializedPlanParser::addColumn(ActionsDAG & actions_dag, const DataTypePtr & type, const Field & field)
{
    return &actions_dag.addColumn(
        ColumnWithTypeAndName(type->createColumnConst(1, field), type, getUniqueName(toString(field).substr(0, 10))));
}


void SerializedPlanParser::parseExtensions(
    const ::google::protobuf::RepeatedPtrField<substrait::extensions::SimpleExtensionDeclaration> & extensions)
{
    for (const auto & extension : extensions)
    {
        if (extension.has_extension_function())
        {
            function_mapping.emplace(
                std::to_string(extension.extension_function().function_anchor()), extension.extension_function().name());
        }
    }
}

ActionsDAG SerializedPlanParser::expressionsToActionsDAG(
    const std::vector<substrait::Expression> & expressions, const Block & header, const Block & read_schema)
{
    ActionsDAG actions_dag{blockToNameAndTypeList(header)};
    NamesWithAliases required_columns;
    std::set<String> distinct_columns;

    for (const auto & expr : expressions)
    {
        if (expr.has_selection())
        {
            auto position = expr.selection().direct_reference().struct_field().field();
            auto col_name = read_schema.getByPosition(position).name;
            const ActionsDAG::Node * field = actions_dag.tryFindInOutputs(col_name);
            if (distinct_columns.contains(field->result_name))
            {
                auto unique_name = getUniqueName(field->result_name);
                required_columns.emplace_back(NameWithAlias(field->result_name, unique_name));
                distinct_columns.emplace(unique_name);
            }
            else
            {
                required_columns.emplace_back(NameWithAlias(field->result_name, field->result_name));
                distinct_columns.emplace(field->result_name);
            }
        }
        else if (expr.has_scalar_function())
        {
            const auto & scalar_function = expr.scalar_function();
            auto function_signature = function_mapping.at(std::to_string(scalar_function.function_reference()));

            std::vector<String> result_names;
            if (startsWith(function_signature, "explode:"))
                parseArrayJoinWithDAG(expr, result_names, actions_dag, true, false);
            else if (startsWith(function_signature, "posexplode:"))
                parseArrayJoinWithDAG(expr, result_names, actions_dag, true, true);
            else if (startsWith(function_signature, "json_tuple:"))
                parseJsonTuple(expr, result_names, actions_dag, true, false);
            else
            {
                result_names.resize(1);
                parseFunctionWithDAG(expr, result_names[0], actions_dag, true);
            }

            for (const auto & result_name : result_names)
            {
                if (result_name.empty())
                    continue;

                if (distinct_columns.contains(result_name))
                {
                    auto unique_name = getUniqueName(result_name);
                    required_columns.emplace_back(NameWithAlias(result_name, unique_name));
                    distinct_columns.emplace(unique_name);
                }
                else
                {
                    required_columns.emplace_back(NameWithAlias(result_name, result_name));
                    distinct_columns.emplace(result_name);
                }
            }
        }
        else if (expr.has_cast() || expr.has_if_then() || expr.has_literal() || expr.has_singular_or_list())
        {
            const auto * node = parseExpression(actions_dag, expr);
            actions_dag.addOrReplaceInOutputs(*node);
            if (distinct_columns.contains(node->result_name))
            {
                auto unique_name = getUniqueName(node->result_name);
                required_columns.emplace_back(NameWithAlias(node->result_name, unique_name));
                distinct_columns.emplace(unique_name);
            }
            else
            {
                required_columns.emplace_back(NameWithAlias(node->result_name, node->result_name));
                distinct_columns.emplace(node->result_name);
            }
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported projection type {}.", magic_enum::enum_name(expr.rex_type_case()));
    }
    actions_dag.project(required_columns);
    actions_dag.appendInputsForUnusedColumns(header);
    return actions_dag;
}

std::string getDecimalFunction(const substrait::Type_Decimal & decimal, bool null_on_overflow)
{
    std::string ch_function_name;
    UInt32 precision = decimal.precision();

    if (precision <= DataTypeDecimal32::maxPrecision())
        ch_function_name = "toDecimal32";
    else if (precision <= DataTypeDecimal64::maxPrecision())
        ch_function_name = "toDecimal64";
    else if (precision <= DataTypeDecimal128::maxPrecision())
        ch_function_name = "toDecimal128";
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);

    if (null_on_overflow)
        ch_function_name = ch_function_name + "OrNull";
    return ch_function_name;
}

IQueryPlanStep * SerializedPlanParser::addRemoveNullableStep(QueryPlan & plan, const std::set<String> & columns)
{
    if (columns.empty())
        return nullptr;

    ActionsDAG remove_nullable_actions_dag{blockToNameAndTypeList(plan.getCurrentDataStream().header)};
    removeNullableForRequiredColumns(columns, remove_nullable_actions_dag);
    auto expression_step = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), std::move(remove_nullable_actions_dag));
    expression_step->setStepDescription("Remove nullable properties");
    auto * step_ptr = expression_step.get();
    plan.addStep(std::move(expression_step));
    return step_ptr;
}

IQueryPlanStep * SerializedPlanParser::addRollbackFilterHeaderStep(QueryPlanPtr & query_plan, const Block & input_header)
{
    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
        query_plan->getCurrentDataStream().header.getColumnsWithTypeAndName(),
        input_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);
    auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), std::move(convert_actions_dag));
    expression_step->setStepDescription("Generator for rollback filter");
    auto * step_ptr = expression_step.get();
    query_plan->addStep(std::move(expression_step));
    return step_ptr;
}

void adjustOutput(const DB::QueryPlanPtr & query_plan, const substrait::PlanRel & root_rel)
{
    if (root_rel.root().names_size())
    {
        ActionsDAG actions_dag{blockToNameAndTypeList(query_plan->getCurrentDataStream().header)};
        NamesWithAliases aliases;
        auto cols = query_plan->getCurrentDataStream().header.getNamesAndTypesList();
        if (cols.getNames().size() != static_cast<size_t>(root_rel.root().names_size()))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Missmatch result columns size. plan column size {}, subtrait plan size {}.",
                cols.getNames().size(),
                root_rel.root().names_size());
        for (int i = 0; i < static_cast<int>(cols.getNames().size()); i++)
            aliases.emplace_back(NameWithAlias(cols.getNames()[i], root_rel.root().names(i)));
        actions_dag.project(aliases);
        auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), std::move(actions_dag));
        expression_step->setStepDescription("Rename Output");
        query_plan->addStep(std::move(expression_step));
    }

    // fixes: issue-1874, to keep the nullability as expected.
    const auto & output_schema = root_rel.root().output_schema();
    if (output_schema.types_size())
    {
        auto original_header = query_plan->getCurrentDataStream().header;
        const auto & original_cols = original_header.getColumnsWithTypeAndName();
        if (static_cast<size_t>(output_schema.types_size()) != original_cols.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mismatch output schema");
        bool need_final_project = false;
        ColumnsWithTypeAndName final_cols;
        for (int i = 0; i < output_schema.types_size(); ++i)
        {
            const auto & col = original_cols[i];
            auto type = TypeParser::parseType(output_schema.types(i));
            // At present, we only check nullable mismatch.
            // intermediate aggregate data is special, no check here.
            if (type->isNullable() != col.type->isNullable() && !typeid_cast<const DataTypeAggregateFunction *>(col.type.get()))
            {
                if (type->isNullable())
                {
                    auto wrapped = wrapNullableType(true, col.type);
                    final_cols.emplace_back(type->createColumn(), wrapped, col.name);
                    need_final_project = !wrapped->equals(*col.type);
                }
                else
                {
                    final_cols.emplace_back(type->createColumn(), removeNullable(col.type), col.name);
                    need_final_project = true;
                }
            }
            else
            {
                final_cols.push_back(col);
            }
        }
        if (need_final_project)
        {
            ActionsDAG final_project = ActionsDAG::makeConvertingActions(original_cols, final_cols, ActionsDAG::MatchColumnsMode::Position);
            QueryPlanStepPtr final_project_step
                = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), std::move(final_project));
            final_project_step->setStepDescription("Project for output schema");
            query_plan->addStep(std::move(final_project_step));
        }
    }
}

QueryPlanPtr SerializedPlanParser::parse(const substrait::Plan & plan)
{
    logDebugMessage(plan, "substrait plan");
    parseExtensions(plan.extensions());
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
        adjustOutput(query_plan, root_rel);

#ifndef NDEBUG
    PlanUtil::checkOuputType(*query_plan);
#endif

    if (auto * logger = &Poco::Logger::get("SerializedPlanParser"); logger->debug())
    {
        auto out = PlanUtil::explainPlan(*query_plan);
        LOG_DEBUG(logger, "clickhouse plan:\n{}", out);
    }

    return query_plan;
}

std::unique_ptr<LocalExecutor> SerializedPlanParser::createExecutor(const substrait::Plan & plan)
{
    return createExecutor(parse(plan), plan);
}

QueryPlanPtr SerializedPlanParser::parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    DB::QueryPlanPtr query_plan;
    auto rel_parser = RelParserFactory::instance().getBuilder(rel.rel_type_case())(this);

    auto all_input_rels = rel_parser->getInputs(rel);
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
    }

    query_plan = rel_parser->parse(input_query_plans, rel, rel_stack);

    std::vector<DB::IQueryPlanStep *> steps = rel_parser->getSteps();

    if (!context->getSettingsRef().query_plan_enable_optimizations)
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

std::optional<String> SerializedPlanParser::getFunctionSignatureName(UInt32 function_ref) const
{
    auto it = function_mapping.find(std::to_string(function_ref));
    if (it == function_mapping.end())
        return {};
    auto function_signature = it->second;
    auto pos = function_signature.find(':');
    return function_signature.substr(0, pos);
}

std::string
SerializedPlanParser::getFunctionName(const std::string & function_signature, const substrait::Expression_ScalarFunction & function)
{
    auto args = function.arguments();
    auto pos = function_signature.find(':');
    auto function_name = function_signature.substr(0, pos);
    auto function_parser = FunctionParserFactory::instance().tryGet(function_name, this);
    if (!function_parser)
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_FUNCTION, "Unsupported function: {}", function_name);
    return function_parser->getCHFunctionName(function);
}

void SerializedPlanParser::parseArrayJoinArguments(
    ActionsDAG & actions_dag,
    const std::string & function_name,
    const substrait::Expression_ScalarFunction & scalar_function,
    bool position,
    ActionsDAG::NodeRawConstPtrs & parsed_args,
    bool & is_map)
{
    if (function_name != "arrayJoin")
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Function parseArrayJoinArguments should only process arrayJoin function instead of {}",
            function_name);

    /// The argument number of arrayJoin(converted from Spark explode/posexplode) should be 1
    if (scalar_function.arguments_size() != 1)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Argument number of arrayJoin should be 1 instead of {}", scalar_function.arguments_size());

    parseFunctionArguments(actions_dag, parsed_args, scalar_function);

    auto arg = parsed_args[0];
    auto arg_type = removeNullable(arg->result_type);
    if (isMap(arg_type))
        is_map = true;
    else if (isArray(arg_type))
        is_map = false;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument type of arrayJoin should be Array or Map but is {}", arg_type->getName());

    /// Remove Nullable for input argument of arrayJoin function because arrayJoin function only accept non-nullable input
    /// array() or map()
    const auto * empty_node = addColumn(actions_dag, arg_type, is_map ? Field(Map()) : Field(Array()));
    /// ifNull(arg, array()) or ifNull(arg, map())
    const auto * if_null_node = toFunctionNode(actions_dag, "ifNull", {arg, empty_node});
    /// assumeNotNull(ifNull(arg, array())) or assumeNotNull(ifNull(arg, map()))
    const auto * not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {if_null_node});
    /// Wrap with materalize function to make sure column input to ARRAY JOIN STEP is materaized
    arg = &actions_dag.materializeNode(*not_null_node);

    /// If spark function is posexplode, we need to add position column together with input argument
    if (position)
    {
        /// length(arg)
        const auto * length_node = toFunctionNode(actions_dag, "length", {arg});
        /// range(length(arg))
        const auto * range_node = toFunctionNode(actions_dag, "range", {length_node});
        /// mapFromArrays(range(length(arg)), arg)
        arg = toFunctionNode(actions_dag, "mapFromArrays", {range_node, arg});
    }

    parsed_args[0] = arg;
}

ActionsDAG::NodeRawConstPtrs SerializedPlanParser::parseArrayJoinWithDAG(
    const substrait::Expression & rel, std::vector<String> & result_names, ActionsDAG & actions_dag, bool keep_result, bool position)
{
    if (!rel.has_scalar_function())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The root of expression should be a scalar function:\n {}", rel.DebugString());

    const auto & scalar_function = rel.scalar_function();

    auto function_signature = function_mapping.at(std::to_string(rel.scalar_function().function_reference()));
    String function_name = "arrayJoin";

    /// Whether the input argument of explode/posexplode is map type
    bool is_map;
    ActionsDAG::NodeRawConstPtrs args;
    parseArrayJoinArguments(actions_dag, function_name, scalar_function, position, args, is_map);

    /// Note: Make sure result_name keep the same after applying arrayJoin function, which makes it much easier to transform arrayJoin function to ARRAY JOIN STEP
    /// Otherwise an alias node must be appended after ARRAY JOIN STEP, which is not a graceful implementation.
    const auto & arg_not_null = args[0];
    auto array_join_name = arg_not_null->result_name;
    /// arrayJoin(arg_not_null)
    const auto * array_join_node = &actions_dag.addArrayJoin(*arg_not_null, array_join_name);

    auto tuple_element_builder = FunctionFactory::instance().get("sparkTupleElement", context);
    auto tuple_index_type = std::make_shared<DataTypeUInt32>();
    auto add_tuple_element = [&](const ActionsDAG::Node * tuple_node, size_t i) -> const ActionsDAG::Node *
    {
        ColumnWithTypeAndName index_col(tuple_index_type->createColumnConst(1, i), tuple_index_type, getUniqueName(std::to_string(i)));
        const auto * index_node = &actions_dag.addColumn(std::move(index_col));
        auto result_name = "sparkTupleElement(" + tuple_node->result_name + ", " + index_node->result_name + ")";
        return &actions_dag.addFunction(tuple_element_builder, {tuple_node, index_node}, result_name);
    };

    /// Special process to keep compatiable with Spark
    if (!position)
    {
        /// Spark: explode(array_or_map) -> CH: arrayJoin(array_or_map)
        if (is_map)
        {
            /// In Spark: explode(map(k, v)) output 2 columns with default names "key" and "value"
            /// In CH: arrayJoin(map(k, v)) output 1 column with Tuple Type.
            /// So we must wrap arrayJoin with sparkTupleElement function for compatiability.

            /// arrayJoin(arg_not_null).1
            const auto * key_node = add_tuple_element(array_join_node, 1);

            /// arrayJoin(arg_not_null).2
            const auto * val_node = add_tuple_element(array_join_node, 2);

            result_names.push_back(key_node->result_name);
            result_names.push_back(val_node->result_name);
            if (keep_result)
            {
                actions_dag.addOrReplaceInOutputs(*key_node);
                actions_dag.addOrReplaceInOutputs(*val_node);
            }
            return {key_node, val_node};
        }
        else
        {
            result_names.push_back(array_join_name);
            if (keep_result)
                actions_dag.addOrReplaceInOutputs(*array_join_node);
            return {array_join_node};
        }
    }
    else
    {
        /// Spark: posexplode(array_or_map) -> CH: arrayJoin(map), in which map = mapFromArrays(range(length(array_or_map)), array_or_map)

        /// In Spark: posexplode(array_of_map) output 2 or 3 columns: (pos, col) or (pos, key, value)
        /// In CH: arrayJoin(map(k, v)) output 1 column with Tuple Type.
        /// So we must wrap arrayJoin with sparkTupleElement function for compatiability.

        /// pos = cast(arrayJoin(arg_not_null).1, "Int32")
        const auto * pos_node = add_tuple_element(array_join_node, 1);
        pos_node = ActionsDAGUtil::convertNodeType(actions_dag, pos_node, INT());

        /// if is_map is false, output col = arrayJoin(arg_not_null).2
        /// if is_map is true,  output (key, value) = arrayJoin(arg_not_null).2
        const auto * item_node = add_tuple_element(array_join_node, 2);

        if (is_map)
        {
            /// key = arrayJoin(arg_not_null).2.1
            const auto * key_node = add_tuple_element(item_node, 1);

            /// value = arrayJoin(arg_not_null).2.2
            const auto * value_node = add_tuple_element(item_node, 2);

            result_names.push_back(pos_node->result_name);
            result_names.push_back(key_node->result_name);
            result_names.push_back(value_node->result_name);
            if (keep_result)
            {
                actions_dag.addOrReplaceInOutputs(*pos_node);
                actions_dag.addOrReplaceInOutputs(*key_node);
                actions_dag.addOrReplaceInOutputs(*value_node);
            }

            return {pos_node, key_node, value_node};
        }
        else
        {
            /// col = arrayJoin(arg_not_null).2
            result_names.push_back(pos_node->result_name);
            result_names.push_back(item_node->result_name);
            if (keep_result)
            {
                actions_dag.addOrReplaceInOutputs(*pos_node);
                actions_dag.addOrReplaceInOutputs(*item_node);
            }
            return {pos_node, item_node};
        }
    }
}

const ActionsDAG::Node * SerializedPlanParser::parseFunctionWithDAG(
    const substrait::Expression & rel, std::string & result_name, ActionsDAG & actions_dag, bool keep_result)
{
    if (!rel.has_scalar_function())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "the root of expression should be a scalar function:\n {}", rel.DebugString());

    const auto & scalar_function = rel.scalar_function();
    auto function_signature = function_mapping.at(std::to_string(scalar_function.function_reference()));

    /// If the substrait function name is registered in FunctionParserFactory, use it to parse the function, and return result directly
    auto pos = function_signature.find(':');
    auto func_name = function_signature.substr(0, pos);

    auto func_parser = FunctionParserFactory::instance().tryGet(func_name, this);
    if (!func_parser)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Not found function parser for {}", func_name);
    LOG_DEBUG(&Poco::Logger::get("SerializedPlanParser"), "parse function {} by function parser: {}", func_name, func_parser->getName());
    const auto * result_node = func_parser->parse(scalar_function, actions_dag);
    if (keep_result)
        actions_dag.addOrReplaceInOutputs(*result_node);

    result_name = result_node->result_name;
    return result_node;
}

void SerializedPlanParser::parseFunctionArguments(
    ActionsDAG & actions_dag, ActionsDAG::NodeRawConstPtrs & parsed_args, const substrait::Expression_ScalarFunction & scalar_function)
{
    auto function_signature = function_mapping.at(std::to_string(scalar_function.function_reference()));
    const auto & args = scalar_function.arguments();
    parsed_args.reserve(args.size());
    for (const auto & arg : args)
        parsed_args.emplace_back(parseExpression(actions_dag, arg.value()));
}

// Convert signed integer index into unsigned integer index
std::pair<DataTypePtr, Field> SerializedPlanParser::convertStructFieldType(const DataTypePtr & type, const Field & field)
{
    // For tupelElement, field index starts from 1, but int substrait plan, it starts from 0.
#define UINT_CONVERT(type_ptr, field, type_name) \
    if ((type_ptr)->getTypeId() == TypeIndex::type_name) \
    { \
        return {std::make_shared<DataTypeU##type_name>(), static_cast<U##type_name>((field).safeGet<type_name>()) + 1}; \
    }

    auto type_id = type->getTypeId();
    if (type_id == TypeIndex::UInt8 || type_id == TypeIndex::UInt16 || type_id == TypeIndex::UInt32 || type_id == TypeIndex::UInt64)
        return {type, field};
    UINT_CONVERT(type, field, Int8)
    UINT_CONVERT(type, field, Int16)
    UINT_CONVERT(type, field, Int32)
    UINT_CONVERT(type, field, Int64)
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Not valid interger type: {}", type->getName());
#undef UINT_CONVERT
}

bool SerializedPlanParser::isFunction(substrait::Expression_ScalarFunction rel, String function_name)
{
    auto func_signature = function_mapping[std::to_string(rel.function_reference())];
    return func_signature.starts_with(function_name + ":");
}

void SerializedPlanParser::parseFunctionOrExpression(
    const substrait::Expression & rel, std::string & result_name, ActionsDAG & actions_dag, bool keep_result)
{
    if (rel.has_scalar_function())
        parseFunctionWithDAG(rel, result_name, actions_dag, keep_result);
    else
    {
        const auto * result_node = parseExpression(actions_dag, rel);
        result_name = result_node->result_name;
    }
}

void SerializedPlanParser::parseJsonTuple(
    const substrait::Expression & rel, std::vector<String> & result_names, ActionsDAG & actions_dag, bool keep_result, bool)
{
    const auto & scalar_function = rel.scalar_function();
    auto function_signature = function_mapping.at(std::to_string(rel.scalar_function().function_reference()));
    String function_name = "json_tuple";
    auto args = scalar_function.arguments();
    if (args.size() < 2)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The function json_tuple should has at least 2 arguments.");
    }
    auto first_arg = args[0].value();
    const ActionsDAG::Node * json_expr_node = parseExpression(actions_dag, first_arg);
    std::string extract_expr = "Tuple(";
    for (int i = 1; i < args.size(); i++)
    {
        auto arg_value = args[i].value();
        if (!arg_value.has_literal())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The arguments of function {} must be string literal", function_name);
        }

        Field f = arg_value.literal().string();
        std::string s;
        if (f.tryGet(s))
        {
            extract_expr.append(s).append(" Nullable(String)");
            if (i != args.size() - 1)
            {
                extract_expr.append(",");
            }
        }
    }
    extract_expr.append(")");
    const ActionsDAG::Node * extract_expr_node = addColumn(actions_dag, std::make_shared<DataTypeString>(), extract_expr);
    auto json_extract_builder = FunctionFactory::instance().get("JSONExtract", context);
    auto json_extract_result_name = "JSONExtract(" + json_expr_node->result_name + "," + extract_expr_node->result_name + ")";
    const ActionsDAG::Node * json_extract_node
        = &actions_dag.addFunction(json_extract_builder, {json_expr_node, extract_expr_node}, json_extract_result_name);
    auto tuple_element_builder = FunctionFactory::instance().get("sparkTupleElement", context);
    auto tuple_index_type = std::make_shared<DataTypeUInt32>();
    auto add_tuple_element = [&](const ActionsDAG::Node * tuple_node, size_t i) -> const ActionsDAG::Node *
    {
        ColumnWithTypeAndName index_col(tuple_index_type->createColumnConst(1, i), tuple_index_type, getUniqueName(std::to_string(i)));
        const auto * index_node = &actions_dag.addColumn(std::move(index_col));
        auto result_name = "sparkTupleElement(" + tuple_node->result_name + ", " + index_node->result_name + ")";
        return &actions_dag.addFunction(tuple_element_builder, {tuple_node, index_node}, result_name);
    };
    for (int i = 1; i < args.size(); i++)
    {
        const ActionsDAG::Node * tuple_node = add_tuple_element(json_extract_node, i);
        if (keep_result)
        {
            actions_dag.addOrReplaceInOutputs(*tuple_node);
            result_names.push_back(tuple_node->result_name);
        }
    }
}

const ActionsDAG::Node *
SerializedPlanParser::toFunctionNode(ActionsDAG & actions_dag, const String & function, const ActionsDAG::NodeRawConstPtrs & args)
{
    auto function_builder = FunctionFactory::instance().get(function, context);
    std::string args_name = join(args, ',');
    auto result_name = function + "(" + args_name + ")";
    const auto * function_node = &actions_dag.addFunction(function_builder, args, result_name);
    return function_node;
}

std::pair<DataTypePtr, Field> SerializedPlanParser::parseLiteral(const substrait::Expression_Literal & literal)
{
    DataTypePtr type;
    Field field;

    switch (literal.literal_type_case())
    {
        case substrait::Expression_Literal::kFp64: {
            type = std::make_shared<DataTypeFloat64>();
            field = literal.fp64();
            break;
        }
        case substrait::Expression_Literal::kFp32: {
            type = std::make_shared<DataTypeFloat32>();
            field = literal.fp32();
            break;
        }
        case substrait::Expression_Literal::kString: {
            type = std::make_shared<DataTypeString>();
            field = literal.string();
            break;
        }
        case substrait::Expression_Literal::kBinary: {
            type = std::make_shared<DataTypeString>();
            field = literal.binary();
            break;
        }
        case substrait::Expression_Literal::kI64: {
            type = std::make_shared<DataTypeInt64>();
            field = literal.i64();
            break;
        }
        case substrait::Expression_Literal::kI32: {
            type = std::make_shared<DataTypeInt32>();
            field = literal.i32();
            break;
        }
        case substrait::Expression_Literal::kBoolean: {
            type = std::make_shared<DataTypeUInt8>();
            field = literal.boolean() ? UInt8(1) : UInt8(0);
            break;
        }
        case substrait::Expression_Literal::kI16: {
            type = std::make_shared<DataTypeInt16>();
            field = literal.i16();
            break;
        }
        case substrait::Expression_Literal::kI8: {
            type = std::make_shared<DataTypeInt8>();
            field = literal.i8();
            break;
        }
        case substrait::Expression_Literal::kDate: {
            type = std::make_shared<DataTypeDate32>();
            field = literal.date();
            break;
        }
        case substrait::Expression_Literal::kTimestamp: {
            type = std::make_shared<DataTypeDateTime64>(6);
            field = DecimalField<DateTime64>(literal.timestamp(), 6);
            break;
        }
        case substrait::Expression_Literal::kDecimal: {
            UInt32 precision = literal.decimal().precision();
            UInt32 scale = literal.decimal().scale();
            const auto & bytes = literal.decimal().value();

            if (precision <= DataTypeDecimal32::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal32>(precision, scale);
                auto value = *reinterpret_cast<const Int32 *>(bytes.data());
                field = DecimalField<Decimal32>(value, scale);
            }
            else if (precision <= DataTypeDecimal64::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal64>(precision, scale);
                auto value = *reinterpret_cast<const Int64 *>(bytes.data());
                field = DecimalField<Decimal64>(value, scale);
            }
            else if (precision <= DataTypeDecimal128::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal128>(precision, scale);
                String bytes_copy(bytes);
                auto value = *reinterpret_cast<Decimal128 *>(bytes_copy.data());
                field = DecimalField<Decimal128>(value, scale);
            }
            else
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);
            break;
        }
        case substrait::Expression_Literal::kList: {
            const auto & values = literal.list().values();
            if (values.empty())
            {
                type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());
                field = Array();
                break;
            }

            DataTypePtr common_type;
            std::tie(common_type, std::ignore) = parseLiteral(values[0]);
            size_t list_len = values.size();
            Array array(list_len);
            for (int i = 0; i < static_cast<int>(list_len); ++i)
            {
                auto type_and_field = parseLiteral(values[i]);
                common_type = getLeastSupertype(DataTypes{common_type, type_and_field.first});
                array[i] = std::move(type_and_field.second);
            }

            type = std::make_shared<DataTypeArray>(common_type);
            field = std::move(array);
            break;
        }
        case substrait::Expression_Literal::kEmptyList: {
            type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());
            field = Array();
            break;
        }
        case substrait::Expression_Literal::kMap: {
            const auto & key_values = literal.map().key_values();
            if (key_values.empty())
            {
                type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeNothing>(), std::make_shared<DataTypeNothing>());
                field = Map();
                break;
            }

            const auto & first_key_value = key_values[0];

            DataTypePtr common_key_type;
            std::tie(common_key_type, std::ignore) = parseLiteral(first_key_value.key());

            DataTypePtr common_value_type;
            std::tie(common_value_type, std::ignore) = parseLiteral(first_key_value.value());

            Map map;
            map.reserve(key_values.size());
            for (const auto & key_value : key_values)
            {
                Tuple tuple(2);

                DataTypePtr key_type;
                std::tie(key_type, tuple[0]) = parseLiteral(key_value.key());
                /// Each key should has the same type
                if (!common_key_type->equals(*key_type))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Literal map key type mismatch:{} and {}",
                        common_key_type->getName(),
                        key_type->getName());

                DataTypePtr value_type;
                std::tie(value_type, tuple[1]) = parseLiteral(key_value.value());
                /// Each value should has least super type for all of them
                common_value_type = getLeastSupertype(DataTypes{common_value_type, value_type});

                map.emplace_back(std::move(tuple));
            }

            type = std::make_shared<DataTypeMap>(common_key_type, common_value_type);
            field = std::move(map);
            break;
        }
        case substrait::Expression_Literal::kEmptyMap: {
            type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeNothing>(), std::make_shared<DataTypeNothing>());
            field = Map();
            break;
        }
        case substrait::Expression_Literal::kStruct: {
            const auto & fields = literal.struct_().fields();

            DataTypes types;
            types.reserve(fields.size());
            Tuple tuple;
            tuple.reserve(fields.size());
            for (const auto & f : fields)
            {
                DataTypePtr field_type;
                Field field_value;
                std::tie(field_type, field_value) = parseLiteral(f);

                types.emplace_back(std::move(field_type));
                tuple.emplace_back(std::move(field_value));
            }

            type = std::make_shared<DataTypeTuple>(types);
            field = std::move(tuple);
            break;
        }
        case substrait::Expression_Literal::kNull: {
            type = TypeParser::parseType(literal.null());
            field = Field{};
            break;
        }
        default: {
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE, "Unsupported spark literal type {}", magic_enum::enum_name(literal.literal_type_case()));
        }
    }
    return std::make_pair(std::move(type), std::move(field));
}

const ActionsDAG::Node * SerializedPlanParser::parseExpression(ActionsDAG & actions_dag, const substrait::Expression & rel)
{
    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            DataTypePtr type;
            Field field;
            std::tie(type, field) = parseLiteral(rel.literal());
            return addColumn(actions_dag, type, field);
        }

        case substrait::Expression::RexTypeCase::kSelection: {
            if (!rel.selection().has_direct_reference() || !rel.selection().direct_reference().has_struct_field())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can only have direct struct references in selections");

            const auto * field = actions_dag.getInputs()[rel.selection().direct_reference().struct_field().field()];
            return actions_dag.tryFindInOutputs(field->result_name);
        }

        case substrait::Expression::RexTypeCase::kCast: {
            if (!rel.cast().has_type() || !rel.cast().has_input())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Doesn't have type or input in cast node.");
            ActionsDAG::NodeRawConstPtrs args;

            String cast_function = "CAST";
            const auto & input = rel.cast().input();
            args.emplace_back(parseExpression(actions_dag, input));

            const auto & substrait_type = rel.cast().type();
            const auto & input_type = args[0]->result_type;
            DataTypePtr non_nullable_input_type = removeNullable(input_type);
            DataTypePtr output_type = TypeParser::parseType(substrait_type);
            DataTypePtr non_nullable_output_type = removeNullable(output_type);

            const ActionsDAG::Node * function_node = nullptr;
            if (substrait_type.has_binary())
            {
                /// Spark cast(x as BINARY) -> CH reinterpretAsStringSpark(x)
                function_node = toFunctionNode(actions_dag, "reinterpretAsStringSpark", args);
            }
            else if (isString(non_nullable_input_type) && isDate32(non_nullable_output_type))
                function_node = toFunctionNode(actions_dag, "sparkToDate", args);
            else if (isString(non_nullable_input_type) && isDateTime64(non_nullable_output_type))
                function_node = toFunctionNode(actions_dag, "sparkToDateTime", args);
            else if (isDecimal(non_nullable_input_type) && isString(non_nullable_output_type))
            {
                /// Spark cast(x as STRING) if x is Decimal -> CH toDecimalString(x, scale)
                UInt8 scale = getDecimalScale(*non_nullable_input_type);
                args.emplace_back(addColumn(actions_dag, std::make_shared<DataTypeUInt8>(), Field(scale)));
                function_node = toFunctionNode(actions_dag, "toDecimalString", args);
            }
            else if (isFloat(non_nullable_input_type) && isInt(non_nullable_output_type))
            {
                String function_name = "sparkCastFloatTo" + non_nullable_output_type->getName();
                function_node = toFunctionNode(actions_dag, function_name, args);
            }
            else
            {
                if (isString(non_nullable_input_type) && isInt(non_nullable_output_type))
                {
                    /// Spark cast(x as INT) if x is String -> CH cast(trim(x) as INT)
                    /// Refer to https://github.com/apache/incubator-gluten/issues/4956
                    args[0] = toFunctionNode(actions_dag, "trim", {args[0]});
                }
                else if (isString(non_nullable_input_type) && substrait_type.has_bool_())
                {
                    /// cast(string to boolean)
                    cast_function = "accurateCastOrNull";
                }

                /// Common process
                args.emplace_back(addColumn(actions_dag, std::make_shared<DataTypeString>(), output_type->getName()));
                function_node = toFunctionNode(actions_dag, cast_function, args);
            }

            actions_dag.addOrReplaceInOutputs(*function_node);
            return function_node;
        }

        case substrait::Expression::RexTypeCase::kIfThen: {
            const auto & if_then = rel.if_then();
            FunctionOverloadResolverPtr function_ptr = nullptr;
            auto condition_nums = if_then.ifs_size();
            if (condition_nums == 1)
                function_ptr = FunctionFactory::instance().get("if", context);
            else
                function_ptr = FunctionFactory::instance().get("multiIf", context);
            ActionsDAG::NodeRawConstPtrs args;

            for (int i = 0; i < condition_nums; ++i)
            {
                const auto & ifs = if_then.ifs(i);
                const auto * if_node = parseExpression(actions_dag, ifs.if_());
                args.emplace_back(if_node);

                const auto * then_node = parseExpression(actions_dag, ifs.then());
                args.emplace_back(then_node);
            }

            const auto * else_node = parseExpression(actions_dag, if_then.else_());
            args.emplace_back(else_node);
            std::string args_name = join(args, ',');
            std::string result_name;
            if (condition_nums == 1)
                result_name = "if(" + args_name + ")";
            else
                result_name = "multiIf(" + args_name + ")";
            const auto * function_node = &actions_dag.addFunction(function_ptr, args, result_name);
            actions_dag.addOrReplaceInOutputs(*function_node);
            return function_node;
        }

        case substrait::Expression::RexTypeCase::kScalarFunction: {
            std::string result;
            return parseFunctionWithDAG(rel, result, actions_dag, false);
        }

        case substrait::Expression::RexTypeCase::kSingularOrList: {
            const auto & options = rel.singular_or_list().options();
            /// options is empty always return false
            if (options.empty())
                return addColumn(actions_dag, std::make_shared<DataTypeUInt8>(), 0);
            /// options should be literals
            if (!options[0].has_literal())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Options of SingularOrList must have literal type");

            ActionsDAG::NodeRawConstPtrs args;
            args.emplace_back(parseExpression(actions_dag, rel.singular_or_list().value()));

            bool nullable = false;
            int options_len = static_cast<int>(options.size());
            for (int i = 0; i < options_len; ++i)
            {
                if (!options[i].has_literal())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "in expression values must be the literal!");
                if (!nullable)
                    nullable = options[i].literal().has_null();
            }

            DataTypePtr elem_type;
            std::tie(elem_type, std::ignore) = parseLiteral(options[0].literal());
            elem_type = wrapNullableType(nullable, elem_type);

            MutableColumnPtr elem_column = elem_type->createColumn();
            elem_column->reserve(options_len);
            for (int i = 0; i < options_len; ++i)
            {
                auto type_and_field = parseLiteral(options[i].literal());
                auto option_type = wrapNullableType(nullable, type_and_field.first);
                if (!elem_type->equals(*option_type))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "SingularOrList options type mismatch:{} and {}",
                        elem_type->getName(),
                        option_type->getName());

                elem_column->insert(type_and_field.second);
            }

            MutableColumns elem_columns;
            elem_columns.emplace_back(std::move(elem_column));

            auto name = getUniqueName("__set");
            Block elem_block;
            elem_block.insert(ColumnWithTypeAndName(nullptr, elem_type, name));
            elem_block.setColumns(std::move(elem_columns));

            auto future_set = std::make_shared<FutureSetFromTuple>(elem_block, context->getSettingsRef());
            auto arg = ColumnSet::create(1, std::move(future_set));
            args.emplace_back(&actions_dag.addColumn(ColumnWithTypeAndName(std::move(arg), std::make_shared<DataTypeSet>(), name)));

            const auto * function_node = toFunctionNode(actions_dag, "in", args);
            actions_dag.addOrReplaceInOutputs(*function_node);
            if (nullable)
            {
                /// if sets has `null` and value not in sets
                /// In Spark: return `null`, is the standard behaviour from ANSI.(SPARK-37920)
                /// In CH: return `false`
                /// So we used if(a, b, c) cast `false` to `null` if sets has `null`
                auto type = wrapNullableType(true, function_node->result_type);
                ActionsDAG::NodeRawConstPtrs cast_args(
                    {function_node, addColumn(actions_dag, type, true), addColumn(actions_dag, type, Field())});
                auto cast = FunctionFactory::instance().get("if", context);
                function_node = toFunctionNode(actions_dag, "if", cast_args);
                actions_dag.addOrReplaceInOutputs(*function_node);
            }
            return function_node;
        }

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported spark expression type {} : {}",
                magic_enum::enum_name(rel.rex_type_case()),
                rel.DebugString());
    }
}

DB::QueryPipelineBuilderPtr SerializedPlanParser::buildQueryPipeline(DB::QueryPlan & query_plan)
{
    const Settings & settings = context->getSettingsRef();
    QueryPriorities priorities;
    const auto query_status = std::make_shared<QueryStatus>(
        context,
        "",
        context->getClientInfo(),
        priorities.insert(settings.priority),
        CurrentThread::getGroup(),
        IAST::QueryKind::Select,
        settings,
        0);
    const QueryPlanOptimizationSettings optimization_settings{.optimize_plan = settings.query_plan_enable_optimizations};
    return query_plan.buildQueryPipeline(
        optimization_settings,
        BuildQueryPipelineSettings{
            .actions_settings
            = ExpressionActionsSettings{.can_compile_expressions = true, .min_count_to_compile_expression = 3, .compile_expressions = CompileExpressions::yes},
            .process_list_element = query_status});
}

std::unique_ptr<LocalExecutor> SerializedPlanParser::createExecutor(const std::string_view plan)
{
    const auto s_plan = BinaryToMessage<substrait::Plan>(plan);
    return createExecutor(parse(s_plan), s_plan);
}

std::unique_ptr<LocalExecutor> SerializedPlanParser::createExecutor(DB::QueryPlanPtr query_plan, const substrait::Plan & s_plan)
{
    Stopwatch stopwatch;

    const Settings & settings = context->getSettingsRef();
    auto builder = buildQueryPipeline(*query_plan);


    assert(s_plan.relations_size() == 1);
    const substrait::PlanRel & root_rel = s_plan.relations().at(0);
    assert(root_rel.has_root());
    if (root_rel.root().input().has_write())
        addSinkTransform(context, root_rel.root().input().write(), builder);
    auto * logger = &Poco::Logger::get("SerializedPlanParser");
    LOG_INFO(logger, "build pipeline {} ms", stopwatch.elapsedMicroseconds() / 1000.0);
    LOG_DEBUG(
        logger, "clickhouse plan [optimization={}]:\n{}", settings.query_plan_enable_optimizations, PlanUtil::explainPlan(*query_plan));

    auto config = ExecutorConfig::loadFromContext(context);
    return std::make_unique<LocalExecutor>(std::move(query_plan), std::move(builder), config.dump_pipeline);
}

SerializedPlanParser::SerializedPlanParser(const ContextPtr & context_) : context(context_)
{
}

void SerializedPlanParser::removeNullableForRequiredColumns(const std::set<String> & require_columns, ActionsDAG & actions_dag) const
{
    for (const auto & item : require_columns)
    {
        if (const auto * require_node = actions_dag.tryFindInOutputs(item))
        {
            auto function_builder = FunctionFactory::instance().get("assumeNotNull", context);
            ActionsDAG::NodeRawConstPtrs args = {require_node};
            const auto & node = actions_dag.addFunction(function_builder, args, item);
            actions_dag.addOrReplaceInOutputs(node);
        }
    }
}

void SerializedPlanParser::wrapNullable(
    const std::vector<String> & columns, ActionsDAG & actions_dag, std::map<std::string, std::string> & nullable_measure_names)
{
    for (const auto & item : columns)
    {
        ActionsDAG::NodeRawConstPtrs args;
        args.emplace_back(&actions_dag.findInOutputs(item));
        const auto * node = toFunctionNode(actions_dag, "toNullable", args);
        actions_dag.addOrReplaceInOutputs(*node);
        nullable_measure_names[item] = node->result_name;
    }
}

NonNullableColumnsResolver::NonNullableColumnsResolver(
    const Block & header_, SerializedPlanParser & parser_, const substrait::Expression & cond_rel_)
    : header(header_), parser(parser_), cond_rel(cond_rel_)
{
}

// make it simple at present, if the condition contains or, return empty for both side.
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
    auto function_signature = parser.function_mapping.at(std::to_string(scalar_function.function_reference()));
    auto function_name = safeGetFunctionName(function_signature, scalar_function);

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
        auto function_signature = parser.function_mapping.at(std::to_string(scalar_function.function_reference()));
        auto function_name = safeGetFunctionName(function_signature, scalar_function);
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

std::string NonNullableColumnsResolver::safeGetFunctionName(
    const std::string & function_signature, const substrait::Expression_ScalarFunction & function) const
{
    try
    {
        return parser.getFunctionName(function_signature, function);
    }
    catch (const Exception &)
    {
        return "";
    }
}
}
