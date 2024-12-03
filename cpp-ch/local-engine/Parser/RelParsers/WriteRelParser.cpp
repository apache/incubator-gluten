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

#include "WriteRelParser.h"

#include <Core/Settings.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Parser/TypeParser.h>
#include <Processors/Transforms/ApplySquashingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/PlanSquashingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MergeTree/SparkMergeTreeSink.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <Storages/Output/NormalFileWriter.h>
#include <google/protobuf/wrappers.pb.h>
#include <substrait/algebra.pb.h>
#include <substrait/type.pb.h>
#include <write_optimization.pb.h>
#include <Common/GlutenSettings.h>

namespace DB::Setting
{
extern const SettingsUInt64 min_insert_block_size_rows;
extern const SettingsUInt64 min_insert_block_size_bytes;
}

using namespace local_engine;
using namespace DB;

namespace
{
DB::ProcessorPtr make_sink(
    const DB::ContextPtr & context,
    const DB::Names & partition_by,
    const DB::Block & input_header,
    const DB::Block & output_header,
    const std::string & base_path,
    const FileNameGenerator & generator,
    const std::string & format_hint,
    const std::shared_ptr<WriteStats> & stats)
{
    if (partition_by.empty())
    {
        return std::make_shared<SubstraitFileSink>(
            context, base_path, "", generator.generate(), format_hint, input_header, stats, DeltaStats{input_header.columns()});
    }

    return std::make_shared<SubstraitPartitionedFileSink>(
        context, partition_by, input_header, output_header, base_path, generator, format_hint, stats);
}

DB::ExpressionActionsPtr create_rename_action(const DB::Block & input, const DB::Block & output)
{
    DB::NamesWithAliases aliases;
    for (auto output_name = output.begin(), input_iter = input.begin(); output_name != output.end(); ++output_name, ++input_iter)
        aliases.emplace_back(DB::NameWithAlias(input_iter->name, output_name->name));

    ActionsDAG actions_dag{blockToNameAndTypeList(input)};
    actions_dag.project(aliases);
    return std::make_shared<DB::ExpressionActions>(std::move(actions_dag));
}

DB::ExpressionActionsPtr create_project_action(const DB::Block & input, const DB::Block & output)
{
    DB::ColumnsWithTypeAndName final_cols;
    std::ranges::transform(
        output,
        std::back_inserter(final_cols),
        [](const DB::ColumnWithTypeAndName & out_ocl)
        {
            const auto out_type = out_ocl.type;
            return DB::ColumnWithTypeAndName(out_type->createColumn(), out_type, out_ocl.name);
        });
    assert(final_cols.size() == output.columns());

    const auto & original_cols = input.getColumnsWithTypeAndName();
    ActionsDAG final_project = ActionsDAG::makeConvertingActions(original_cols, final_cols, ActionsDAG::MatchColumnsMode::Position);
    return std::make_shared<DB::ExpressionActions>(std::move(final_project));
}

void adjust_output(const DB::QueryPipelineBuilderPtr & builder, const DB::Block & output)
{
    const auto input = builder->getHeader();
    if (input.columns() != output.columns())
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Missmatch result columns size, input size is {}, but output size is {}",
            input.columns(),
            output.columns());
    }

    auto mismatch_pair = std::mismatch(
        input.begin(),
        input.end(),
        output.begin(),
        [](const DB::ColumnWithTypeAndName & lhs, const DB::ColumnWithTypeAndName & rhs) { return lhs.name == rhs.name; });
    bool name_is_different = mismatch_pair.first != input.end();

    mismatch_pair = std::mismatch(
        input.begin(),
        input.end(),
        output.begin(),
        [](const DB::ColumnWithTypeAndName & lhs, const DB::ColumnWithTypeAndName & rhs) { return lhs.type->equals(*rhs.type); });
    bool type_is_different = mismatch_pair.first != input.end();

    DB::ExpressionActionsPtr convert_action;

    if (type_is_different)
        convert_action = create_project_action(input, output);

    if (name_is_different && !convert_action)
        convert_action = create_rename_action(input, output);

    if (!convert_action)
        return;

    builder->addSimpleTransform(
        [&](const DB::Block & cur_header, const DB::QueryPipelineBuilder::StreamType stream_type) -> DB::ProcessorPtr
        {
            if (stream_type != DB::QueryPipelineBuilder::StreamType::Main)
                return nullptr;
            return std::make_shared<DB::ConvertingTransform>(cur_header, convert_action);
        });
}

void addMergeTreeSinkTransform(
    const DB::ContextPtr & context,
    const DB::QueryPipelineBuilderPtr & builder,
    const MergeTreeTable & merge_tree_table,
    const DB::Block & header,
    const DB::Names & partition_by)
{
    Chain chain;
    //
    auto stats = std::make_shared<MergeTreeStats>(header);
    chain.addSink(stats);
    //

    SparkMergeTreeWriteSettings write_settings{context};
    if (partition_by.empty())
        write_settings.partition_settings.partition_dir = SubstraitFileSink::NO_PARTITION_ID;

    auto sink = partition_by.empty()
        ? SparkMergeTreeSink::create(merge_tree_table, write_settings, context->getGlobalContext(), {stats})
        : std::make_shared<SparkMergeTreePartitionedFileSink>(header, partition_by, merge_tree_table, write_settings, context, stats);

    chain.addSource(sink);
    const DB::Settings & settings = context->getSettingsRef();
    chain.addSource(std::make_shared<ApplySquashingTransform>(
        header, settings[Setting::min_insert_block_size_rows], settings[Setting::min_insert_block_size_bytes]));
    chain.addSource(std::make_shared<PlanSquashingTransform>(
        header, settings[Setting::min_insert_block_size_rows], settings[Setting::min_insert_block_size_bytes]));

    builder->addChain(std::move(chain));
}

void addNormalFileWriterSinkTransform(
    const DB::ContextPtr & context,
    const DB::QueryPipelineBuilderPtr & builder,
    const std::string & format_hint,
    const DB::Block & output,
    const DB::Names & partitionCols)
{
    GlutenWriteSettings write_settings = GlutenWriteSettings::get(context);

    if (write_settings.task_write_tmp_dir.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Write Pipeline need inject temp directory.");

    if (write_settings.task_write_filename.empty() && write_settings.task_write_filename_pattern.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Write Pipeline need inject file name or file name pattern.");

    FileNameGenerator generator{
        .pattern = write_settings.task_write_filename.empty(),
        .filename_or_pattern
        = write_settings.task_write_filename.empty() ? write_settings.task_write_filename_pattern : write_settings.task_write_filename};

    auto stats = WriteStats::create(output, partitionCols);

    builder->addSimpleTransform(
        [&](const Block & cur_header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;
            return make_sink(context, partitionCols, cur_header, output, write_settings.task_write_tmp_dir, generator, format_hint, stats);
        });
    builder->addSimpleTransform(
        [&](const Block &, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;
            return stats;
        });
}
}

namespace local_engine
{

IMPLEMENT_GLUTEN_SETTINGS(GlutenWriteSettings, WRITE_RELATED_SETTINGS)

void addSinkTransform(const DB::ContextPtr & context, const substrait::WriteRel & write_rel, const DB::QueryPipelineBuilderPtr & builder)
{
    assert(write_rel.has_named_table());
    const substrait::NamedObjectWrite & named_table = write_rel.named_table();

    local_engine::Write write;
    if (!named_table.advanced_extension().optimization().UnpackTo(&write))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to unpack write optimization with local_engine::Write.");
    assert(write.has_common());
    const substrait::NamedStruct & table_schema = write_rel.table_schema();
    auto output = TypeParser::buildBlockFromNamedStruct(table_schema);
    adjust_output(builder, output);
    const auto partitionCols = collect_partition_cols(output, table_schema);
    if (write.has_mergetree())
    {
        local_engine::MergeTreeTable merge_tree_table(write, table_schema);
        GlutenWriteSettings write_settings = GlutenWriteSettings::get(context);
        if (write_settings.task_write_tmp_dir.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "MergeTree Write Pipeline need inject relative path.");
        if (!merge_tree_table.relative_path.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Non empty relative path for MergeTree table in pipeline mode.");

        merge_tree_table.relative_path = write_settings.task_write_tmp_dir;
        addMergeTreeSinkTransform(context, builder, merge_tree_table, output, partitionCols);
    }
    else
        addNormalFileWriterSinkTransform(context, builder, write.common().format(), output, partitionCols);
}

DB::Names collect_partition_cols(const DB::Block & header, const substrait::NamedStruct & struct_)
{
    DB::Names result;
    assert(struct_.column_types_size() == header.columns());
    assert(struct_.column_types_size() == struct_.struct_().types_size());

    auto name_iter = header.begin();
    auto type_iter = struct_.column_types().begin();
    for (; name_iter != header.end(); ++name_iter, ++type_iter)
        if (*type_iter == ::substrait::NamedStruct::PARTITION_COL)
            result.push_back(name_iter->name);
    return result;
}

}
