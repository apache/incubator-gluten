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
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Output/FileWriterWrappers.h>
#include <google/protobuf/wrappers.pb.h>
#include <substrait/algebra.pb.h>
#include <substrait/type.pb.h>
#include <Poco/StringTokenizer.h>
#include <Common/GlutenSettings.h>

using namespace local_engine;
using namespace DB;

DB::ProcessorPtr make_sink(
    const DB::ContextPtr & context,
    const DB::Names & partition_by,
    const DB::Block & input_header,
    const DB::Block & output_header,
    const std::string & base_path,
    const std::string & filename,
    const std::string & format_hint,
    const std::shared_ptr<local_engine::WriteStats> & stats)
{
    if (partition_by.empty())
    {
        auto file_sink = std::make_shared<local_engine::SubstraitFileSink>(context, base_path, "", filename, format_hint, input_header);
        file_sink->setStats(stats);
        return file_sink;
    }

    auto file_sink = std::make_shared<local_engine::SubstraitPartitionedFileSink>(
        context, partition_by, input_header, output_header, base_path, filename, format_hint);
    file_sink->setStats(stats);
    return file_sink;
}

bool need_fix_tuple(const DB::DataTypePtr & input, const DB::DataTypePtr & output)
{
    const auto original = typeid_cast<const DataTypeTuple *>(input.get());
    const auto output_type = typeid_cast<const DataTypeTuple *>(output.get());
    return original != nullptr && output_type != nullptr && !original->equals(*output_type);
}

DB::ExpressionActionsPtr create_rename_action(const DB::Block & input, const DB::Block & output)
{
    DB::NamesWithAliases aliases;
    for (auto ouput_name = output.begin(), input_iter = input.begin(); ouput_name != output.end(); ++ouput_name, ++input_iter)
        aliases.emplace_back(DB::NameWithAlias(input_iter->name, ouput_name->name));

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

namespace local_engine
{

IMPLEMENT_GLUTEN_SETTINGS(GlutenWriteSettings, WRITE_RELATED_SETTINGS)

void addSinkTransform(const DB::ContextPtr & context, const substrait::WriteRel & write_rel, const DB::QueryPipelineBuilderPtr & builder)
{
    GlutenWriteSettings write_settings = GlutenWriteSettings::get(context);

    if (write_settings.task_write_tmp_dir.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Write Pipeline need inject temp directory.");

    if (write_settings.task_write_filename.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Write Pipeline need inject file name.");

    assert(write_rel.has_named_table());
    const substrait::NamedObjectWrite & named_table = write_rel.named_table();
    google::protobuf::StringValue optimization;
    named_table.advanced_extension().optimization().UnpackTo(&optimization);
    auto config = local_engine::parse_write_parameter(optimization.value());

    //TODO : set compression codec according to format
    assert(config["isSnappy"] == "1");
    assert(config.contains("format"));

    assert(write_rel.has_table_schema());
    const substrait::NamedStruct & table_schema = write_rel.table_schema();
    auto blockHeader = TypeParser::buildBlockFromNamedStruct(table_schema);
    const auto partitionCols = collect_partition_cols(blockHeader, table_schema);

    auto stats = std::make_shared<WriteStats>(blockHeader);

    adjust_output(builder, blockHeader);

    builder->addSimpleTransform(
        [&](const Block & cur_header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;
            return make_sink(
                context,
                partitionCols,
                cur_header,
                blockHeader,
                write_settings.task_write_tmp_dir,
                write_settings.task_write_filename,
                config["format"],
                stats);
        });
    builder->addSimpleTransform(
        [&](const Block &, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;
            return stats;
        });
}

std::map<std::string, std::string> parse_write_parameter(const std::string & input)
{
    std::map<std::string, std::string> reuslt;
    const std::string prefix = "WriteParameters:";
    const size_t prefix_pos = input.find(prefix);
    if (prefix_pos == std::string::npos)
        return reuslt;

    const size_t start_pos = prefix_pos + prefix.length();
    const size_t end_pos = input.find('\n', start_pos);

    if (end_pos == std::string::npos)
        return reuslt;

    for (const Poco::StringTokenizer tok(input.substr(start_pos, end_pos - start_pos), ";", Poco::StringTokenizer::TOK_TRIM);
         const auto & parameter : tok)
    {
        const size_t pos = parameter.find('=');
        if (pos == std::string::npos)
            continue;
        reuslt[parameter.substr(0, pos)] = parameter.substr(pos + 1);
    }
    return reuslt;
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