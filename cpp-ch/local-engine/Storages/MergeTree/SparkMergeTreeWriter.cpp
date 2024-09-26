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
#include "SparkMergeTreeWriter.h"

#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Transforms/ApplySquashingTransform.h>
#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MetaDataHelper.h>
#include <Storages/MergeTree/SparkMergeTreeSink.h>
#include <rapidjson/prettywriter.h>
#include <Poco/StringTokenizer.h>

namespace DB::Setting
{
extern const SettingsUInt64 min_insert_block_size_rows;
extern const SettingsUInt64 min_insert_block_size_bytes;
}
using namespace DB;
namespace
{
Block removeColumnSuffix(const Block & block)
{
    ColumnsWithTypeAndName columns;
    for (int i = 0; i < block.columns(); ++i)
    {
        auto name = block.getByPosition(i).name;
        Poco::StringTokenizer splits(name, "#");
        auto column = block.getByPosition(i);
        column.name = splits[0];
        columns.emplace_back(column);
    }
    return Block(columns);
}
}

namespace local_engine
{

std::unique_ptr<SparkMergeTreeWriter> SparkMergeTreeWriter::create(
    const MergeTreeTable & merge_tree_table,
    const SparkMergeTreeWritePartitionSettings & write_settings_,
    const DB::ContextMutablePtr & context)
{
    const DB::Settings & settings = context->getSettingsRef();
    const auto dest_storage = merge_tree_table.getStorage(context);
    StorageMetadataPtr metadata_snapshot = dest_storage->getInMemoryMetadataPtr();
    Block header = metadata_snapshot->getSampleBlock();
    ASTPtr none;
    Chain chain;
    auto sink = dest_storage->write(none, metadata_snapshot, context, false);
    chain.addSink(sink);
    chain.addSource(
        std::make_shared<ApplySquashingTransform>(header, settings[Setting::min_insert_block_size_rows], settings[Setting::min_insert_block_size_bytes]));
    chain.addSource(
        std::make_shared<PlanSquashingTransform>(header, settings[Setting::min_insert_block_size_rows], settings[Setting::min_insert_block_size_bytes]));

    std::unordered_map<String, String> partition_values;
    if (!write_settings_.partition_dir.empty())
        extractPartitionValues(write_settings_.partition_dir, partition_values);
    return std::make_unique<SparkMergeTreeWriter>(
        header, assert_cast<const SparkMergeTreeSink &>(*sink).sinkHelper(), QueryPipeline{std::move(chain)}, std::move(partition_values));
}

SparkMergeTreeWriter::SparkMergeTreeWriter(
    const DB::Block & header_,
    const SinkHelper & sink_helper_,
    DB::QueryPipeline && pipeline_,
    std::unordered_map<String, String> && partition_values_)
    : header{header_}, sink_helper{sink_helper_}, pipeline{std::move(pipeline_)}, executor{pipeline}, partition_values{partition_values_}
{
}

void SparkMergeTreeWriter::write(const DB::Block & block)
{
    auto new_block = removeColumnSuffix(block);
    auto converter = ActionsDAG::makeConvertingActions(
        new_block.getColumnsWithTypeAndName(), header.getColumnsWithTypeAndName(), DB::ActionsDAG::MatchColumnsMode::Position);
    const ExpressionActions expression_actions{std::move(converter)};
    expression_actions.execute(new_block);
    executor.push(new_block);
}

void SparkMergeTreeWriter::finalize()
{
    executor.finish();
}

std::vector<PartInfo> SparkMergeTreeWriter::getAllPartInfo() const
{
    std::vector<PartInfo> res;
    auto parts = sink_helper.unsafeGet();
    res.reserve(parts.size());

    for (const auto & part : parts)
    {
        res.emplace_back(PartInfo{
            part->name,
            part->getMarksCount(),
            part->getBytesOnDisk(),
            part->rows_count,
            partition_values,
            sink_helper.write_settings.partition_settings.bucket_dir});
    }
    return res;
}

String SparkMergeTreeWriter::partInfosToJson(const std::vector<PartInfo> & part_infos)
{
    rapidjson::StringBuffer result;
    rapidjson::Writer<rapidjson::StringBuffer> writer(result);
    writer.StartArray();
    for (const auto & item : part_infos)
    {
        writer.StartObject();
        writer.Key("part_name");
        writer.String(item.part_name.c_str());
        writer.Key("mark_count");
        writer.Uint(item.mark_count);
        writer.Key("disk_size");
        writer.Uint(item.disk_size);
        writer.Key("row_count");
        writer.Uint(item.row_count);
        writer.Key("bucket_id");
        writer.String(item.bucket_id.c_str());
        writer.Key("partition_values");
        writer.StartObject();
        for (const auto & key_value : item.partition_values)
        {
            writer.Key(key_value.first.c_str());
            writer.String(key_value.second.c_str());
        }
        writer.EndObject();
        writer.EndObject();
    }
    writer.EndArray();
    return result.GetString();
}

}