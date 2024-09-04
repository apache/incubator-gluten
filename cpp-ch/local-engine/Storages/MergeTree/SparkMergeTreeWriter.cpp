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

#include <Interpreters/ActionsDAG.h>
#include <Parser/MergeTreeRelParser.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MetaDataHelper.h>
#include <Storages/MergeTree/SparkMergeTreeSink.h>
#include <Storages/MergeTree/StorageMergeTreeFactory.h>
#include <rapidjson/prettywriter.h>
#include <Poco/StringTokenizer.h>
#include <Common/CHUtil.h>
#include <Common/QueryContext.h>

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
SparkMergeTreeWriter::SparkMergeTreeWriter(
    const MergeTreeTable & merge_tree_table, const GlutenMergeTreeWriteSettings & write_settings_, const DB::ContextPtr & context_)
    : write_settings(write_settings_)
    , dataWrapper(SinkHelper::create(merge_tree_table, write_settings, SerializedPlanParser::global_context))
    , context(context_)
{
    const DB::Settings & settings = context->getSettingsRef();
    squashing
        = std::make_unique<DB::Squashing>(dataWrapper->header, settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);
    if (!write_settings.partition_settings.partition_dir.empty())
        extractPartitionValues(write_settings.partition_settings.partition_dir, partition_values);
}

void SparkMergeTreeWriter::write(const DB::Block & block)
{
    auto new_block = removeColumnSuffix(block);
    auto converter = ActionsDAG::makeConvertingActions(
        new_block.getColumnsWithTypeAndName(), dataWrapper->header.getColumnsWithTypeAndName(), DB::ActionsDAG::MatchColumnsMode::Position);
    const ExpressionActions expression_actions{std::move(converter)};
    expression_actions.execute(new_block);

    bool has_part = chunkToPart(squashing->add({new_block.getColumns(), new_block.rows()}));

    if (has_part && write_settings.merge_after_insert)
        dataWrapper->checkAndMerge();
}

bool SparkMergeTreeWriter::chunkToPart(Chunk && plan_chunk)
{
    if (Chunk result_chunk = DB::Squashing::squash(std::move(plan_chunk)))
    {
        auto result = squashing->getHeader().cloneWithColumns(result_chunk.detachColumns());
        return blockToPart(result);
    }
    return false;
}

bool SparkMergeTreeWriter::blockToPart(Block & block)
{
    auto blocks_with_partition = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), 10, dataWrapper->metadata_snapshot, context);

    if (blocks_with_partition.empty())
        return false;

    for (auto & item : blocks_with_partition)
    {
        size_t before_write_memory = 0;
        if (auto * memory_tracker = CurrentThread::getMemoryTracker())
        {
            CurrentThread::flushUntrackedMemory();
            before_write_memory = memory_tracker->get();
        }
        dataWrapper->writeTempPart(item, context, part_num);
        part_num++;
        /// Reset earlier to free memory
        item.block.clear();
        item.partition.clear();
    }

    return true;
}

void SparkMergeTreeWriter::finalize()
{
    chunkToPart(squashing->flush());
    if (write_settings.merge_after_insert)
        dataWrapper->finalizeMerge();

    dataWrapper->commit(context->getReadSettings(), context->getWriteSettings());
    dataWrapper->saveMetadata(context);
}

std::vector<PartInfo> SparkMergeTreeWriter::getAllPartInfo() const
{
    std::vector<PartInfo> res;
    auto parts = dataWrapper->unsafeGet();
    res.reserve(parts.size());

    for (const auto & part : parts)
    {
        res.emplace_back(PartInfo{
            part->name,
            part->getMarksCount(),
            part->getBytesOnDisk(),
            part->rows_count,
            partition_values,
            write_settings.partition_settings.bucket_dir});
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