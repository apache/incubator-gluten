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

#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
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

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
extern const Metric GlobalThread;
extern const Metric GlobalThreadActive;
extern const Metric GlobalThreadScheduled;
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

SparkMergeTreeWriter::SparkMergeTreeWriter(
    const MergeTreeTable & merge_tree_table,
    const GlutenMergeTreeWriteSettings & write_settings_,
    const DB::ContextPtr & context_)
    : write_settings(write_settings_)
    , context(context_)
    , thread_pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1, 1, 100000)
{
    const DB::Settings & settings = context->getSettingsRef();
    merge_after_insert = settings.get(MERGETREE_MERGE_AFTER_INSERT).safeGet<bool>();
    insert_without_local_storage = settings.get(MERGETREE_INSERT_WITHOUT_LOCAL_STORAGE).safeGet<bool>();

    Field limit_size_field;
    if (settings.tryGet("optimize.minFileSize", limit_size_field))
        merge_min_size = limit_size_field.safeGet<Int64>() <= 0 ? merge_min_size : limit_size_field.safeGet<Int64>();

    Field limit_cnt_field;
    if (settings.tryGet("mergetree.max_num_part_per_merge_task", limit_cnt_field))
        merge_limit_parts = limit_cnt_field.safeGet<Int64>() <= 0 ? merge_limit_parts : limit_cnt_field.safeGet<Int64>();

    dest_storage = MergeTreeRelParser::parseStorage(merge_tree_table, SerializedPlanParser::global_context);
    isRemoteStorage = dest_storage->getStoragePolicy()->getAnyDisk()->isRemote();

    if (useLocalStorage())
    {
        temp_storage = MergeTreeRelParser::copyToDefaultPolicyStorage(merge_tree_table, SerializedPlanParser::global_context);
        data = temp_storage;
        LOG_DEBUG(
            &Poco::Logger::get("SparkMergeTreeWriter"),
            "Create temp table {} for local merge.",
            temp_storage->getStorageID().getFullNameNotQuoted());
    }
    else
        data = dest_storage;

    metadata_snapshot = data->getInMemoryMetadataPtr();
    header = metadata_snapshot->getSampleBlock();
    squashing = std::make_unique<DB::Squashing>(header, settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);
    if (!write_settings.partition_dir.empty())
        extractPartitionValues(write_settings.partition_dir, partition_values);
}

bool SparkMergeTreeWriter::useLocalStorage() const
{
    return !insert_without_local_storage && isRemoteStorage;
}

void SparkMergeTreeWriter::write(const DB::Block & block)
{
    auto new_block = removeColumnSuffix(block);
    auto converter = ActionsDAG::makeConvertingActions(
        new_block.getColumnsWithTypeAndName(), header.getColumnsWithTypeAndName(), DB::ActionsDAG::MatchColumnsMode::Position);
    const ExpressionActions expression_actions{std::move(converter)};
    expression_actions.execute(new_block);

    bool has_part = chunkToPart(squashing->add({new_block.getColumns(), new_block.rows()}));

    if (has_part && merge_after_insert)
        checkAndMerge();
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
    auto blocks_with_partition = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), 10, metadata_snapshot, context);

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

        new_parts.emplace_back(writeTempPartAndFinalize(item, metadata_snapshot).part);
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
    if (merge_after_insert)
        finalizeMerge();

    commitPartToRemoteStorageIfNeeded();
    saveMetadata();
}

void SparkMergeTreeWriter::saveMetadata()
{
    if (!isRemoteStorage)
        return;

    for (const auto & merge_tree_data_part : new_parts.unsafeGet())
    {
        auto part = dest_storage->loadDataPartsWithNames({merge_tree_data_part->name});
        if (part.empty())
        {
            LOG_WARNING(
                &Poco::Logger::get("SparkMergeTreeWriter"),
                "Save metadata failed because dest storage load part name {} empty.",
                merge_tree_data_part->name);
            continue;
        }

        saveFileStatus(
            *dest_storage, context, merge_tree_data_part->name, const_cast<IDataPartStorage &>(part.at(0)->getDataPartStorage()));
    }
}

void SparkMergeTreeWriter::commitPartToRemoteStorageIfNeeded()
{
    if (!useLocalStorage())
        return;

    LOG_DEBUG(
        &Poco::Logger::get("SparkMergeTreeWriter"), "Begin upload to disk {}.", dest_storage->getStoragePolicy()->getAnyDisk()->getName());

    auto read_settings = context->getReadSettings();
    auto write_settings = context->getWriteSettings();
    Stopwatch watch;
    for (const auto & merge_tree_data_part : new_parts.unsafeGet())
    {
        String local_relative_path = data->getRelativeDataPath() + "/" + merge_tree_data_part->name;
        String remote_relative_path = dest_storage->getRelativeDataPath() + "/" + merge_tree_data_part->name;

        std::vector<String> files;
        data->getStoragePolicy()->getAnyDisk()->listFiles(local_relative_path, files);
        auto src_disk = data->getStoragePolicy()->getAnyDisk();
        auto dest_disk = dest_storage->getStoragePolicy()->getAnyDisk();
        auto tx = dest_disk->createTransaction();
        for (const auto & file : files)
        {
            auto read_buffer = src_disk->readFile(local_relative_path + "/" + file, read_settings);
            auto write_buffer
                = tx->writeFile(remote_relative_path + "/" + file, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, write_settings);
            copyData(*read_buffer, *write_buffer);
            write_buffer->finalize();
        }
        tx->commit();
        LOG_DEBUG(
            &Poco::Logger::get("SparkMergeTreeWriter"),
            "Upload part {} to disk {} success.",
            merge_tree_data_part->name,
            dest_storage->getStoragePolicy()->getAnyDisk()->getName());
    }
    watch.stop();
    LOG_INFO(
        &Poco::Logger::get("SparkMergeTreeWriter"),
        "Upload to disk {} finished, total elapsed {} ms",
        dest_storage->getStoragePolicy()->getAnyDisk()->getName(),
        watch.elapsedMilliseconds());
    StorageMergeTreeFactory::freeStorage(temp_storage->getStorageID());
    temp_storage->dropAllData();
    LOG_DEBUG(
        &Poco::Logger::get("SparkMergeTreeWriter"), "Clean temp table {} success.", temp_storage->getStorageID().getFullNameNotQuoted());
}

void SparkMergeTreeWriter::finalizeMerge()
{
    LOG_DEBUG(&Poco::Logger::get("SparkMergeTreeWriter"), "Waiting all merge task end and do final merge");
    // waiting all merge task end and do final merge
    thread_pool.wait();

    size_t before_merge_size;
    do
    {
        before_merge_size = new_parts.size();
        checkAndMerge(true);
        thread_pool.wait();
    } while (before_merge_size != new_parts.size());

    std::unordered_set<String> final_parts;
    for (const auto & merge_tree_data_part : new_parts.unsafeGet())
        final_parts.emplace(merge_tree_data_part->name);

    // default storage need clean temp.
    if (!temp_storage)
    {
        for (const auto & tmp_part : tmp_parts)
        {
            if (final_parts.contains(tmp_part))
                continue;

            GlobalThreadPool::instance().scheduleOrThrow(
                [storage_ = data, tmp = tmp_part]() -> void
                {
                    for (const auto & disk : storage_->getDisks())
                    {
                        auto rel_path = storage_->getRelativeDataPath() + "/" + tmp;
                        disk->removeRecursive(rel_path);
                    }
                });
        }
    }
}

DB::MergeTreeDataWriter::TemporaryPart SparkMergeTreeWriter::writeTempPartAndFinalize(
    DB::BlockWithPartition & block_with_partition, const DB::StorageMetadataPtr & metadata_snapshot) const
{
    const SparkMergeTreeDataWriter writer(*data);
    return writer.writeTempPart(
        block_with_partition,
        metadata_snapshot,
        context,
        write_settings,
        part_num);
}

std::vector<PartInfo> SparkMergeTreeWriter::getAllPartInfo()
{
    std::vector<PartInfo> res;
    res.reserve(new_parts.size());

    for (const auto & part : new_parts.unsafeGet())
    {
        res.emplace_back(
            PartInfo{part->name, part->getMarksCount(), part->getBytesOnDisk(), part->rows_count, partition_values, write_settings.bucket_dir});
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

void SparkMergeTreeWriter::checkAndMerge(bool force)
{
    // Only finalize should force merge.
    if (!force && new_parts.size() < merge_limit_parts)
        return;

    auto doMergeTask = [this](const std::vector<MergeTreeDataPartPtr> & prepare_merge_parts)
    {
        for (const auto & selected_part : prepare_merge_parts)
            tmp_parts.emplace(selected_part->name);
        // check thread group initialized in task thread
        currentThreadGroupMemoryUsage();
        thread_pool.scheduleOrThrow(
            [this, prepare_merge_parts, thread_group = CurrentThread::getGroup()]() -> void
            {
                Stopwatch watch;
                CurrentThread::detachFromGroupIfNotDetached();
                CurrentThread::attachToGroup(thread_group);
                size_t before_size = 0;
                size_t after_size = 0;
                for (const auto & prepare_merge_part : prepare_merge_parts)
                    before_size += prepare_merge_part->getBytesOnDisk();

                std::unordered_map<String, String> partition_values;
                const auto merged_parts = mergeParts(
                    prepare_merge_parts, partition_values, toString(UUIDHelpers::generateV4()), data, write_settings.partition_dir, write_settings.bucket_dir);
                for (const auto & merge_tree_data_part : merged_parts)
                    after_size += merge_tree_data_part->getBytesOnDisk();

                new_parts.emplace_back(merged_parts);
                watch.stop();
                LOG_INFO(
                    &Poco::Logger::get("SparkMergeTreeWriter"),
                    "Merge success. Before merge part size {}, part count {}, after part size {}, part count {}, "
                    "total elapsed {} ms",
                    before_size,
                    prepare_merge_parts.size(),
                    after_size,
                    merged_parts.size(),
                    watch.elapsedMilliseconds());
            });
    };

    std::vector<MergeTreeDataPartPtr> selected_parts;
    selected_parts.reserve(merge_limit_parts);
    size_t totol_size = 0;
    std::vector<MergeTreeDataPartPtr> skip_parts;

    while (const auto merge_tree_data_part_option = new_parts.pop_front())
    {
        auto merge_tree_data_part = merge_tree_data_part_option.value();
        if (merge_tree_data_part->getBytesOnDisk() >= merge_min_size)
        {
            skip_parts.emplace_back(merge_tree_data_part);
            continue;
        }

        selected_parts.emplace_back(merge_tree_data_part);
        totol_size += merge_tree_data_part->getBytesOnDisk();
        if (merge_min_size > totol_size && merge_limit_parts > selected_parts.size())
            continue;

        doMergeTask(selected_parts);
        selected_parts.clear();
        totol_size = 0;
    }

    if (!selected_parts.empty())
    {
        if (force && selected_parts.size() > 1)
            doMergeTask(selected_parts);
        else
            new_parts.emplace_back(selected_parts);
    }

    new_parts.emplace_back(skip_parts);
}
}