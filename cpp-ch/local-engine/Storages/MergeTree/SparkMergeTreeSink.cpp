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
#include "SparkMergeTreeSink.h"

#include <Core/Settings.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Storages/MergeTree/MetaDataHelper.h>
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

namespace local_engine
{

void SparkMergeTreeSink::consume(Chunk & chunk)
{
    assert(!sink_helper->metadata_snapshot->hasPartitionKey());

    BlockWithPartition item{getHeader().cloneWithColumns(chunk.getColumns()), Row{}};
    size_t before_write_memory = 0;
    if (auto * memory_tracker = CurrentThread::getMemoryTracker())
    {
        CurrentThread::flushUntrackedMemory();
        before_write_memory = memory_tracker->get();
    }
    sink_helper->writeTempPart(item, context, part_num);
    part_num++;
    /// Reset earlier to free memory
    item.block.clear();
    item.partition.clear();

    sink_helper->checkAndMerge();
}

void SparkMergeTreeSink::onStart()
{
    // DO NOTHING
}

void SparkMergeTreeSink::onFinish()
{
    sink_helper->finish(context);
}

/////
SinkHelperPtr SparkMergeTreeSink::create(
    const MergeTreeTable & merge_tree_table, const SparkMergeTreeWriteSettings & write_settings_, const DB::ContextMutablePtr & context)
{
    auto dest_storage = merge_tree_table.getStorage(context);
    bool isRemoteStorage = dest_storage->getStoragePolicy()->getAnyDisk()->isRemote();
    bool insert_with_local_storage = !write_settings_.insert_without_local_storage;
    if (insert_with_local_storage && isRemoteStorage)
    {
        auto temp = merge_tree_table.copyToDefaultPolicyStorage(context);
        LOG_DEBUG(
            &Poco::Logger::get("SparkMergeTreeWriter"),
            "Create temp table {} for local merge.",
            temp->getStorageID().getFullNameNotQuoted());
        return std::make_shared<CopyToRemoteSinkHelper>(temp, dest_storage, write_settings_);
    }

    return std::make_shared<DirectSinkHelper>(dest_storage, write_settings_, isRemoteStorage);
}

SinkHelper::SinkHelper(const SparkStorageMergeTreePtr & data_, const SparkMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_)
    : data(data_)
    , isRemoteStorage(isRemoteStorage_)
    , thread_pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, 1, 1, 100000)
    , write_settings(write_settings_)
    , metadata_snapshot(data->getInMemoryMetadataPtr())
{
}

void SinkHelper::saveMetadata(const DB::ContextPtr & context)
{
    if (!isRemoteStorage)
        return;

    const std::deque<DB::MergeTreeDataPartPtr> & parts = new_parts.unsafeGet();
    for (const auto & merge_tree_data_part : parts)
    {
        auto part = dest_storage().loadDataPartsWithNames({merge_tree_data_part->name});
        if (part.empty())
        {
            LOG_WARNING(
                &Poco::Logger::get("SparkMergeTreeWriter"),
                "Save metadata failed because dest storage load part name {} empty.",
                merge_tree_data_part->name);
            continue;
        }

        saveFileStatus(
            dest_storage(), context, merge_tree_data_part->name, const_cast<IDataPartStorage &>(part.at(0)->getDataPartStorage()));
    }
}

void SinkHelper::doMergePartsAsync(const std::vector<DB::MergeTreeDataPartPtr> & prepare_merge_parts)
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
                prepare_merge_parts,
                partition_values,
                toString(UUIDHelpers::generateV4()),
                dataRef(),
                write_settings.partition_settings.partition_dir,
                write_settings.partition_settings.bucket_dir);
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
}
void SinkHelper::writeTempPart(DB::BlockWithPartition & block_with_partition, const ContextPtr & context, int part_num)
{
    const std::string & part_name_prefix = write_settings.partition_settings.part_name_prefix;
    std::string part_dir = fmt::format("{}_{:03d}", part_name_prefix, part_num);
    auto tmp = dataRef().getWriter().writeTempPart(block_with_partition, metadata_snapshot, context, part_dir);
    new_parts.emplace_back(tmp.part);
}

void SinkHelper::checkAndMerge(bool force)
{
    if (!write_settings.merge_after_insert)
        return;
    // Only finalize should force merge.
    if (!force && new_parts.size() < write_settings.merge_limit_parts)
        return;

    std::vector<MergeTreeDataPartPtr> selected_parts;
    selected_parts.reserve(write_settings.merge_limit_parts);
    size_t total_size = 0;
    std::vector<MergeTreeDataPartPtr> skip_parts;

    while (const auto merge_tree_data_part_option = new_parts.pop_front())
    {
        auto merge_tree_data_part = merge_tree_data_part_option.value();
        if (merge_tree_data_part->getBytesOnDisk() >= write_settings.merge_min_size)
        {
            skip_parts.emplace_back(merge_tree_data_part);
            continue;
        }

        selected_parts.emplace_back(merge_tree_data_part);
        total_size += merge_tree_data_part->getBytesOnDisk();
        if (write_settings.merge_min_size > total_size && write_settings.merge_limit_parts > selected_parts.size())
            continue;

        doMergePartsAsync(selected_parts);
        selected_parts.clear();
        total_size = 0;
    }

    if (!selected_parts.empty())
    {
        if (force && selected_parts.size() > 1)
            doMergePartsAsync(selected_parts);
        else
            new_parts.emplace_back(selected_parts);
    }

    new_parts.emplace_back(skip_parts);
}
void SinkHelper::finish(const DB::ContextPtr & context)
{
    if (write_settings.merge_after_insert)
        finalizeMerge();
    commit(context->getReadSettings(), context->getWriteSettings());
    saveMetadata(context);
}

void SinkHelper::finalizeMerge()
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
    cleanup();
}

void CopyToRemoteSinkHelper::commit(const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    LOG_DEBUG(
        &Poco::Logger::get("SparkMergeTreeWriter"), "Begin upload to disk {}.", dest_storage().getStoragePolicy()->getAnyDisk()->getName());

    const std::deque<DB::MergeTreeDataPartPtr> & parts = new_parts.unsafeGet();

    Stopwatch watch;
    for (const auto & merge_tree_data_part : parts)
    {
        String local_relative_path = dataRef().getRelativeDataPath() + "/" + merge_tree_data_part->name;
        String remote_relative_path = dest_storage().getRelativeDataPath() + "/" + merge_tree_data_part->name;

        std::vector<String> files;
        dataRef().getStoragePolicy()->getAnyDisk()->listFiles(local_relative_path, files);
        auto src_disk = dataRef().getStoragePolicy()->getAnyDisk();
        auto dest_disk = dest_storage().getStoragePolicy()->getAnyDisk();
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
            dest_storage().getStoragePolicy()->getAnyDisk()->getName());
    }
    watch.stop();
    LOG_INFO(
        &Poco::Logger::get("SparkMergeTreeWriter"),
        "Upload to disk {} finished, total elapsed {} ms",
        dest_storage().getStoragePolicy()->getAnyDisk()->getName(),
        watch.elapsedMilliseconds());
    StorageMergeTreeFactory::freeStorage(temp_storage()->getStorageID());
    temp_storage()->dropAllData();
    LOG_DEBUG(
        &Poco::Logger::get("SparkMergeTreeWriter"), "Clean temp table {} success.", temp_storage()->getStorageID().getFullNameNotQuoted());
}

void DirectSinkHelper::cleanup()
{
    // default storage need clean temp.
    std::unordered_set<String> final_parts;
    for (const auto & merge_tree_data_part : new_parts.unsafeGet())
        final_parts.emplace(merge_tree_data_part->name);

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