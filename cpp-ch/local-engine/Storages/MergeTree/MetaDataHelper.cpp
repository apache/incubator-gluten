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
#include "MetaDataHelper.h"
#include <filesystem>
#include <Core/Settings.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Storages/MergeTree/MergeSparkMergeTreeTask.h>
#include <Poco/StringTokenizer.h>
#include <Common/QueryContext.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
}

namespace DB
{
namespace Setting
{
extern const SettingsSeconds lock_acquire_timeout;
extern const SettingsMaxThreads max_threads;
}
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

using namespace DB;

namespace local_engine
{
static const String METADATA_FILE_NAME = "metadata.gluten";

std::unordered_map<String, String> extractPartMetaData(ReadBuffer & in)
{
    std::unordered_map<String, String> result;
    while (!in.eof())
    {
        String name;
        readString(name, in);
        assertChar('\t', in);
        UInt64 size;
        readIntText(size, in);
        assertChar('\n', in);
        String data;
        data.resize(size);
        in.read(data.data(), size);
        result.emplace(name, data);
    }
    return result;
}

enum SupportedMetaDataStorageType
{
    UNKNOWN =0,
    ROCKSDB,
    LOCAL
};

template <SupportedMetaDataStorageType type>
static void restoreMetaData(const SparkStorageMergeTreePtr & storage, const MergeTreeTableInstance & mergeTreeTable, const Context & context)
{
    UNREACHABLE();
}

template <>
void restoreMetaData<ROCKSDB>(const SparkStorageMergeTreePtr & storage, const MergeTreeTableInstance & mergeTreeTable, const Context & context)
{
    auto data_disk = storage->getStoragePolicy()->getAnyDisk();
    std::unordered_set<String> not_exists_part;
    auto metadata_storage = data_disk->getMetadataStorage();
    auto table_path = std::filesystem::path(mergeTreeTable.relative_path);
    for (const auto & part : mergeTreeTable.getPartNames())
    {
        auto part_path = table_path / part;
        if (!metadata_storage->exists(part_path))
            not_exists_part.emplace(part);
    }

    if (auto lock = storage->lockForAlter(context.getSettingsRef()[Setting::lock_acquire_timeout]))
    {
        // put this return clause in lockForAlter
        // so that it will not return until other thread finishes restoring
        if (not_exists_part.empty())
            return;

        auto s3 = data_disk->getObjectStorage();
        auto transaction = metadata_storage->createTransaction();

        if (!metadata_storage->exists(table_path))
            transaction->createDirectoryRecursive(table_path.generic_string());

        for (const auto & part : not_exists_part)
        {
                auto part_path = table_path / part;
                auto metadata_file_path = part_path / METADATA_FILE_NAME;

                if (metadata_storage->exists(part_path))
                    return;
                else
                    transaction->createDirectoryRecursive(part_path);
                auto key = s3->generateObjectKeyForPath(metadata_file_path.generic_string(), std::nullopt);
                StoredObject metadata_object(key.serialize());
                auto read_settings = ReadSettings{};
                read_settings.enable_filesystem_cache = false;
                auto part_metadata = extractPartMetaData(*s3->readObject(metadata_object, read_settings));
                for (const auto & item : part_metadata)
                {
                    auto item_path = part_path / item.first;
                    transaction->writeStringToFile(item_path, item.second);
                }
        }
        transaction->commit();
    }
}

template <>
void restoreMetaData<LOCAL>(
    const SparkStorageMergeTreePtr & storage, const MergeTreeTableInstance & mergeTreeTable, const Context & context)
{
    const auto data_disk = storage->getStoragePolicy()->getAnyDisk();
    std::unordered_set<String> not_exists_part;
    const DB::MetadataStorageFromDisk * metadata_storage = static_cast<MetadataStorageFromDisk *>(data_disk->getMetadataStorage().get());
    const auto metadata_disk = metadata_storage->getDisk();
    const auto table_path = std::filesystem::path(mergeTreeTable.relative_path);
    for (const auto & part : mergeTreeTable.getPartNames())
    {
        auto part_path = table_path / part;
        if (!metadata_disk->exists(part_path))
            not_exists_part.emplace(part);
    }

    if (auto lock = storage->lockForAlter(context.getSettingsRef()[Setting::lock_acquire_timeout]))
    {
        // put this return clause in lockForAlter
        // so that it will not return until other thread finishes restoring
        if (not_exists_part.empty())
            return;

        // Increase the speed of metadata recovery
        auto max_concurrency = std::max(10UL, QueryContext::globalContext()->getSettingsRef()[Setting::max_threads].value);
        auto max_threads = std::min(max_concurrency, not_exists_part.size());
        FreeThreadPool thread_pool(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            CurrentMetrics::LocalThreadScheduled,
            max_threads,
            max_threads,
            not_exists_part.size());
        auto s3 = data_disk->getObjectStorage();

        if (!metadata_disk->exists(table_path))
            metadata_disk->createDirectories(table_path.generic_string());

        for (const auto & part : not_exists_part)
        {
            auto job = [&]()
            {
                auto part_path = table_path / part;
                auto metadata_file_path = part_path / METADATA_FILE_NAME;

                if (metadata_disk->exists(part_path))
                    return;
                else
                    metadata_disk->createDirectories(part_path);
                auto key = s3->generateObjectKeyForPath(metadata_file_path.generic_string(), std::nullopt);
                StoredObject metadata_object(key.serialize());
                auto read_settings = ReadSettings{};
                read_settings.enable_filesystem_cache = false;
                auto part_metadata = extractPartMetaData(*s3->readObject(metadata_object, read_settings));
                for (const auto & item : part_metadata)
                {
                    auto item_path = part_path / item.first;
                    auto out = metadata_disk->writeFile(item_path);
                    out->write(item.second.data(), item.second.size());
                }
            };
            thread_pool.scheduleOrThrow(job);
        }
        thread_pool.wait();
    }
}


bool isMergeTreePartMetaDataFile(const String & file_name)
{
    return file_name.ends_with(METADATA_FILE_NAME);
}

void restoreMetaData(const SparkStorageMergeTreePtr & storage, const MergeTreeTableInstance & mergeTreeTable, const Context & context)
{
    const auto data_disk = storage->getStoragePolicy()->getAnyDisk();
    if (!data_disk->isRemote())
        return;
    auto metadata_storage = data_disk->getMetadataStorage();
    if (metadata_storage->getType() == MetadataStorageType::Local)
    {
        restoreMetaData<LOCAL>(storage, mergeTreeTable, context);
    }
    // None is RocksDB
    else if (metadata_storage->getType() == MetadataStorageType::None)
    {
        restoreMetaData<ROCKSDB>(storage, mergeTreeTable, context);
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported metadata storage type {}.", metadata_storage->getType());
    }
}

void saveFileStatus(
    const DB::MergeTreeData & storage,
    const DB::ContextPtr& context,
    const String & part_name,
    IDataPartStorage & data_part_storage)
{
    const DiskPtr disk = storage.getStoragePolicy()->getAnyDisk();
    if (!disk->isRemote())
        return;
    auto meta_storage = disk->getMetadataStorage();
    const auto out = data_part_storage.writeFile(METADATA_FILE_NAME, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
    for (const auto it = data_part_storage.iterate(); it->isValid(); it->next())
    {
        auto content = meta_storage->readFileToString(it->path());
        writeString(it->name(), *out);
        writeChar('\t', *out);
        writeIntText(content.length(), *out);
        writeChar('\n', *out);
        writeString(content, *out);
    }
    out->finalize();

    LOG_DEBUG(&Poco::Logger::get("MetaDataHelper"), "Save part {} metadata success.", part_name);
}


std::vector<MergeTreeDataPartPtr> mergeParts(
    std::vector<DB::DataPartPtr> selected_parts,
    std::unordered_map<String, String> & partition_values,
    const String & new_part_uuid,
    SparkStorageMergeTree & storage,
    const String  & partition_dir,
    const String & bucket_dir)
{
    auto future_part = std::make_shared<DB::FutureMergedMutatedPart>();
    future_part->uuid = UUIDHelpers::generateV4();

    future_part->assign(std::move(selected_parts));
    future_part->part_info = MergeListElement::FAKE_RESULT_PART_FOR_PROJECTION;

    future_part->name = "";
    if(!partition_dir.empty())
    {
        future_part->name =  partition_dir + "/";
        extractPartitionValues(partition_dir, partition_values);
    }
    if(!bucket_dir.empty())
    {
        future_part->name = future_part->name + bucket_dir + "/";
    }
    future_part->name = future_part->name +  new_part_uuid + "-merged";

    auto entry = std::make_shared<DB::MergeMutateSelectedEntry>(future_part, DB::CurrentlyMergingPartsTaggerPtr{}, std::make_shared<DB::MutationCommands>());

    // Copying a vector of columns `deduplicate by columns.
    DB::IExecutableTask::TaskResultCallback f = [](bool) {};
    auto task = std::make_shared<local_engine::MergeSparkMergeTreeTask>(
        storage, storage.getInMemoryMetadataPtr(), false,  std::vector<std::string>{}, false, entry,
        DB::TableLockHolder{}, f);

    task->setCurrentTransaction(DB::MergeTreeTransactionHolder{}, DB::MergeTreeTransactionPtr{});

    executeHere(task);

    std::unordered_set<std::string> to_load{future_part->name};
    std::vector<MergeTreeDataPartPtr> merged = storage.loadDataPartsWithNames(to_load);
    return merged;
}

void extractPartitionValues(const String & partition_dir, std::unordered_map<String, String> & partition_values)
{
    Poco::StringTokenizer partitions(partition_dir, "/");
    for (const auto & partition : partitions)
    {
        Poco::StringTokenizer key_value(partition, "=");
        chassert(key_value.count() == 2);
        partition_values.emplace(key_value[0], key_value[1]);
    }
}
}