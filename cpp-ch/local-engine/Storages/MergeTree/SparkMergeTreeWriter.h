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
#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/Squashing.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeTool.h>
#include <Storages/MergeTree/SparkMergeTreeSink.h>
#include <Storages/MergeTree/StorageMergeTreeFactory.h>
#include <Common/CHUtil.h>

namespace DB
{
struct BlockWithPartition;
class MergeTreeData;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
}

namespace local_engine
{

struct PartInfo
{
    String part_name;
    size_t mark_count;
    size_t disk_size;
    size_t row_count;
    std::unordered_map<String, String> partition_values;
    String bucket_id;

    bool operator<(const PartInfo & rhs) const { return disk_size < rhs.disk_size; }
};

class SparkMergeTreeWriter;

class StorageMergeTreeWrapper;
using StorageMergeTreeWrapperPtr = std::shared_ptr<StorageMergeTreeWrapper>;
class StorageMergeTreeWrapper
{
    friend class SparkMergeTreeWriter;

protected:
    CustomStorageMergeTreePtr merge_tree = nullptr;
    bool isRemoteStorage;
    DB::StorageMetadataPtr metadata_snapshot;
    DB::Block header;

public:
    virtual ~StorageMergeTreeWrapper() = default;
    explicit StorageMergeTreeWrapper(const CustomStorageMergeTreePtr & data_, bool isRemoteStorage_)
        : merge_tree(data_)
        , isRemoteStorage(isRemoteStorage_)
        , metadata_snapshot(merge_tree->getInMemoryMetadataPtr())
        , header(metadata_snapshot->getSampleBlock())
    {
    }
    static StorageMergeTreeWrapperPtr
    create(const MergeTreeTable & merge_tree_table, bool insert_with_local_storage, const DB::ContextMutablePtr & context);

    virtual CustomStorageMergeTree & dest_storage() { return *merge_tree; }
    virtual CustomStorageMergeTreePtr temp_storage() { return nullptr; }
    CustomStorageMergeTreePtr data() const { return merge_tree; }
    CustomStorageMergeTree & dataRef() const { return *merge_tree; }

    virtual void commitPartToRemoteStorageIfNeeded(
        const std::deque<DB::MergeTreeDataPartPtr> & parts, const ReadSettings & read_settings, const WriteSettings & write_settings)
    {
    }
    void saveMetadata(const std::deque<DB::MergeTreeDataPartPtr> & parts, const DB::ContextPtr & context);
};

class DirectStorageMergeTreeWrapper : public StorageMergeTreeWrapper
{
public:
    explicit DirectStorageMergeTreeWrapper(const CustomStorageMergeTreePtr & data_, bool isRemoteStorage_)
        : StorageMergeTreeWrapper(data_, isRemoteStorage_)
    {
    }
};

class CopyToRemoteStorageMergeTreeWrapper : public StorageMergeTreeWrapper
{
    CustomStorageMergeTreePtr org_storage;

public:
    explicit CopyToRemoteStorageMergeTreeWrapper(const CustomStorageMergeTreePtr & data_, const CustomStorageMergeTreePtr & org_)
        : StorageMergeTreeWrapper(data_, true), org_storage(org_)
    {
        assert(merge_tree != org_storage);
    }

    CustomStorageMergeTree & dest_storage() override { return *org_storage; }
    CustomStorageMergeTreePtr temp_storage() override { return merge_tree; }

    void commitPartToRemoteStorageIfNeeded(
        const std::deque<DB::MergeTreeDataPartPtr> & parts,
        const ReadSettings & read_settings,
        const WriteSettings & write_settings) override;
};

class SparkMergeTreeWriter
{
public:
    static String partInfosToJson(const std::vector<PartInfo> & part_infos);
    SparkMergeTreeWriter(
        const MergeTreeTable & merge_tree_table, const GlutenMergeTreeWriteSettings & write_settings_, const DB::ContextPtr & context_);

    void write(const DB::Block & block);
    void finalize();
    std::vector<PartInfo> getAllPartInfo();

private:
    DB::MergeTreeDataWriter::TemporaryPart
    writeTempPartAndFinalize(DB::BlockWithPartition & block_with_partition, const DB::StorageMetadataPtr & metadata_snapshot) const;
    void checkAndMerge(bool force = false);
    void safeEmplaceBackPart(DB::MergeTreeDataPartPtr);
    void safeAddPart(DB::MergeTreeDataPartPtr);
    void finalizeMerge();
    bool chunkToPart(Chunk && plan_chunk);
    bool blockToPart(Block & block);

    const GlutenMergeTreeWriteSettings write_settings;
    DB::ContextPtr context;


    StorageMergeTreeWrapperPtr dataWrapper = nullptr;

    std::unique_ptr<DB::Squashing> squashing;
    int part_num = 1;
    ConcurrentDeque<DB::MergeTreeDataPartPtr> new_parts;
    std::unordered_map<String, String> partition_values;
    std::unordered_set<String> tmp_parts;

    ThreadPool thread_pool;

    std::mutex memory_mutex;
};
}
