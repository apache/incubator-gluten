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

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MergeTree/CustomStorageMergeTree.h>
#include <Common/CHUtil.h>
#include <Common/GlutenSettings.h>

namespace local_engine
{
struct MergeTreeTable;
using CustomStorageMergeTreePtr = std::shared_ptr<CustomStorageMergeTree>;

#define MERGE_TREE_WRITE_RELATED_SETTINGS(M, ALIAS, UNIQ) \
    M(String, part_name_prefix, , "The part name prefix for writing data", UNIQ) \
    M(String, partition_dir, , "The parition directory for writing data", UNIQ) \
    M(String, bucket_dir, , "The bucket directory for writing data", UNIQ)

DECLARE_GLUTEN_SETTINGS(MergeTreePartitionWriteSettings, MERGE_TREE_WRITE_RELATED_SETTINGS)

struct GlutenMergeTreeWriteSettings
{
    MergeTreePartitionWriteSettings partition_settings;
    bool merge_after_insert{true};
    bool insert_without_local_storage{false};
    size_t merge_min_size = 1024 * 1024 * 1024;
    size_t merge_limit_parts = 10;

    void load(const DB::ContextPtr & context)
    {
        const DB::Settings & settings = context->getSettingsRef();
        merge_after_insert = settings.get(MERGETREE_MERGE_AFTER_INSERT).safeGet<bool>();
        insert_without_local_storage = settings.get(MERGETREE_INSERT_WITHOUT_LOCAL_STORAGE).safeGet<bool>();

        if (Field limit_size_field; settings.tryGet("optimize.minFileSize", limit_size_field))
            merge_min_size = limit_size_field.safeGet<Int64>() <= 0 ? merge_min_size : limit_size_field.safeGet<Int64>();

        if (Field limit_cnt_field; settings.tryGet("mergetree.max_num_part_per_merge_task", limit_cnt_field))
            merge_limit_parts = limit_cnt_field.safeGet<Int64>() <= 0 ? merge_limit_parts : limit_cnt_field.safeGet<Int64>();
    }
};

class SparkMergeTreeDataWriter
{
public:
    explicit SparkMergeTreeDataWriter(MergeTreeData & data_) : data(data_), log(getLogger(data.getLogName() + " (Writer)")) { }
    MergeTreeDataWriter::TemporaryPart writeTempPart(
        DB::BlockWithPartition & block_with_partition,
        const DB::StorageMetadataPtr & metadata_snapshot,
        const ContextPtr & context,
        const MergeTreePartitionWriteSettings & write_settings,
        int part_num) const;

private:
    MergeTreeData & data;
    LoggerPtr log;
};

class SparkMergeTreeSink;

class SparkStorageMergeTree final : public CustomStorageMergeTree
{
    friend class SparkMergeTreeSink;

public:
    SparkStorageMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        const ContextMutablePtr & context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_)
        : CustomStorageMergeTree(
              table_id_,
              relative_data_path_,
              metadata,
              attach,
              context_,
              date_column_name,
              merging_params_,
              std::move(settings_),
              false /*has_force_restore_data_flag*/)
        , writer(*this)
    {
    }

    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

private:
    SparkMergeTreeDataWriter writer;
};


// TODO: Remove ConcurrentDeque
template <typename T>
class ConcurrentDeque
{
public:
    std::optional<T> pop_front()
    {
        std::lock_guard<std::mutex> lock(mtx);

        if (deq.empty())
            return {};

        T t = deq.front();
        deq.pop_front();
        return t;
    }

    void emplace_back(T value)
    {
        std::lock_guard<std::mutex> lock(mtx);
        deq.emplace_back(value);
    }

    void emplace_back(std::vector<T> values)
    {
        std::lock_guard<std::mutex> lock(mtx);
        deq.insert(deq.end(), values.begin(), values.end());
    }

    void emplace_front(T value)
    {
        std::lock_guard<std::mutex> lock(mtx);
        deq.emplace_front(value);
    }

    size_t size()
    {
        std::lock_guard<std::mutex> lock(mtx);
        return deq.size();
    }

    bool empty()
    {
        std::lock_guard<std::mutex> lock(mtx);
        return deq.empty();
    }

    /// !!! unsafe get, only called when background tasks are finished
    const std::deque<T> & unsafeGet() const { return deq; }

private:
    std::deque<T> deq;
    mutable std::mutex mtx;
};

class SinkHelper;
using SinkHelperPtr = std::shared_ptr<SinkHelper>;
class SinkHelper
{
protected:
    const GlutenMergeTreeWriteSettings write_settings;
    CustomStorageMergeTreePtr data;
    bool isRemoteStorage;

    ConcurrentDeque<DB::MergeTreeDataPartPtr> new_parts;
    std::unordered_set<String> tmp_parts{};
    ThreadPool thread_pool;

public:
    const DB::StorageMetadataPtr metadata_snapshot;
    const DB::Block header;

protected:
    void doMergePartsAsync(const std::vector<DB::MergeTreeDataPartPtr> & prepare_merge_parts);
    virtual void cleanup() { }

public:
    void emplacePart(const DB::MergeTreeDataPartPtr & part) { new_parts.emplace_back(part); }

    const std::deque<DB::MergeTreeDataPartPtr> & unsafeGet() const { return new_parts.unsafeGet(); }
    void checkAndMerge(bool force = false);
    void finalizeMerge();

    virtual ~SinkHelper() = default;
    explicit SinkHelper(
        const CustomStorageMergeTreePtr & data_, const GlutenMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_);
    static SinkHelperPtr create(
        const MergeTreeTable & merge_tree_table,
        const GlutenMergeTreeWriteSettings & write_settings_,
        const DB::ContextMutablePtr & context);

    virtual CustomStorageMergeTree & dest_storage() { return *data; }
    CustomStorageMergeTree & dataRef() const { return *data; }

    virtual void commit(const ReadSettings & read_settings, const WriteSettings & write_settings) { }
    void saveMetadata(const DB::ContextPtr & context);
};

class DirectSinkHelper : public SinkHelper
{
protected:
    void cleanup() override;

public:
    explicit DirectSinkHelper(
        const CustomStorageMergeTreePtr & data_, const GlutenMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_)
        : SinkHelper(data_, write_settings_, isRemoteStorage_)
    {
    }
};

class CopyToRemoteSinkHelper : public SinkHelper
{
    CustomStorageMergeTreePtr dest;

public:
    explicit CopyToRemoteSinkHelper(
        const CustomStorageMergeTreePtr & temp,
        const CustomStorageMergeTreePtr & dest_,
        const GlutenMergeTreeWriteSettings & write_settings_)
        : SinkHelper(temp, write_settings_, true), dest(dest_)
    {
        assert(data != dest);
    }

    CustomStorageMergeTree & dest_storage() override { return *dest; }
    const CustomStorageMergeTreePtr & temp_storage() const { return data; }

    void commit(const ReadSettings & read_settings, const WriteSettings & write_settings) override;
};

class SparkMergeTreeSink : public DB::SinkToStorage
{
public:
    explicit SparkMergeTreeSink(
        SparkStorageMergeTree & storage_, const StorageMetadataPtr & metadata_snapshot_, const ContextPtr & context_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_)
        , write_settings(MergeTreePartitionWriteSettings::get(context_))
    {
    }
    ~SparkMergeTreeSink() override = default;

    String getName() const override { return "SparkMergeTreeSink"; }
    void consume(Chunk & chunk) override;
    void onStart() override;
    void onFinish() override;

private:
    SparkStorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    MergeTreePartitionWriteSettings write_settings;
    int part_num = 1;
    std::vector<DB::MergeTreeDataPartPtr> new_parts{};
};

}
