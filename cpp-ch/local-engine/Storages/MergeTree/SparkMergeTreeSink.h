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
#include <Common/GlutenSettings.h>

namespace local_engine
{

#define MERGE_TREE_WRITE_RELATED_SETTINGS(M, ALIAS, UNIQ) \
    M(String, part_name_prefix, , "The part name prefix for writing data", UNIQ) \
    M(String, partition_dir, , "The parition directory for writing data", UNIQ) \
    M(String, bucket_dir, , "The bucket directory for writing data", UNIQ)

DECLARE_GLUTEN_SETTINGS(GlutenMergeTreeWriteSettings, MERGE_TREE_WRITE_RELATED_SETTINGS)

class SparkMergeTreeDataWriter
{
public:
    struct PartitionInfo
    {
        std::string part_name_prefix;
        std::string partition_dir;
        std::string bucket_dir;
        int part_num;
    };

    explicit SparkMergeTreeDataWriter(MergeTreeData & data_) : data(data_), log(getLogger(data.getLogName() + " (Writer)")) { }
    MergeTreeDataWriter::TemporaryPart writeTempPart(
        DB::BlockWithPartition & block_with_partition,
        const DB::StorageMetadataPtr & metadata_snapshot,
        const ContextPtr & context,
        const GlutenMergeTreeWriteSettings & write_settings,
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

class SparkMergeTreeSink : public DB::SinkToStorage
{
public:
    explicit SparkMergeTreeSink(
        SparkStorageMergeTree & storage_, const StorageMetadataPtr & metadata_snapshot_, const ContextPtr & context_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_)
        , write_settings(GlutenMergeTreeWriteSettings::get(context_))
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
    GlutenMergeTreeWriteSettings write_settings;
    int part_num = 1;
    std::vector<DB::MergeTreeDataPartPtr> new_parts{};
};

}
