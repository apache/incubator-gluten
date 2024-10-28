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

#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MutationCommands.h>
#include <Storages/StorageMergeTree.h>

namespace local_engine
{
struct SparkMergeTreeWritePartitionSettings;
using namespace DB;

class SparkMergeTreeDataWriter
{
public:
    explicit SparkMergeTreeDataWriter(MergeTreeData & data_) : data(data_), log(getLogger(data.getLogName() + " (Writer)")) { }
    MergeTreeDataWriter::TemporaryPart writeTempPart(
        DB::BlockWithPartition & block_with_partition,
        const DB::StorageMetadataPtr & metadata_snapshot,
        const ContextPtr & context,
        const std::string & part_dir) const;

private:
    MergeTreeData & data;
    LoggerPtr log;
};

class SparkStorageMergeTree : public MergeTreeData
{
    friend class MergeSparkMergeTreeTask;

    struct SparkMutationsSnapshot : public IMutationsSnapshot
    {
        SparkMutationsSnapshot() = default;

        MutationCommands getAlterMutationCommandsForPart(const MergeTreeData::DataPartPtr & part) const override { return {}; }
        std::shared_ptr<MergeTreeData::IMutationsSnapshot> cloneEmpty() const override
        {
            return std::make_shared<SparkMutationsSnapshot>();
        }

        NameSet getAllUpdatedColumns() const override { return {}; }
    };

public:
    static void wrapRangesInDataParts(DB::ReadFromMergeTree & source, const DB::RangesInDataParts & ranges);
    static void analysisPartsByRanges(DB::ReadFromMergeTree & source, const DB::RangesInDataParts & ranges_in_data_parts);
    std::string getName() const override;
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & executor) override;
    std::map<std::string, MutationCommands> getUnfinishedMutationCommands() const override;
    std::vector<MergeTreeDataPartPtr> loadDataPartsWithNames(const std::unordered_set<std::string> & parts);
    void removePartFromMemory(const MergeTreeData::DataPart & part_to_detach);

    MergeTreeDataSelectExecutor reader;
    MergeTreeDataMergerMutator merger_mutator;

protected:
    SparkStorageMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        const ContextMutablePtr & context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag = false);

private:
    static std::atomic<int> part_num;
    SimpleIncrement increment;

    void prefetchMetaDataFile(std::unordered_set<std::string> parts) const;
    void startBackgroundMovesIfNeeded() override;
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;
    LoadPartResult loadDataPart(
        const MergeTreePartInfo & part_info, const String & part_name, const DiskPtr & part_disk_ptr, MergeTreeDataPartState to_state);

protected:
    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr context) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context) override;
    PartitionCommandsResultInfo
    attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr context) override;
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) override;
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;
    void attachRestoredParts(MutableDataPartsVector && /*parts*/) override { throw std::runtime_error("not implement"); }

public:
    MutationsSnapshotPtr getMutationsSnapshot(const IMutationsSnapshot::Params & /*params*/) const override
    {
        return std::make_shared<SparkMutationsSnapshot>();
    };
};

class SparkWriteStorageMergeTree final : public SparkStorageMergeTree
{
public:
    SparkWriteStorageMergeTree(const MergeTreeTable & table_, const StorageInMemoryMetadata & metadata, const ContextMutablePtr & context_)
        : SparkStorageMergeTree(
              StorageID(table_.database, table_.table),
              table_.relative_path,
              metadata,
              false,
              context_,
              "",
              MergingParams(),
              buildMergeTreeSettings(table_.table_configs),
              false /*has_force_restore_data_flag*/)
        , table(table_)
        , writer(*this)
    {
    }

    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    SparkMergeTreeDataWriter & getWriter() { return writer; }

private:
    MergeTreeTable table;
    SparkMergeTreeDataWriter writer;
};

}
