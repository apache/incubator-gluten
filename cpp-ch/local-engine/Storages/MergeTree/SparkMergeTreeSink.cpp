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
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace local_engine
{

IMPLEMENT_GLUTEN_SETTINGS(MergeTreePartitionWriteSettings, MERGE_TREE_WRITE_RELATED_SETTINGS)

MergeTreeDataWriter::TemporaryPart SparkMergeTreeDataWriter::writeTempPart(
    BlockWithPartition & block_with_partition,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context,
    const MergeTreePartitionWriteSettings & write_settings,
    int part_num) const
{
    const std::string & part_name_prefix = write_settings.part_name_prefix;
    const std::string & partition_dir = write_settings.partition_dir;
    const std::string & bucket_dir = write_settings.bucket_dir;


    MergeTreeDataWriter::TemporaryPart temp_part;

    Block & block = block_with_partition.block;

    auto columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());

    for (auto & column : columns)
        if (column.type->hasDynamicSubcolumns())
            column.type = block.getByName(column.name).type;

    auto minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
    minmax_idx->update(block, MergeTreeData::getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));

    MergeTreePartition partition(block_with_partition.partition);

    MergeTreePartInfo new_part_info(partition.getID(metadata_snapshot->getPartitionKey().sample_block), 1, 1, 0);

    std::string part_dir;
    if (!partition_dir.empty() && !bucket_dir.empty())
        part_dir = fmt::format("{}/{}/{}_{:03d}", partition_dir, bucket_dir, part_name_prefix, part_num);
    else if (!partition_dir.empty())
        part_dir = fmt::format("{}/{}_{:03d}", partition_dir, part_name_prefix, part_num);
    else if (!bucket_dir.empty())
        part_dir = fmt::format("{}/{}_{:03d}", bucket_dir, part_name_prefix, part_num);
    else
        part_dir = fmt::format("{}_{:03d}", part_name_prefix, part_num);

    // assert(part_num > 0 && !part_name_prefix.empty());

    String part_name = part_dir;

    temp_part.temporary_directory_lock = data.getTemporaryPartDirectoryHolder(part_dir);

    auto indices = MergeTreeIndexFactory::instance().getMany(metadata_snapshot->getSecondaryIndices());

    /// If we need to calculate some columns to sort.
    if (metadata_snapshot->hasSortingKey() || metadata_snapshot->hasSecondaryIndices())
        data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot, indices)->execute(block);

    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(sort_columns[i], 1, 1);

    /// Sort
    IColumn::Permutation * perm_ptr = nullptr;
    IColumn::Permutation perm;
    if (!sort_description.empty())
    {
        if (!isAlreadySorted(block, sort_description))
        {
            stableGetPermutation(block, sort_description, perm);
            perm_ptr = &perm;
        }
    }

    Names partition_key_columns = metadata_snapshot->getPartitionKey().column_names;

    /// Size of part would not be greater than block.bytes() + epsilon
    size_t expected_size = block.bytes();

    /// If optimize_on_insert is true, block may become empty after merge.
    /// There is no need to create empty part.
    if (expected_size == 0)
        return temp_part;

    VolumePtr volume = data.getStoragePolicy()->getVolume(0);
    VolumePtr data_part_volume = std::make_shared<SingleDiskVolume>(volume->getName(), volume->getDisk(), volume->max_data_part_size);
    auto new_data_part = data.getDataPartBuilder(part_name, data_part_volume, part_dir)
                             .withPartFormat(data.choosePartFormat(expected_size, block.rows()))
                             .withPartInfo(new_part_info)
                             .build();

    auto data_part_storage = new_data_part->getDataPartStoragePtr();


    const auto & data_settings = data.getSettings();

    SerializationInfo::Settings settings{data_settings->ratio_of_defaults_for_sparse_serialization, true};
    SerializationInfoByName infos(columns, settings);
    infos.add(block);

    new_data_part->setColumns(columns, infos, metadata_snapshot->getMetadataVersion());
    new_data_part->rows_count = block.rows();
    new_data_part->partition = std::move(partition);
    new_data_part->minmax_idx = std::move(minmax_idx);

    data_part_storage->beginTransaction();
    SyncGuardPtr sync_guard;
    if (new_data_part->isStoredOnDisk())
    {
        /// The name could be non-unique in case of stale files from previous runs.
        String full_path = new_data_part->getDataPartStorage().getFullPath();

        if (new_data_part->getDataPartStorage().exists())
        {
            // LOG_WARNING(log, "Removing old temporary directory {}", full_path);
            data_part_storage->removeRecursive();
        }

        data_part_storage->createDirectories();

        if (data.getSettings()->fsync_part_directory)
        {
            const auto disk = data_part_volume->getDisk();
            sync_guard = disk->getDirectorySyncGuard(full_path);
        }
    }

    /// This effectively chooses minimal compression method:
    ///  either default lz4 or compression method with zero thresholds on absolute and relative part size.
    auto compression_codec = data.getContext()->chooseCompressionCodec(0, 0);
    auto txn = context->getCurrentTransaction();
    auto out = std::make_unique<MergedBlockOutputStream>(
        new_data_part,
        metadata_snapshot,
        columns,
        indices,
        MergeTreeStatisticsFactory::instance().getMany(metadata_snapshot->getColumns()),
        compression_codec,
        txn ? txn->tid : Tx::PrehistoricTID,
        false,
        false,
        context->getWriteSettings());

    out->writeWithPermutation(block, perm_ptr);
    auto finalizer = out->finalizePartAsync(new_data_part, data_settings->fsync_after_insert, nullptr, nullptr);

    temp_part.part = new_data_part;
    temp_part.streams.emplace_back(MergeTreeDataWriter::TemporaryPart::Stream{.stream = std::move(out), .finalizer = std::move(finalizer)});
    temp_part.finalize();
    data_part_storage->commitTransaction();
    return temp_part;
}


SinkToStoragePtr SparkStorageMergeTree::write(
    const ASTPtr &, const StorageMetadataPtr & storage_in_memory_metadata, ContextPtr context, bool /*async_insert*/)
{
    return std::make_shared<SparkMergeTreeSink>(*this, storage_in_memory_metadata, context);
}

void SparkMergeTreeSink::consume(Chunk & chunk)
{
    assert(!metadata_snapshot->hasPartitionKey());
    auto block = getHeader().cloneWithColumns(chunk.getColumns());
    auto blocks_with_partition = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), 10, metadata_snapshot, context);

    for (auto & item : blocks_with_partition)
    {
        size_t before_write_memory = 0;
        if (auto * memory_tracker = CurrentThread::getMemoryTracker())
        {
            CurrentThread::flushUntrackedMemory();
            before_write_memory = memory_tracker->get();
        }

        MergeTreeDataWriter::TemporaryPart temp_part
            = storage.writer.writeTempPart(item, metadata_snapshot, context, write_settings, part_num);
        new_parts.emplace_back(temp_part.part);
        part_num++;
        /// Reset earlier to free memory
        item.block.clear();
        item.partition.clear();
    }
}

void SparkMergeTreeSink::onStart()
{
    // DO NOTHING
}

void SparkMergeTreeSink::onFinish()
{
    // DO NOTHING
}

}