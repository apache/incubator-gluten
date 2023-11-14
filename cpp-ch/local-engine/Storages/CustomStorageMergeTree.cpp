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
#include "CustomStorageMergeTree.h"

namespace local_engine
{
CustomStorageMergeTree::CustomStorageMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool /*has_force_restore_data_flag*/)
    : MergeTreeData(
        table_id_,
        metadata_,
        context_,
        date_column_name,
        merging_params_,
        std::move(storage_settings_),
        false, /// require_part_metadata
        attach)
    , writer(*this)
    , reader(*this)
{
    initializeDirectoriesAndFormatVersion(relative_data_path_, attach, date_column_name);
}
void CustomStorageMergeTree::dropPartNoWaitNoThrow(const String & /*part_name*/)
{
    throw std::runtime_error("not implement");
}
void CustomStorageMergeTree::dropPart(const String & /*part_name*/, bool /*detach*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
void CustomStorageMergeTree::dropPartition(const ASTPtr & /*partition*/, bool /*detach*/, ContextPtr /*context*/)
{
}
PartitionCommandsResultInfo CustomStorageMergeTree::attachPartition(
    const ASTPtr & /*partition*/, const StorageMetadataPtr & /*metadata_snapshot*/, bool /*part*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
void CustomStorageMergeTree::replacePartitionFrom(
    const StoragePtr & /*source_table*/, const ASTPtr & /*partition*/, bool /*replace*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
void CustomStorageMergeTree::movePartitionToTable(const StoragePtr & /*dest_table*/, const ASTPtr & /*partition*/, ContextPtr /*context*/)
{
    throw std::runtime_error("not implement");
}
bool CustomStorageMergeTree::partIsAssignedToBackgroundOperation(const MergeTreeData::DataPartPtr & /*part*/) const
{
    throw std::runtime_error("not implement");
}

std::string CustomStorageMergeTree::getName() const
{
    throw std::runtime_error("not implement");
}
std::vector<MergeTreeMutationStatus> CustomStorageMergeTree::getMutationsStatus() const
{
    throw std::runtime_error("not implement");
}
bool CustomStorageMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & /*executor*/)
{
    throw std::runtime_error("not implement");
}
void CustomStorageMergeTree::startBackgroundMovesIfNeeded()
{
    throw std::runtime_error("not implement");
}
std::unique_ptr<MergeTreeSettings> CustomStorageMergeTree::getDefaultSettings() const
{
    throw std::runtime_error("not implement");
}
std::map<std::string, MutationCommands> CustomStorageMergeTree::getUnfinishedMutationCommands() const
{
    throw std::runtime_error("not implement");
}
}
