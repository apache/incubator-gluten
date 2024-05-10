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
#include <Interpreters/SquashingTransform.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/StorageMergeTreeFactory.h>
#include <Poco/StringTokenizer.h>
#include <Common/CHUtil.h>
#include <Common/MergeTreeTool.h>

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

class SparkMergeTreeWriter
{
public:
    static String partInfosToJson(const std::vector<PartInfo> & part_infos);
    SparkMergeTreeWriter(
        const MergeTreeTable & merge_tree_table,
        const DB::ContextPtr & context_,
        const String & part_name_prefix_,
        const String & partition_dir_ = "",
        const String & bucket_dir_ = "");

    void write(DB::Block & block);
    void finalize();
    std::vector<PartInfo> getAllPartInfo();

private:
    void
    writeTempPart(MergeTreeDataWriter::TemporaryPart & temp_part, DB::BlockWithPartition & block_with_partition, const DB::StorageMetadataPtr & metadata_snapshot);
    DB::MergeTreeDataWriter::TemporaryPart
    writeTempPartAndFinalize(DB::BlockWithPartition & block_with_partition, const DB::StorageMetadataPtr & metadata_snapshot);
    void checkAndMerge(bool force = false);
    void safeEmplaceBackPart(DB::MergeTreeDataPartPtr);
    void safeAddPart(DB::MergeTreeDataPartPtr);
    void manualFreeMemory(size_t before_write_memory);

    CustomStorageMergeTreePtr storage = nullptr;
    CustomStorageMergeTreePtr dest_storage = nullptr;
    CustomStorageMergeTreePtr temp_storage = nullptr;
    DB::StorageMetadataPtr metadata_snapshot = nullptr;

    String part_name_prefix;
    String partition_dir;
    String bucket_dir;

    DB::ContextPtr context;
    std::unique_ptr<DB::SquashingTransform> squashing_transform;
    int part_num = 1;
    ConcurrentDeque<DB::MergeTreeDataPartPtr> new_parts;
    std::unordered_map<String, String> partition_values;
    std::unordered_set<String> tmp_parts;
    DB::Block header;
    bool merge_after_insert;
    FreeThreadPool thread_pool;
    size_t merge_min_size = 1024 * 1024 * 1024;
    size_t merge_limit_parts = 10;
    std::mutex memory_mutex;
    bool isRemoteDisk = false;
};

}
