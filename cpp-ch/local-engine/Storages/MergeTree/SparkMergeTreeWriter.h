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
        const MergeTreeTable & merge_tree_table, const GlutenMergeTreeWriteSettings & write_settings_, const DB::ContextPtr & context_);

    void write(const DB::Block & block);
    void finalize();
    std::vector<PartInfo> getAllPartInfo() const;

private:
    DB::MergeTreeDataWriter::TemporaryPart
    writeTempPartAndFinalize(DB::BlockWithPartition & block_with_partition, const DB::StorageMetadataPtr & metadata_snapshot) const;
    bool chunkToPart(Chunk && plan_chunk);
    bool blockToPart(Block & block);

    const GlutenMergeTreeWriteSettings write_settings;
    SinkHelperPtr dataWrapper;
    DB::ContextPtr context;
    std::unordered_map<String, String> partition_values;


    std::unique_ptr<DB::Squashing> squashing;

    int part_num = 1;
};
}
