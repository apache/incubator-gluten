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
#include <queue>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>


namespace local_engine
{
class SortedPartitionDataMerger;
using SortedPartitionDataMergerPtr = std::unique_ptr<SortedPartitionDataMerger>;
class SortedPartitionDataMerger
{
public:
    struct Result
    {
        bool empty = false;
        std::vector<std::pair<DB::Block, size_t>> blocks;
    };

    class SortedData
    {
    public:
        SortedData(DB::TemporaryFileStream * stream, size_t partitionId) : stream(stream), partition_id(partitionId) { }
        SortedData(const std::queue<DB::Block> & blocksInMemory, size_t partitionId)
            : blocks_in_memory(blocksInMemory), partition_id(partitionId)
        {
        }
        DB::Block next()
        {
            if (stream)
            {
                auto data = stream->read();
                end = !data;
                return data;
            }
            if (!blocks_in_memory.empty())
            {
                auto block = blocks_in_memory.front();
                blocks_in_memory.pop();
                return block;
            }
            return {};
        }
        bool isEnd() const
        {
            if (stream)
                return stream->isEof();
            return blocks_in_memory.empty() || end;
        }
        size_t getPartitionId() const { return partition_id; }

    private:
        DB::TemporaryFileStream * stream = nullptr;
        std::queue<DB::Block> blocks_in_memory;
        size_t partition_id;
        bool end = false;
    };

    SortedPartitionDataMerger(
        std::unique_ptr<DB::MergingSortedAlgorithm> algorithm,
        const std::vector<DB::TemporaryFileStream *> & streams,
        std::queue<DB::Block> & extra_blocks_in_memory,
        const DB::Block & output_header);
    Result next();
    bool isFinished() const { return finished; }

private:
    std::unique_ptr<DB::MergingSortedAlgorithm> merging_algorithm;
    std::vector<std::shared_ptr<SortedData>> sources;
    DB::Block output_header;
    bool finished = false;
    size_t current_partition_id = 0;
};

}