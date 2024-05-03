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

#include "SortedPartitionDataMerger.h"
using namespace DB;
namespace local_engine
{
SortedPartitionDataMerger::SortedPartitionDataMerger(
    std::unique_ptr<MergingSortedAlgorithm> algorithm,
    const std::vector<TemporaryFileStream *> & streams,
    std::queue<DB::Block> & extra_blocks_in_memory_,
    const Block & output_header_)
{
    merging_algorithm = std::move(algorithm);
    IMergingAlgorithm::Inputs initial_inputs;
    bool use_in_memory_data = !extra_blocks_in_memory_.empty();
    for (auto * stream : streams)
    {
        Block data = stream->read();
        IMergingAlgorithm::Input input;
        input.set({data.getColumns(), data.rows()});
        initial_inputs.emplace_back(std::move(input));
    }
    if (use_in_memory_data)
    {
        IMergingAlgorithm::Input input;
        const auto & data = extra_blocks_in_memory_.front();
        input.set({data.getColumns(), data.rows()});
        initial_inputs.emplace_back(std::move(input));
        extra_blocks_in_memory_.pop();
    }
    for (int i = 0; i < streams.size(); ++i)
        sources.emplace_back(std::make_shared<SortedData>(streams[i], i));
    if (use_in_memory_data)
        sources.emplace_back(std::make_shared<SortedData>(extra_blocks_in_memory_, sources.size()));
    output_header = output_header_;
    merging_algorithm->initialize(std::move(initial_inputs));
}

int64_t searchLastPartitionIdIndex(ColumnPtr column, size_t start, size_t partition_id)
{
    const auto * int64_column = checkAndGetColumn<ColumnUInt64>(*column);
    int64_t low = start, high = int64_column->size() - 1;
    while (low <= high)
    {
        int64_t mid = low + (high - low) / 2;
        if (int64_column->get64(mid) > partition_id)
            high = mid - 1;
        else
            low = mid + 1;
        if (int64_column->get64(high) == partition_id)
            return high;
    }
    return -1;
}

SortedPartitionDataMerger::Result SortedPartitionDataMerger::next()
{
    if (finished)
        return Result{.empty = true};
    Chunk chunk;
    while (true)
    {
        auto result = merging_algorithm->merge();

        if (result.required_source >= 0)
        {
            auto stream = sources[result.required_source];
            auto block = stream->next();
            if (block)
            {
                IMergingAlgorithm::Input input;
                input.set({block.getColumns(), block.rows()});
                merging_algorithm->consume(input, stream->getPartitionId());
            }
        }
        if (result.chunk.getNumRows() > 0)
        {
            chunk = std::move(result.chunk);
            break;
        }
        if (result.is_finished)
        {
            finished = true;
            if (chunk.getNumRows() == 0)
                return Result{.empty = true};
            break;
        }
    }
    Result partitions;
    size_t row_idx = 0;
    Columns result_columns;
    result_columns.reserve(chunk.getColumns().size() - 1);
    for (size_t i = 0; i < chunk.getColumns().size() - 1; ++i)
        result_columns.push_back(chunk.getColumns()[i]);
    while (row_idx < chunk.getNumRows())
    {
        auto idx = searchLastPartitionIdIndex(chunk.getColumns().back(), row_idx, current_partition_id);
        if (idx >= 0)
        {
            if (row_idx == 0 && idx == chunk.getNumRows() - 1)
            {
                partitions.blocks.emplace_back(output_header.cloneWithColumns(result_columns), current_partition_id);
                break;
            }
            else
            {
                Columns cut_columns;
                cut_columns.reserve(result_columns.size());
                for (auto & result_column : result_columns)
                    cut_columns.push_back(result_column->cut(row_idx, idx - row_idx + 1));
                partitions.blocks.emplace_back(output_header.cloneWithColumns(cut_columns), current_partition_id);
                row_idx = idx + 1;
                if (idx != chunk.getNumRows() - 1)
                    current_partition_id++;
            }
        }
        else
        {
            current_partition_id++;
        }
    }
    return partitions;
}
}