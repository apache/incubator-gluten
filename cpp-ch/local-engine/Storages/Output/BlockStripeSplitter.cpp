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
#include "BlockStripeSplitter.h"
#include <Columns/ColumnNullable.h>

using namespace local_engine;

BlockStripes
local_engine::BlockStripeSplitter::split(const DB::Block & block, const std::vector<size_t> & partition_column_indices, bool has_bucket, bool reserve_partition_columns)
{
    BlockStripes ret;
    ret.origin_block_address = reinterpret_cast<int64_t>(&block);
    ret.origin_block_num_columns = static_cast<int>(block.columns());

    /// In case block has zero rows
    const size_t rows = block.rows();
    if (rows == 0)
        return ret;

    std::vector<size_t> partition_bucket_column_indices = partition_column_indices;
    if (has_bucket)
        partition_bucket_column_indices.push_back(block.columns() - 1);

    std::vector<size_t> split_points;
    for (size_t i = 0; i < partition_bucket_column_indices.size(); i++)
    {
        auto column = block.safeGetByPosition(partition_bucket_column_indices.at(i)).column;

        if (i == 0 && column->compareAt(0, rows - 1, *column, 1) == 0)
        {
            /// No value changes for this whole column
            continue;
        }

        for (size_t j = 1; j < rows; ++j)
        {
            if (column->compareAt(j - 1, j, *column, 1) != 0)
                split_points.push_back(j);
        }
    }

    const bool no_need_split = split_points.empty();

    /// Sort split points
    std::sort(split_points.begin(), split_points.end());

    /// Deduplicate split points
    split_points.erase(std::unique(split_points.begin(), split_points.end()), split_points.end());
    split_points.push_back(rows);

    /// Create output block by ignoring the partition cols
    DB::ColumnsWithTypeAndName output_columns;
    for (size_t col_i = 0; col_i < block.columns(); ++col_i)
    {
//        /// Partition columns will not be written to the file (they're written to folder name)

        if (!reserve_partition_columns && std::find(partition_column_indices.begin(), partition_column_indices.end(), col_i) != partition_column_indices.end())
            continue;

        /// The last column is a column representing bucketing hash value (__bucket_value__), which is not written to the file
        if (has_bucket && col_i == block.columns() - 1)
            continue;

        output_columns.push_back(block.getByPosition(col_i));
    }

    DB::Block output_block(output_columns);
    for (size_t i = 0; i < split_points.size(); i++)
    {
        size_t from = i == 0 ? 0 : split_points.at(i - 1);
        size_t to = split_points.at(i);

        DB::Block * p = nullptr;
        if (!no_need_split)
        {
            DB::Block cut_block = output_block.cloneWithCutColumns(from, to - from);
            p = new DB::Block(std::move(cut_block));
        }
        else
        {
            /// Optimization for no split
            p = new DB::Block(std::move(output_block));
        }

        ret.heading_row_indice.push_back(static_cast<int32_t>(from));
        ret.block_addresses.push_back(reinterpret_cast<int64_t>(p));
    }
    return ret;
}
