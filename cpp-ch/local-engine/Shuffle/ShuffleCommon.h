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
#include <memory>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Storages/IO/NativeWriter.h>
#include <Shuffle/SelectorBuilder.h>
#include <base/types.h>
#include <Storages/IO/CompressedWriteBuffer.h>


namespace local_engine
{
    class SparkExchangeManager;
}

namespace local_engine
{
struct SplitOptions
{
    size_t split_size = DB::DEFAULT_BLOCK_SIZE;
    size_t io_buffer_size = DB::DBMS_DEFAULT_BUFFER_SIZE;
    std::string data_file;
    std::vector<std::string> local_dirs_list;
    int num_sub_dirs;
    int shuffle_id;
    int map_id;
    size_t partition_num;
    std::string hash_exprs;
    std::string out_exprs;
    std::string compress_method = "zstd";
    std::optional<int> compress_level;
    size_t spill_threshold = 300 * 1024 * 1024;
    std::string hash_algorithm;
    size_t max_sort_buffer_size = 1_GiB;
    bool force_memory_sort = false;
};

class ColumnsBuffer
{
public:
    ColumnsBuffer(size_t prefer_buffer_size = 8192);
    ~ColumnsBuffer() = default;

    void add(DB::Block & columns, int start, int end);
    void appendSelective(size_t column_idx, const DB::Block & source, const DB::IColumn::Selector & selector, size_t from, size_t length);

    DB::Block getHeader();
    size_t size() const;
    bool empty() const;

    DB::Block releaseColumns();

    size_t bytes() const
    {
        size_t res = 0;
        for (const auto & col : accumulated_columns)
            res += col->byteSize();
        return res;
    }

private:
    DB::MutableColumns accumulated_columns;
    DB::Block header;
    size_t prefer_buffer_size;
};
using ColumnsBufferPtr = std::shared_ptr<ColumnsBuffer>;

struct SplitResult
{
    UInt64 total_compute_pid_time = 0;           // Total nanoseconds to compute partition id
    UInt64 total_write_time = 0;                 // Total nanoseconds to write data to local/celeborn, including the time writing to buffer
    UInt64 total_spill_time = 0;                 // Total nanoseconds to execute PartitionWriter::evictPartitions
    UInt64 total_compress_time = 0;              // Total nanoseconds to execute compression before writing data to local/celeborn
    UInt64 total_bytes_written = 0;              // Sum of partition_length
    UInt64 total_bytes_spilled = 0;              // Total bytes of blocks spilled to local/celeborn before serialization and compression
    std::vector<UInt64> partition_lengths;        // Total written bytes of each partition after serialization and compression
    std::vector<UInt64> raw_partition_lengths;    // Total written bytes of each partition after serialization
    UInt64 total_split_time = 0;                 // Total nanoseconds to execute CachedShuffleWriter::split, excluding total_compute_pid_time
    UInt64 total_io_time = 0;                    // Total nanoseconds to write data to local/celeborn, excluding the time writing to buffer
    UInt64 total_serialize_time = 0;             // Total nanoseconds to execute spill_to_file/spill_to_celeborn. Bad naming, it works not as the name suggests.
    UInt64 total_rows = 0;
    UInt64 total_blocks = 0;
    UInt64 wall_time = 0;                        // Wall nanoseconds time of shuffle.

    String toString() const
    {
        std::ostringstream oss;

        auto to_seconds = [](UInt64 nanoseconds) -> double {
            return static_cast<double>(nanoseconds) / 1000000000ULL;
        };

        oss << "compute_pid_time(s):" << to_seconds(total_compute_pid_time) << " split_time(s):" << to_seconds(total_split_time)
            << " spill time(s):" << to_seconds(total_spill_time) << " serialize_time(s):" << to_seconds(total_serialize_time)
            << " compress_time(s):" << to_seconds(total_compress_time) << " write_time(s):" << to_seconds(total_write_time)
            << " bytes_writen:" << total_bytes_written << " bytes_spilled:" << total_bytes_spilled
            << " partition_num: " << partition_lengths.size() << std::endl;
        return oss.str();
    }
};

struct SplitterHolder
{
    std::unique_ptr<SparkExchangeManager> exchange_manager;
};


}
