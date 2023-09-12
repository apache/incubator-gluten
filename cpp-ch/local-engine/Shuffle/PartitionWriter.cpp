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
#include "PartitionWriter.h"
#include <filesystem>
#include <memory>
#include <ostream>
#include <vector>
#include <Storages/IO/CompressedWriteBuffer.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Shuffle/CachedShuffleWriter.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/CHUtil.h>
#include <IO/WriteBufferFromString.h>
#include <format>

using namespace DB;

namespace local_engine
{
void local_engine::PartitionWriter::write(const PartitionInfo& partition_info, DB::Block & data)
{
    Stopwatch time;
    time.start();
    for (size_t col = 0; col < data.columns(); ++col)
    {
        for (size_t j = 0; j < partition_info.partition_num; ++j)
        {
            size_t from = partition_info.partition_start_points[j];
            size_t length = partition_info.partition_start_points[j + 1] - from;
            if (length == 0)
                continue; // no data for this partition continue;
            partition_block_buffer[j].appendSelective(col, data, partition_info.partition_selector, from, length);
        }
    }

    for (size_t i = 0; i < shuffle_writer->options.partition_nums; ++i)
    {
        ColumnsBuffer & buffer = partition_block_buffer[i];
        if (buffer.size() >= shuffle_writer->options.split_size)
        {
            Block block = buffer.releaseColumns();
            auto bytes = block.bytes();
            total_partition_buffer_size += bytes;
            shuffle_writer->split_result.raw_partition_length[i] += bytes;
            partition_buffer[i].emplace_back(std::move(block));
        }
    }
    shuffle_writer->split_result.total_split_time += time.elapsedNanoseconds();
}

void LocalPartitionWriter::evictPartitions(bool for_memory_spill)
{

    auto spill_to_file = [this]() -> void {
        auto file = getNextSpillFile();
        WriteBufferFromFile output(file, shuffle_writer->options.io_buffer_size);
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
        CompressedWriteBuffer compressed_output(output, codec, shuffle_writer->options.io_buffer_size);
        NativeWriter writer(compressed_output, 0, shuffle_writer->output_header);
        SpillInfo info;
        info.spilled_file = file;
        size_t partition_id = 0;
        Stopwatch serialization_time_watch;
        serialization_time_watch.start();
        for (auto & partition : partition_buffer)
        {
            size_t raw_size = 0;
            PartitionSpillInfo partition_spill_info;
            partition_spill_info.start = output.count();
            for (const auto & block : partition)
            {
                raw_size += writer.write(block);
            }
            compressed_output.sync();
            partition_spill_info.length = output.count() - partition_spill_info.start;
            shuffle_writer->split_result.raw_partition_length[partition_id] += raw_size;
            partition_spill_info.partition_id = partition_id;
            partition_id++;
            info.partition_spill_infos.emplace_back(partition_spill_info);
        }
        spill_infos.emplace_back(info);
        shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
        shuffle_writer->split_result.total_write_time += compressed_output.getWriteTime();
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
    };
    Stopwatch spill_time_watch;
    spill_time_watch.start();
    if (for_memory_spill)
    {
        // escape memory track from current thread status; add untracked memory limit for create thread object, avoid trigger memory spill again
        IgnoreMemoryTracker ignore(2 * 1024 * 1024);
        ThreadFromGlobalPool thread(spill_to_file);
        thread.join();
    }
    else
    {
        spill_to_file();
    }
    shuffle_writer->split_result.total_spill_time += spill_time_watch.elapsedNanoseconds();

    for (auto & partition : partition_buffer)
    {
        partition.clear();
    }
    shuffle_writer->split_result.total_bytes_spilled += total_partition_buffer_size;
    total_partition_buffer_size = 0;
}
std::vector<Int64> LocalPartitionWriter::mergeSpills(WriteBuffer& data_file)
{
    auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
    CompressedWriteBuffer compressed_output(data_file, codec, shuffle_writer->options.io_buffer_size);
    NativeWriter writer(compressed_output, 0, shuffle_writer->output_header);

    std::vector<Int64> partition_length;
    partition_length.resize(shuffle_writer->options.partition_nums, 0);
    std::vector<ReadBufferPtr> spill_inputs;
    spill_inputs.reserve(spill_infos.size());
    for (const auto & spill : spill_infos)
    {
        spill_inputs.emplace_back(std::make_shared<ReadBufferFromFile>(spill.spilled_file, shuffle_writer->options.io_buffer_size));
    }

    Stopwatch write_time_watch;
    write_time_watch.start();
    Stopwatch io_time_watch;
    size_t merge_io_time = 0;
    String buffer;
    for (size_t partition_id = 0; partition_id < shuffle_writer->options.partition_nums; ++partition_id)
    {
        auto size_before = data_file.count();
        io_time_watch.restart();
        for (size_t i = 0; i < spill_infos.size(); ++i)
        {
            size_t size = spill_infos[i].partition_spill_infos[partition_id].length;
            buffer.reserve(size);
            auto count = spill_inputs[i]->readBig(buffer.data(), size);
            data_file.write(buffer.data(), count);
        }
        merge_io_time += io_time_watch.elapsedNanoseconds();

        if (partition_block_buffer[partition_id].size() > 0)
        {
            Block block = partition_block_buffer[partition_id].releaseColumns();
            partition_buffer[partition_id].emplace_back(std::move(block));
        }
        Stopwatch serialization_time_watch;
        serialization_time_watch.start();
        size_t raw_size = 0;
        for (const auto & block : partition_buffer[partition_id])
        {
            raw_size += writer.write(block);
        }
        compressed_output.sync();
        partition_length[partition_id] = data_file.count() - size_before;
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
        shuffle_writer->split_result.total_bytes_written += partition_length[partition_id];
        shuffle_writer->split_result.raw_partition_length[partition_id] += raw_size;
    }
    shuffle_writer->split_result.total_write_time += write_time_watch.elapsedNanoseconds();
    shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
    shuffle_writer->split_result.total_disk_time += compressed_output.getWriteTime();
    shuffle_writer->split_result.total_serialize_time = shuffle_writer->split_result.total_serialize_time - shuffle_writer->split_result.total_disk_time - shuffle_writer->split_result.total_compress_time;
    shuffle_writer->split_result.total_disk_time += merge_io_time;

    for (const auto & spill : spill_infos)
    {
        std::filesystem::remove(spill.spilled_file);
    }
    return partition_length;
}
LocalPartitionWriter::LocalPartitionWriter(CachedShuffleWriter * shuffle_writer)
    : PartitionWriter(shuffle_writer)
{
}
String LocalPartitionWriter::getNextSpillFile()
{
    auto file_name = std::to_string(options->shuffle_id) + "_" + std::to_string(options->map_id) + "_" + std::to_string(spill_infos.size());
    std::hash<std::string> hasher;
    auto hash = hasher(file_name);
    auto dir_id = hash % options->local_dirs_list.size();
    auto sub_dir_id = (hash / options->local_dirs_list.size()) % options->num_sub_dirs;

    std::string dir = std::filesystem::path(options->local_dirs_list[dir_id]) / std::format("{:02x}", sub_dir_id);
    if (!std::filesystem::exists(dir))
        std::filesystem::create_directories(dir);
    return std::filesystem::path(dir) / file_name;
}
void LocalPartitionWriter::stop()
{
    WriteBufferFromFile output(options->data_file, options->io_buffer_size);
    auto offsets = mergeSpills(output);
    shuffle_writer->split_result.partition_length = offsets;
}

PartitionWriter::PartitionWriter(CachedShuffleWriter * shuffle_writer_)
{
    shuffle_writer = shuffle_writer_;
    options = &shuffle_writer->options;
    for (size_t i = 0; i < options->partition_nums; ++i)
    {
        partition_block_buffer.emplace_back(ColumnsBuffer(options->split_size));
    }
    partition_buffer.resize(options->partition_nums);
}
CelebornPartitionWriter::CelebornPartitionWriter(CachedShuffleWriter * shuffleWriter, std::unique_ptr<CelebornClient> celeborn_client_)
    : PartitionWriter(shuffleWriter), celeborn_client(std::move(celeborn_client_))
{

}

void CelebornPartitionWriter::evictPartitions(bool for_memory_spill)
{
    auto spill_to_celeborn = [this]() -> void {
        Stopwatch serialization_time_watch;
        serialization_time_watch.start();
        for (size_t partition_id = 0; partition_id < partition_buffer.size(); ++partition_id)
        {
            const auto & partition = partition_buffer[partition_id];
            size_t raw_size = 0;
            if (partition.empty()) continue;
            WriteBufferFromOwnString output;
            auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
            CompressedWriteBuffer compressed_output(output, codec, shuffle_writer->options.io_buffer_size);
            NativeWriter writer(compressed_output, 0, shuffle_writer->output_header);
            for (const auto & block : partition)
            {
                raw_size += writer.write(block);
            }
            compressed_output.sync();
            Stopwatch push_time_watch;
            push_time_watch.start();
            celeborn_client->pushPartitionData(partition_id, output.str().data(), output.str().size());
            shuffle_writer->split_result.partition_length[partition_id] += output.str().size();
            shuffle_writer->split_result.raw_partition_length[partition_id] += raw_size;
            shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
            shuffle_writer->split_result.total_write_time += compressed_output.getWriteTime();
            shuffle_writer->split_result.total_write_time += push_time_watch.elapsedNanoseconds();
            shuffle_writer->split_result.total_disk_time += push_time_watch.elapsedNanoseconds();
        }
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
    };
    Stopwatch spill_time_watch;
    spill_time_watch.start();
    if (for_memory_spill)
    {
        // escape memory track from current thread status; add untracked memory limit for create thread object, avoid trigger memory spill again
        IgnoreMemoryTracker ignore(2 * 1024 * 1024);
        ThreadFromGlobalPool thread(spill_to_celeborn);
        thread.join();
    }
    else
    {
        IgnoreMemoryTracker ignore(2 * 1024 * 1024);
        spill_to_celeborn();
    }
    shuffle_writer->split_result.total_spill_time += spill_time_watch.elapsedNanoseconds();

    for (auto & partition : partition_buffer)
    {
        partition.clear();
    }
    shuffle_writer->split_result.total_bytes_spilled += total_partition_buffer_size;
    total_partition_buffer_size = 0;
}

void CelebornPartitionWriter::stop()
{
    for (size_t partition_id = 0; partition_id < shuffle_writer->options.partition_nums; ++partition_id)
    {
        if (partition_block_buffer[partition_id].size() > 0)
        {
            Block block = partition_block_buffer[partition_id].releaseColumns();
            partition_buffer[partition_id].emplace_back(std::move(block));
        }
    }
    evictPartitions(false);
    for (const auto & item : shuffle_writer->split_result.partition_length)
    {
        shuffle_writer->split_result.total_bytes_written += item;
    }
}
}

