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
#include <format>
#include <memory>
#include <vector>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>
#include <Shuffle/CachedShuffleWriter.h>
#include <Storages/IO/AggregateSerializationUtils.h>
#include <Storages/IO/CompressedWriteBuffer.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/CHUtil.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/QueryContext.h>

#include <Processors/Transforms/SortingTransform.h>
#include <Storages/IO/NativeWriter.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

using namespace DB;
namespace local_engine
{
static const String PARTITION_COLUMN_NAME = "partition";

int64_t searchLastPartitionIdIndex(ColumnPtr column, size_t start, size_t partition_id)
{
    const auto & int64_column = checkAndGetColumn<ColumnUInt64>(*column);
    int64_t low = start, high = int64_column.size() - 1;
    while (low <= high)
    {
        int64_t mid = low + (high - low) / 2;
        if (int64_column.get64(mid) > partition_id)
            high = mid - 1;
        else
            low = mid + 1;
        if (int64_column.get64(high) == partition_id)
            return high;
    }
    return -1;
}

bool PartitionWriter::worthToSpill(size_t cache_size) const
{
    return (options->spill_threshold > 0 && cache_size >= options->spill_threshold) ||
        currentThreadGroupMemoryUsageRatio() > settings.spill_mem_ratio;
}

void PartitionWriter::write(const PartitionInfo & partition_info, DB::Block & block)
{
    /// PartitionWriter::write is alwasy the top frame who occupies evicting_or_writing
    Stopwatch watch;
    size_t current_cached_bytes = bytes();
    for (size_t partition_id = 0; partition_id < partition_info.partition_num; ++partition_id)
    {
        size_t from = partition_info.partition_start_points[partition_id];
        size_t length = partition_info.partition_start_points[partition_id + 1] - from;

        /// Make sure buffer size is no greater than split_size
        auto & block_buffer = partition_block_buffer[partition_id];
        auto & buffer = partition_buffer[partition_id];
        if (block_buffer->size() && block_buffer->size() + length >= shuffle_writer->options.split_size)
            buffer->addBlock(block_buffer->releaseColumns());

        current_cached_bytes -= block_buffer->bytes();
        for (size_t col_i = 0; col_i < block.columns(); ++col_i)
            block_buffer->appendSelective(col_i, block, partition_info.partition_selector, from, length);
        current_cached_bytes += block_buffer->bytes();

        /// Only works for celeborn partitiion writer
        if (supportsEvictSinglePartition() && worthToSpill(current_cached_bytes))
        {
            /// Calculate average rows of each partition block buffer
            size_t avg_size = 0;
            size_t cnt = 0;
            for (size_t i = (last_partition_id + 1) % options->partition_num; i != (partition_id + 1) % options->partition_num;
                 i = (i + 1) % options->partition_num)
            {
                avg_size += partition_block_buffer[i]->size();
                ++cnt;
            }
            avg_size /= cnt;


            for (size_t i = (last_partition_id + 1) % options->partition_num; i != (partition_id + 1) % options->partition_num;
                 i = (i + 1) % options->partition_num)
            {
                bool flush_block_buffer = partition_block_buffer[i]->size() >= avg_size;
                current_cached_bytes -= flush_block_buffer ? partition_block_buffer[i]->bytes() + partition_buffer[i]->bytes()
                                                           : partition_buffer[i]->bytes();
                evictSinglePartition(i);
            }
            // std::cout << "current cached bytes after evict partitions is " << current_cached_bytes << " partition from "
            //           << (last_partition_id + 1) % options->partition_num << " to " << partition_id << " average size:" << avg_size
            //           << std::endl;
            last_partition_id = partition_id;
        }
    }

    /// Only works for local partition writer
    if (!supportsEvictSinglePartition() && worthToSpill(current_cached_bytes))
        evictPartitions();

    shuffle_writer->split_result.total_split_time += watch.elapsedNanoseconds();
}

size_t LocalPartitionWriter::evictPartitions()
{
    size_t res = 0;
    size_t spilled_bytes = 0;

    auto spill_to_file = [this, &res, &spilled_bytes]()
    {
        auto file = getNextSpillFile();
        WriteBufferFromFile output(file, shuffle_writer->options.io_buffer_size);
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
        CompressedWriteBuffer compressed_output(output, codec, shuffle_writer->options.io_buffer_size);
        NativeWriter writer(compressed_output, shuffle_writer->output_header);

        SpillInfo info;
        info.spilled_file = file;

        Stopwatch serialization_time_watch;
        for (size_t partition_id = 0; partition_id < partition_buffer.size(); ++partition_id)
        {
            auto & buffer = partition_buffer[partition_id];

            auto & block_buffer = partition_block_buffer[partition_id];
            if (!block_buffer->empty())
                buffer->addBlock(block_buffer->releaseColumns());

            if (buffer->empty())
                continue;

            std::pair<size_t, size_t> offsets;
            offsets.first = output.count();
            spilled_bytes += buffer->bytes();

            size_t written_bytes = buffer->spill(writer);
            res += written_bytes;

            compressed_output.sync();
            offsets.second = output.count() - offsets.first;
            shuffle_writer->split_result.raw_partition_lengths[partition_id] += written_bytes;
            info.partition_spill_infos[partition_id] = offsets;
        }
        spill_infos.emplace_back(info);
        shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
        shuffle_writer->split_result.total_write_time += compressed_output.getWriteTime();
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
    };

    Stopwatch spill_time_watch;
    spill_to_file();
    shuffle_writer->split_result.total_spill_time += spill_time_watch.elapsedNanoseconds();
    shuffle_writer->split_result.total_bytes_spilled += spilled_bytes;
    LOG_INFO(logger, "spill shuffle data {} bytes, use spill time {} ms", spilled_bytes, spill_time_watch.elapsedMilliseconds());
    return res;
}

String Spillable::getNextSpillFile()
{
    auto file_name = std::to_string(static_cast<Int64>(split_options.shuffle_id)) + "_" + std::to_string(static_cast<Int64>(split_options.map_id)) + "_" + std::to_string(spill_infos.size());
    std::hash<std::string> hasher;
    auto hash = hasher(file_name);
    auto dir_id = hash % split_options.local_dirs_list.size();
    auto sub_dir_id = (hash / split_options.local_dirs_list.size()) % split_options.num_sub_dirs;

    std::string dir = std::filesystem::path(split_options.local_dirs_list[dir_id]) / std::format("{:02x}", sub_dir_id);
    if (!std::filesystem::exists(dir))
        std::filesystem::create_directories(dir);
    return std::filesystem::path(dir) / file_name;
}

std::vector<UInt64> Spillable::mergeSpills(CachedShuffleWriter * shuffle_writer, WriteBuffer & data_file, ExtraData extra_data)
{
    auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});

    CompressedWriteBuffer compressed_output(data_file, codec, shuffle_writer->options.io_buffer_size);
    NativeWriter writer(compressed_output, shuffle_writer->output_header);

    std::vector<UInt64> partition_length(shuffle_writer->options.partition_num, 0);

    std::vector<std::shared_ptr<ReadBufferFromFile>> spill_inputs;
    spill_inputs.reserve(spill_infos.size());
    for (const auto & spill : spill_infos)
    {
        // only use readBig
        spill_inputs.emplace_back(std::make_shared<ReadBufferFromFilePRead>(spill.spilled_file, 0));
    }

    Stopwatch write_time_watch;
    Stopwatch io_time_watch;
    Stopwatch serialization_time_watch;
    size_t merge_io_time = 0;
    String buffer;
    for (size_t partition_id = 0; partition_id < split_options.partition_num; ++partition_id)
    {
        auto size_before = data_file.count();

        io_time_watch.restart();
        for (size_t i = 0; i < spill_infos.size(); ++i)
        {
            if (!spill_infos[i].partition_spill_infos.contains(partition_id))
            {
                continue;
            }
            size_t size = spill_infos[i].partition_spill_infos[partition_id].second;
            size_t offset = spill_infos[i].partition_spill_infos[partition_id].first;
            if (!size)
            {
                continue;
            }
            buffer.reserve(size);
            auto count = spill_inputs[i]->readBigAt(buffer.data(), size, offset, nullptr);

            chassert(count == size);
            data_file.write(buffer.data(), count);
        }
        merge_io_time += io_time_watch.elapsedNanoseconds();

        serialization_time_watch.restart();
        if (!extra_data.partition_block_buffer.empty() && !extra_data.partition_block_buffer[partition_id]->empty())
        {
            Block block = extra_data.partition_block_buffer[partition_id]->releaseColumns();
            extra_data.partition_buffer[partition_id]->addBlock(std::move(block));
        }
        if (!extra_data.partition_buffer.empty())
        {
            size_t raw_size = extra_data.partition_buffer[partition_id]->spill(writer);
            shuffle_writer->split_result.raw_partition_lengths[partition_id] += raw_size;
        }
        compressed_output.sync();
        partition_length[partition_id] = data_file.count() - size_before;
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
        shuffle_writer->split_result.total_bytes_written += partition_length[partition_id];
    }

    shuffle_writer->split_result.total_write_time += write_time_watch.elapsedNanoseconds();
    shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
    shuffle_writer->split_result.total_io_time += compressed_output.getWriteTime();
    shuffle_writer->split_result.total_serialize_time = shuffle_writer->split_result.total_serialize_time
        - shuffle_writer->split_result.total_io_time - shuffle_writer->split_result.total_compress_time;
    shuffle_writer->split_result.total_io_time += merge_io_time;

    for (const auto & spill : spill_infos)
        std::filesystem::remove(spill.spilled_file);
    return partition_length;
}

void SortBasedPartitionWriter::write(const PartitionInfo & info, DB::Block & block)
{
    Stopwatch write_time_watch;
    if (output_header.columns() == 0)
        output_header = block.cloneEmpty();
    auto partition_column = ColumnUInt64::create();
    partition_column->reserve(block.rows());
    partition_column->getData().insert_assume_reserved(info.src_partition_num.begin(), info.src_partition_num.end());
    block.insert({std::move(partition_column), std::make_shared<DataTypeUInt64>(), PARTITION_COLUMN_NAME});
    if (sort_header.columns() == 0)
    {
        sort_header = block.cloneEmpty();
        sort_description.emplace_back(SortColumnDescription(PARTITION_COLUMN_NAME));
    }
    // partial sort
    sortBlock(block, sort_description);
    Chunk chunk;
    chunk.setColumns(block.getColumns(), block.rows());
    accumulated_blocks.emplace_back(std::move(chunk));
    current_accumulated_bytes += accumulated_blocks.back().allocatedBytes();
    current_accumulated_rows += accumulated_blocks.back().getNumRows();
    shuffle_writer->split_result.total_write_time += write_time_watch.elapsedNanoseconds();
    if (worthToSpill(current_accumulated_bytes))
        evictPartitions();
}

LocalPartitionWriter::LocalPartitionWriter(CachedShuffleWriter * shuffle_writer_) : PartitionWriter(shuffle_writer_, getLogger("LocalPartitionWriter")), Spillable(shuffle_writer_->options)
{
}

void LocalPartitionWriter::stop()
{
    WriteBufferFromFile output(options->data_file, options->io_buffer_size);
    auto offsets = mergeSpills(shuffle_writer, output, {partition_block_buffer, partition_buffer});
    shuffle_writer->split_result.partition_lengths = offsets;
}

PartitionWriter::PartitionWriter(CachedShuffleWriter * shuffle_writer_, LoggerPtr logger_)
    : shuffle_writer(shuffle_writer_)
    , options(&shuffle_writer->options)
    , partition_block_buffer(options->partition_num)
    , partition_buffer(options->partition_num)
    , last_partition_id(options->partition_num - 1)
    , logger(logger_)
{
    for (size_t partition_id = 0; partition_id < options->partition_num; ++partition_id)
    {
        partition_block_buffer[partition_id] = std::make_shared<ColumnsBuffer>(options->split_size);
        partition_buffer[partition_id] = std::make_shared<Partition>();
    }
    settings.loadFromContext(SerializedPlanParser::global_context);
}

size_t PartitionWriter::bytes() const
{
    size_t bytes = 0;

    for (const auto & buffer : partition_block_buffer)
        bytes += buffer->bytes();

    for (const auto & buffer : partition_buffer)
        bytes += buffer->bytes();

    return bytes;
}


size_t MemorySortLocalPartitionWriter::evictPartitions()
{
    size_t res = 0;
    size_t spilled_bytes = 0;

    auto spill_to_file = [this, &res, &spilled_bytes]()
    {
        if (accumulated_blocks.empty())
            return;
        auto file = getNextSpillFile();
        WriteBufferFromFile output(file, shuffle_writer->options.io_buffer_size);
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
        CompressedWriteBuffer compressed_output(output, codec, shuffle_writer->options.io_buffer_size);
        NativeWriter writer(compressed_output, output_header);

        SpillInfo info;
        info.spilled_file = file;

        Stopwatch serialization_time_watch;
        MergeSorter sorter(sort_header, std::move(accumulated_blocks), sort_description, adaptiveBlockSize(), 0);
        size_t cur_partition_id = 0;
        info.partition_spill_infos[cur_partition_id] = {0,0};
        while (auto data = sorter.read())
        {
            Block serialized_block = sort_header.cloneWithColumns(data.detachColumns());
            const auto partitions = serialized_block.getByName(PARTITION_COLUMN_NAME).column;
            serialized_block.erase(PARTITION_COLUMN_NAME);
            size_t row_offset = 0;
            while (row_offset < serialized_block.rows())
            {
                auto last_idx = searchLastPartitionIdIndex(partitions, row_offset, cur_partition_id);
                if (last_idx < 0)
                {
                    auto& last = info.partition_spill_infos[cur_partition_id];
                    compressed_output.sync();
                    last.second = output.count() - last.first;
                    cur_partition_id++;
                    info.partition_spill_infos[cur_partition_id] = {last.first + last.second, 0};
                    continue;
                }

                if (row_offset == 0 && last_idx == serialized_block.rows() - 1)
                {
                    auto count = writer.write(serialized_block);
                    shuffle_writer->split_result.raw_partition_lengths[cur_partition_id] += count;
                    break;
                }
                else
                {
                    auto cut_block = serialized_block.cloneWithCutColumns(row_offset, last_idx - row_offset + 1);

                    auto count = writer.write(cut_block);
                    shuffle_writer->split_result.raw_partition_lengths[cur_partition_id] += count;
                    row_offset = last_idx + 1;
                    if (last_idx != serialized_block.rows() - 1)
                    {
                        auto& last = info.partition_spill_infos[cur_partition_id];
                        compressed_output.sync();
                        last.second = output.count() - last.first;
                        cur_partition_id++;
                        info.partition_spill_infos[cur_partition_id] = {last.first + last.second, 0};
                    }
                }
            }
        }
        compressed_output.sync();
        auto& last = info.partition_spill_infos[cur_partition_id];
        last.second = output.count() - last.first;
        spilled_bytes = current_accumulated_bytes;
        res = current_accumulated_bytes;
        current_accumulated_bytes = 0;
        current_accumulated_rows = 0;
        std::erase_if(info.partition_spill_infos, [](const auto & item)
        {
            auto const& [key, value] = item;
           return value.second == 0;
        });
        spill_infos.emplace_back(info);
        shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
        shuffle_writer->split_result.total_io_time += compressed_output.getWriteTime();
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
    };

    Stopwatch spill_time_watch;
    spill_to_file();
    shuffle_writer->split_result.total_spill_time += spill_time_watch.elapsedNanoseconds();
    shuffle_writer->split_result.total_bytes_spilled += spilled_bytes;
    LOG_INFO(logger, "spill shuffle data {} bytes, use spill time {} ms", spilled_bytes, spill_time_watch.elapsedMilliseconds());
    return res;
}

void MemorySortLocalPartitionWriter::stop()
{
    evictPartitions();
    WriteBufferFromFile output(options->data_file, options->io_buffer_size);
    auto offsets = mergeSpills(shuffle_writer, output);
    shuffle_writer->split_result.partition_lengths = offsets;
}

size_t MemorySortCelebornPartitionWriter::evictPartitions()
{
    size_t res = 0;
    size_t spilled_bytes = 0;
    auto spill_to_celeborn = [this, &res, &spilled_bytes]()
    {
        Stopwatch serialization_time_watch;

        /// Skip empty buffer
        if (accumulated_blocks.empty())
            return;

        WriteBufferFromOwnString output;
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
        CompressedWriteBuffer compressed_output(output, codec, shuffle_writer->options.io_buffer_size);
        NativeWriter writer(compressed_output, shuffle_writer->output_header);

        MergeSorter sorter(sort_header, std::move(accumulated_blocks), sort_description, adaptiveBlockSize(), 0);
        size_t cur_partition_id = 0;
        auto push_to_celeborn = [&]()
        {
            compressed_output.sync();
            auto& data = output.str();
            if (!data.empty())
            {
                Stopwatch push_time_watch;
                celeborn_client->pushPartitionData(cur_partition_id, data.data(), data.size());
                shuffle_writer->split_result.total_io_time += push_time_watch.elapsedNanoseconds();
                shuffle_writer->split_result.partition_lengths[cur_partition_id] += data.size();
            }
            output.restart();
        };

        while (auto data = sorter.read())
        {
            Block serialized_block = sort_header.cloneWithColumns(data.detachColumns());
            const auto partitions = serialized_block.getByName(PARTITION_COLUMN_NAME).column;
            serialized_block.erase(PARTITION_COLUMN_NAME);
            size_t row_offset = 0;
            while (row_offset < serialized_block.rows())
            {
                auto last_idx = searchLastPartitionIdIndex(partitions, row_offset, cur_partition_id);
                if (last_idx < 0)
                {
                    push_to_celeborn();
                    cur_partition_id++;
                    continue;
                }

                if (row_offset == 0 && last_idx == serialized_block.rows() - 1)
                {
                    auto count = writer.write(serialized_block);
                    shuffle_writer->split_result.raw_partition_lengths[cur_partition_id] += count;
                    break;
                }
                auto cut_block = serialized_block.cloneWithCutColumns(row_offset, last_idx - row_offset + 1);
                auto count = writer.write(cut_block);
                shuffle_writer->split_result.raw_partition_lengths[cur_partition_id] += count;
                row_offset = last_idx + 1;
                if (last_idx != serialized_block.rows() - 1)
                {
                    push_to_celeborn();
                    cur_partition_id++;
                }
            }
        }
        push_to_celeborn();
        spilled_bytes = current_accumulated_bytes;
        res = current_accumulated_bytes;
        current_accumulated_bytes = 0;
        current_accumulated_rows = 0;

        shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
        shuffle_writer->split_result.total_io_time += compressed_output.getWriteTime();
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
    };

    Stopwatch spill_time_watch;
    spill_to_celeborn();
    shuffle_writer->split_result.total_spill_time += spill_time_watch.elapsedNanoseconds();
    shuffle_writer->split_result.total_bytes_spilled += spilled_bytes;
    LOG_INFO(logger, "spill shuffle data {} bytes, use spill time {} ms", spilled_bytes, spill_time_watch.elapsedMilliseconds());
    return res;
}

void MemorySortCelebornPartitionWriter::stop()
{
    evictPartitions();
}

CelebornPartitionWriter::CelebornPartitionWriter(CachedShuffleWriter * shuffleWriter, std::unique_ptr<CelebornClient> celeborn_client_)
    : PartitionWriter(shuffleWriter, getLogger("CelebornPartitionWriter")), celeborn_client(std::move(celeborn_client_))
{
}

size_t CelebornPartitionWriter::evictPartitions()
{
    size_t res = 0;
    for (size_t partition_id = 0; partition_id < options->partition_num; ++partition_id)
        res += evictSinglePartition(partition_id);
    return res;
}

size_t CelebornPartitionWriter::evictSinglePartition(size_t partition_id)
{
    size_t res = 0;
    size_t spilled_bytes = 0;
    auto spill_to_celeborn = [this,partition_id, &res, &spilled_bytes]()
    {
        Stopwatch serialization_time_watch;
        auto & buffer = partition_buffer[partition_id];

        auto & block_buffer = partition_block_buffer[partition_id];
        if (!block_buffer->empty())
        {
            // std::cout << "flush block buffer for partition:" << partition_id << " rows:" << block_buffer->size() << std::endl;
            buffer->addBlock(block_buffer->releaseColumns());
        }

        /// Skip empty buffer
        if (buffer->empty())
            return;

        WriteBufferFromOwnString output;
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
        CompressedWriteBuffer compressed_output(output, codec, shuffle_writer->options.io_buffer_size);
        NativeWriter writer(compressed_output, shuffle_writer->output_header);

        spilled_bytes += buffer->bytes();
        size_t written_bytes = buffer->spill(writer);
        res += written_bytes;
        compressed_output.sync();

        // std::cout << "evict partition " << partition_id << " uncompress_bytes:" << compressed_output.getUncompressedBytes()
        //           << " compress_bytes:" << compressed_output.getCompressedBytes() << std::endl;

        Stopwatch push_time_watch;
        celeborn_client->pushPartitionData(partition_id, output.str().data(), output.str().size());

        shuffle_writer->split_result.partition_lengths[partition_id] += output.str().size();
        shuffle_writer->split_result.raw_partition_lengths[partition_id] += written_bytes;
        shuffle_writer->split_result.total_compress_time += compressed_output.getCompressTime();
        shuffle_writer->split_result.total_write_time += compressed_output.getWriteTime();
        shuffle_writer->split_result.total_write_time += push_time_watch.elapsedNanoseconds();
        shuffle_writer->split_result.total_io_time += push_time_watch.elapsedNanoseconds();
        shuffle_writer->split_result.total_serialize_time += serialization_time_watch.elapsedNanoseconds();
    };

    Stopwatch spill_time_watch;
    spill_to_celeborn();
    shuffle_writer->split_result.total_spill_time += spill_time_watch.elapsedNanoseconds();
    shuffle_writer->split_result.total_bytes_spilled += spilled_bytes;
    LOG_INFO(logger, "spill shuffle data {} bytes, use spill time {} ms", spilled_bytes, spill_time_watch.elapsedMilliseconds());
    return res;
}

void CelebornPartitionWriter::stop()
{
    evictPartitions();
    for (const auto & length : shuffle_writer->split_result.partition_lengths)
        shuffle_writer->split_result.total_bytes_written += length;
}

void Partition::addBlock(DB::Block block)
{
    /// Do not insert empty blocks, otherwise will cause the shuffle read terminate early.
    if (!block.rows())
        return;

    cached_bytes += block.bytes();
    blocks.emplace_back(std::move(block));
}

size_t Partition::spill(NativeWriter & writer)
{
    size_t written_bytes = 0;
    for (auto & block : blocks)
    {
        written_bytes += writer.write(block);

        /// Clear each block once it is serialized to reduce peak memory
        DB::Block().swap(block);
    }

    blocks.clear();
    cached_bytes = 0;
    return written_bytes;
}



}
