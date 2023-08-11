#include "PartitionWriter.h"
#include <filesystem>
#include <memory>
#include <vector>
#include <Compression/CompressedWriteBuffer.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Shuffle/CachedShuffleWriter.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <format>

using namespace DB;

namespace local_engine
{
void local_engine::LocalPartitionWriter::write(const PartitionInfo& partition_info, DB::Block & data)
{
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
            total_partition_buffer_size += block.bytes();
            partition_buffer[i].emplace_back(block);
        }
    }
}

void LocalPartitionWriter::evictPartitions()
{
    auto file = getNextSpillFile();
    WriteBufferFromFile output(file);
    auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
    CompressedWriteBuffer compressed_output(output, codec);
    NativeWriter writer(compressed_output, 0, shuffle_writer->output_header);
    SpillInfo info;
    info.spilledFile = file;
    size_t start = 0;
    size_t partition_id = 0;
    for (const auto & partition : partition_buffer)
    {
        PartitionSpillInfo partition_spill_info;
        partition_spill_info.start = start;
        for (const auto & block : partition)
        {
            start += writer.write(block);
        }
        partition_spill_info.length = start - partition_spill_info.start;
        partition_spill_info.partition_id = partition_id;
        partition_id++;
        info.partitionSpillInfos.emplace_back(partition_spill_info);
    }
}
std::vector<Int64> LocalPartitionWriter::mergeSpills(WriteBuffer& data_file)
{
    auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(shuffle_writer->options.compress_method), {});
    CompressedWriteBuffer compressed_output(data_file, codec);
    NativeWriter writer(compressed_output, 0, shuffle_writer->output_header);

    std::vector<Int64> partition_length;
    partition_length.resize(shuffle_writer->options.partition_nums, 0);
    std::vector<ReadBufferPtr> spill_inputs;
    spill_inputs.reserve(spill_infos.size());
    for (const auto & spill : spill_infos)
    {
        spill_inputs.emplace_back(std::make_shared<ReadBufferFromFile>(spill.spilledFile));
    }

    String buffer;
    for (size_t partition_id = 0; partition_id < shuffle_writer->options.partition_nums; ++partition_id)
    {
        auto size_before = data_file.count();
        for (size_t i = 0; i < spill_infos.size(); ++i)
        {
            size_t size = spill_infos[i].partitionSpillInfos[partition_id].length;
            buffer.reserve(size);
            auto count = spill_inputs[i]->readBig(buffer.data(), size);
            data_file.write(buffer.data(), count);
        }

        if (partition_block_buffer[partition_id].size() > 0)
        {
            Block block = partition_block_buffer[partition_id].releaseColumns();
            partition_buffer[partition_id].emplace_back(block);
        }

        for (const auto & block : partition_buffer[partition_id])
        {
            writer.write(block);
        }
        compressed_output.sync();
//        data_file.sync();
        partition_length[partition_id] = data_file.count() - size_before;
        shuffle_writer->split_result.total_bytes_written += partition_length[partition_id];
    }

    for (const auto & spill : spill_infos)
    {
        std::filesystem::remove(spill.spilledFile);
    }
    return partition_length;
}
LocalPartitionWriter::LocalPartitionWriter(CachedShuffleWriter * shuffle_writer)
    : PartitionWriter(shuffle_writer)
{
    partition_buffer.resize(options->partition_nums);
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
}
}

