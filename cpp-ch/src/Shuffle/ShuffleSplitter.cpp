#include "ShuffleSplitter.h"
#include <filesystem>
#include <format>
#include <memory>
#include <string>
#include <fcntl.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Functions/FunctionFactory.h>
#include <IO/BrotliWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parser/SerializedPlanParser.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/DebugUtils.h>
#include <Poco/StringTokenizer.h>

namespace local_engine
{
void ShuffleSplitter::split(DB::Block & block)
{
    if (block.rows() == 0)
    {
        return;
    }
    Stopwatch watch;
    watch.start();
    computeAndCountPartitionId(block);
    splitBlockByPartition(block);
    split_result.total_write_time += watch.elapsedNanoseconds();
}
SplitResult ShuffleSplitter::stop()
{
    // spill all buffers
    Stopwatch watch;
    watch.start();
    for (size_t i = 0; i < options.partition_nums; i++)
    {
        spillPartition(i);
        partition_outputs[i]->flush();
        partition_write_buffers[i].reset();
    }
    partition_outputs.clear();
    partition_cached_write_buffers.clear();
    partition_write_buffers.clear();
    mergePartitionFiles();
    split_result.total_write_time += watch.elapsedNanoseconds();
    stopped = true;
    return split_result;
}
void ShuffleSplitter::splitBlockByPartition(DB::Block & block)
{
    if (!output_header.columns()) [[unlikely]]
    {
        if (output_columns_indicies.empty())
        {
            output_header = block.cloneEmpty();
            for (size_t i = 0; i < block.columns(); ++i)
            {
                output_columns_indicies.push_back(i);
            }
        }
        else
        {
            DB::ColumnsWithTypeAndName cols;
            for (const auto & index : output_columns_indicies)
            {
                cols.push_back(block.getByPosition(index));
            }
            output_header = DB::Block(cols);
        }
    }
    DB::Block out_block;
    for (size_t col = 0; col < output_header.columns(); ++col)
    {
        out_block.insert(block.getByPosition(output_columns_indicies[col]));
    }
    for (size_t col = 0; col < output_header.columns(); ++col)
    {
        for (size_t j = 0; j < partition_info.partition_num; ++j)
        {
            size_t from = partition_info.partition_start_points[j];
            size_t length = partition_info.partition_start_points[j + 1] - from;
            if (length == 0)
                continue; // no data for this partition continue;
            partition_buffer[j].appendSelective(col, out_block, partition_info.partition_selector, from, length);
        }
    }

    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        ColumnsBuffer & buffer = partition_buffer[i];
        if (buffer.size() >= options.split_size)
        {
            spillPartition(i);
        }
    }

}
void ShuffleSplitter::init()
{
    partition_buffer.reserve(options.partition_nums);
    partition_outputs.reserve(options.partition_nums);
    partition_write_buffers.reserve(options.partition_nums);
    partition_cached_write_buffers.reserve(options.partition_nums);
    split_result.partition_length.reserve(options.partition_nums);
    split_result.raw_partition_length.reserve(options.partition_nums);
    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        partition_buffer.emplace_back(ColumnsBuffer());
        split_result.partition_length.emplace_back(0);
        split_result.raw_partition_length.emplace_back(0);
        partition_outputs.emplace_back(nullptr);
        partition_write_buffers.emplace_back(nullptr);
        partition_cached_write_buffers.emplace_back(nullptr);
    }
}

void ShuffleSplitter::spillPartition(size_t partition_id)
{
    Stopwatch watch;
    watch.start();
    if (!partition_outputs[partition_id])
    {
        partition_write_buffers[partition_id] = getPartitionWriteBuffer(partition_id);
        partition_outputs[partition_id]
            = std::make_unique<DB::NativeWriter>(*partition_write_buffers[partition_id], 0, partition_buffer[partition_id].getHeader());
    }
    DB::Block result = partition_buffer[partition_id].releaseColumns();
    if (result.rows() > 0)
    {
        partition_outputs[partition_id]->write(result);
    }
    split_result.total_spill_time += watch.elapsedNanoseconds();
    split_result.total_bytes_spilled += result.bytes();
}

void ShuffleSplitter::mergePartitionFiles()
{
    DB::WriteBufferFromFile data_write_buffer = DB::WriteBufferFromFile(options.data_file);
    std::string buffer;
    int buffer_size = options.io_buffer_size;
    buffer.reserve(buffer_size);
    for (size_t i = 0; i < options.partition_nums; ++i)
    {
        auto file = getPartitionTempFile(i);
        DB::ReadBufferFromFile reader = DB::ReadBufferFromFile(file, options.io_buffer_size);
        while (reader.next())
        {
            auto bytes = reader.readBig(buffer.data(), buffer_size);
            data_write_buffer.write(buffer.data(), bytes);
            split_result.partition_length[i] += bytes;
            split_result.total_bytes_written += bytes;
        }
        reader.close();
        std::filesystem::remove(file);
    }
    data_write_buffer.close();
}

ShuffleSplitter::ShuffleSplitter(SplitOptions && options_) : options(options_)
{
    init();
}

ShuffleSplitter::Ptr ShuffleSplitter::create(const std::string & short_name, SplitOptions options_)
{
    if (short_name == "rr")
    {
        return RoundRobinSplitter::create(std::move(options_));
    }
    else if (short_name == "hash")
    {
        return HashSplitter::create(std::move(options_));
    }
    else if (short_name == "single")
    {
        options_.partition_nums = 1;
        return RoundRobinSplitter::create(std::move(options_));
    }
    else if (short_name == "range")
    {
        return RangeSplitter::create(std::move(options_));
    }
    else
    {
        throw std::runtime_error("unsupported splitter " + short_name);
    }
}

std::string ShuffleSplitter::getPartitionTempFile(size_t partition_id)
{
    auto file_name = std::to_string(options.shuffle_id) + "_" + std::to_string(options.map_id) + "_" + std::to_string(partition_id);
    std::hash<std::string> hasher;
    auto hash = hasher(file_name);
    auto dir_id = hash % options.local_dirs_list.size();
    auto sub_dir_id = (hash / options.local_dirs_list.size()) % options.num_sub_dirs;

    std::string dir = std::filesystem::path(options.local_dirs_list[dir_id]) / std::format("{:02x}", sub_dir_id);
    if (!std::filesystem::exists(dir))
        std::filesystem::create_directories(dir);
    return std::filesystem::path(dir) / file_name;
}

std::unique_ptr<DB::WriteBuffer> ShuffleSplitter::getPartitionWriteBuffer(size_t partition_id)
{
    auto file = getPartitionTempFile(partition_id);
    if (partition_cached_write_buffers[partition_id] == nullptr)
        partition_cached_write_buffers[partition_id]
            = std::make_unique<DB::WriteBufferFromFile>(file, options.io_buffer_size, O_CREAT | O_WRONLY | O_APPEND);
    if (!options.compress_method.empty()
        && std::find(compress_methods.begin(), compress_methods.end(), options.compress_method) != compress_methods.end())
    {
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(options.compress_method), {});
        return std::make_unique<DB::CompressedWriteBuffer>(*partition_cached_write_buffers[partition_id], codec);
    }
    else
    {
        return std::move(partition_cached_write_buffers[partition_id]);
    }
}

const std::vector<std::string> ShuffleSplitter::compress_methods = {"", "ZSTD", "LZ4"};

void ShuffleSplitter::writeIndexFile()
{
    auto index_file = options.data_file + ".index";
    auto writer = std::make_unique<DB::WriteBufferFromFile>(index_file, options.io_buffer_size, O_CREAT | O_WRONLY | O_TRUNC);
    for (auto len : split_result.partition_length)
    {
        DB::writeIntText(len, *writer);
        DB::writeChar('\n', *writer);
    }
}

void ColumnsBuffer::add(DB::Block & block, int start, int end)
{
    if (header.columns() == 0)
        header = block.cloneEmpty();
    if (accumulated_columns.empty()) [[unlikely]]
    {
        accumulated_columns.reserve(block.columns());
        for (size_t i = 0; i < block.columns(); i++)
        {
            auto column = block.getColumns()[i]->cloneEmpty();
            column->reserve(prefer_buffer_size);
            accumulated_columns.emplace_back(std::move(column));
        }
    }
    assert(!accumulated_columns.empty());
    for (size_t i = 0; i < block.columns(); ++i)
        accumulated_columns[i]->insertRangeFrom(*block.getByPosition(i).column, start, end - start);
}

void ColumnsBuffer::appendSelective(size_t column_idx, const DB::Block & source, const DB::IColumn::Selector & selector, size_t from, size_t length)
{
    if (header.columns() == 0)
        header = source.cloneEmpty();
    if (accumulated_columns.empty()) [[unlikely]]
    {
        accumulated_columns.reserve(source.columns());
        for (size_t i = 0; i < source.columns(); i++)
        {
            auto column = source.getColumns()[i]->convertToFullColumnIfConst()->cloneEmpty();
            column->reserve(prefer_buffer_size);
            accumulated_columns.emplace_back(std::move(column));
        }
    }
    accumulated_columns[column_idx]->insertRangeSelective(*source.getByPosition(column_idx).column->convertToFullColumnIfConst(), selector, from, length);
}

size_t ColumnsBuffer::size() const
{
    if (accumulated_columns.empty())
        return 0;
    return accumulated_columns.at(0)->size();
}

DB::Block ColumnsBuffer::releaseColumns()
{
    DB::Columns res(std::make_move_iterator(accumulated_columns.begin()), std::make_move_iterator(accumulated_columns.end()));
    accumulated_columns.clear();
    if (res.empty())
    {
        return header.cloneEmpty();
    }
    else
    {
        return header.cloneWithColumns(res);
    }
}

DB::Block ColumnsBuffer::getHeader()
{
    return header;
}
ColumnsBuffer::ColumnsBuffer(size_t prefer_buffer_size_) : prefer_buffer_size(prefer_buffer_size_)
{
}

RoundRobinSplitter::RoundRobinSplitter(SplitOptions options_) : ShuffleSplitter(std::move(options_))
{
    Poco::StringTokenizer output_column_tokenizer(options_.out_exprs, ",");
    for (auto iter = output_column_tokenizer.begin(); iter != output_column_tokenizer.end(); ++iter)
    {
        output_columns_indicies.push_back(std::stoi(*iter));
    }
    selector_builder = std::make_unique<RoundRobinSelectorBuilder>(options.partition_nums);
}

void RoundRobinSplitter::computeAndCountPartitionId(DB::Block & block)
{
    Stopwatch watch;
    watch.start();
    partition_info = selector_builder->build(block);
    split_result.total_compute_pid_time += watch.elapsedNanoseconds();
}

std::unique_ptr<ShuffleSplitter> RoundRobinSplitter::create(SplitOptions && options_)
{
    return std::make_unique<RoundRobinSplitter>(std::move(options_));
}

HashSplitter::HashSplitter(SplitOptions options_) : ShuffleSplitter(std::move(options_))
{
    Poco::StringTokenizer exprs_list(options_.hash_exprs, ",");
    std::vector<size_t> hash_fields;
    for (auto iter = exprs_list.begin(); iter != exprs_list.end(); ++iter)
    {
        hash_fields.push_back(std::stoi(*iter));
    }

    Poco::StringTokenizer output_column_tokenizer(options_.out_exprs, ",");
    for (auto iter = output_column_tokenizer.begin(); iter != output_column_tokenizer.end(); ++iter)
    {
        output_columns_indicies.push_back(std::stoi(*iter));
    }

    selector_builder = std::make_unique<HashSelectorBuilder>(options.partition_nums, hash_fields, "cityHash64");
}
std::unique_ptr<ShuffleSplitter> HashSplitter::create(SplitOptions && options_)
{
    return std::make_unique<HashSplitter>(std::move(options_));
}

void HashSplitter::computeAndCountPartitionId(DB::Block & block)
{
    Stopwatch watch;
    watch.start();
    partition_info = selector_builder->build(block);
    split_result.total_compute_pid_time += watch.elapsedNanoseconds();
}

std::unique_ptr<ShuffleSplitter> RangeSplitter::create(SplitOptions && options_)
{
    return std::make_unique<RangeSplitter>(std::move(options_));
}

RangeSplitter::RangeSplitter(SplitOptions options_) : ShuffleSplitter(std::move(options_))
{
    Poco::StringTokenizer output_column_tokenizer(options_.out_exprs, ",");
    for (auto iter = output_column_tokenizer.begin(); iter != output_column_tokenizer.end(); ++iter)
    {
        output_columns_indicies.push_back(std::stoi(*iter));
    }
    selector_builder = std::make_unique<RangeSelectorBuilder>(options.hash_exprs, options.partition_nums);
}
void RangeSplitter::computeAndCountPartitionId(DB::Block & block)
{
    Stopwatch watch;
    watch.start();
    partition_info = selector_builder->build(block);
    split_result.total_compute_pid_time += watch.elapsedNanoseconds();
}
}
