#include "CachedShuffleWriter.h"
#include <Poco/StringTokenizer.h>
#include <Common/Stopwatch.h>
#include <Shuffle/PartitionWriter.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}


namespace local_engine
{
using namespace DB;
CachedShuffleWriter::CachedShuffleWriter(const String & short_name, SplitOptions & options_)
{
    options = options_;
    if (short_name == "rr")
    {
        partitioner = std::make_unique<RoundRobinSelectorBuilder>(options.partition_nums);
    }
    else if (short_name == "hash")
    {
        Poco::StringTokenizer expr_list(options_.hash_exprs, ",");
        std::vector<size_t> hash_fields;
        for (const auto & expr : expr_list)
        {
            hash_fields.push_back(std::stoi(expr));
        }
        partitioner = std::make_unique<HashSelectorBuilder>(options.partition_nums, hash_fields, "cityHash64");
    }
    else if (short_name == "single")
    {
        options.partition_nums = 1;
        partitioner = std::make_unique<RoundRobinSelectorBuilder>(options.partition_nums);
    }
    else if (short_name == "range")
    {
        partitioner = std::make_unique<RangeSelectorBuilder>(options.hash_exprs, options.partition_nums);
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "unsupported splitter {}", short_name);
    }

    Poco::StringTokenizer output_column_tokenizer(options_.out_exprs, ",");
    for (const auto & iter : output_column_tokenizer)
    {
        output_columns_indicies.push_back(std::stoi(iter));
    }

    partition_writer = std::make_unique<LocalPartitionWriter>(this);
    split_result.partition_length.resize(options.partition_nums, 0);
    split_result.raw_partition_length.resize(options.partition_nums, 0);
}


void CachedShuffleWriter::split(DB::Block & block)
{
    initOutputIfNeeded(block);

    Stopwatch compute_pid_time_watch;
    compute_pid_time_watch.start();
    partition_info = partitioner->build(block);
    split_result.total_compute_pid_time += compute_pid_time_watch.elapsedNanoseconds();

    DB::Block out_block;
    for (size_t col = 0; col < output_header.columns(); ++col)
    {
        out_block.insert(block.getByPosition(output_columns_indicies[col]));
    }

    partition_writer->write(partition_info, out_block);

    if (options.spill_threshold > 0 && partition_writer->totalCacheSize() > options.spill_threshold)
    {
        std::cerr << "spill shuffle data: " << partition_writer->totalCacheSize() << std::endl;
        partition_writer->evictPartitions();
    }
}

void CachedShuffleWriter::initOutputIfNeeded(Block & block)
{
    if (output_header.columns() == 0) [[unlikely]]
    {
        output_header = block.cloneEmpty();
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
            ColumnsWithTypeAndName cols;
            for (const auto & index : output_columns_indicies)
            {
                cols.push_back(block.getByPosition(index));
            }
            output_header = DB::Block(cols);
        }
    }
}
SplitResult CachedShuffleWriter::stop()
{
    partition_writer->stop();
    return split_result;
}
size_t CachedShuffleWriter::evictPartitions()
{
    auto size = partition_writer->totalCacheSize();
    partition_writer->evictPartitions(true);
    return size;
}

}
