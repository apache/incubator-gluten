#pragma once
#include <algorithm>
#include <memory>
#include <Shuffle/ShuffleSplitter.h>
#include <Shuffle/SelectorBuilder.h>
#include <Shuffle/ShuffleWriterBase.h>

namespace local_engine
{

class PartitionWriter;
class LocalPartitionWriter;

class CachedShuffleWriter : public ShuffleWriterBase
{
public:
    friend class PartitionWriter;
    friend class LocalPartitionWriter;
    explicit CachedShuffleWriter(const String & short_name, SplitOptions & options);
    ~CachedShuffleWriter() override = default;
    void split(DB::Block & block) override;
    size_t evictPartitions() override;
    SplitResult stop() override;

private:
    void initOutputIfNeeded(DB::Block & block);

    bool stopped = false;
    PartitionInfo partition_info;
    DB::Block output_header;
    SplitOptions options;
    SplitResult split_result;
    std::unique_ptr<SelectorBuilder> partitioner;
    std::vector<size_t> output_columns_indicies;
    std::unique_ptr<PartitionWriter> partition_writer;
};
}



