#pragma once
#include <cstddef>
#include <memory>
#include <vector>
#include <IO/WriteBuffer.h>
#include <Core/Block.h>
#include <Shuffle/ShuffleSplitter.h>

namespace local_engine
{
struct PartitionSpillInfo {
    size_t partition_id;
    size_t start;
    size_t length; // in Bytes
};

struct SpillInfo {
    std::string spilled_file;
    std::vector<PartitionSpillInfo> partition_spill_infos;
};

class CachedShuffleWriter;

class PartitionWriter {
public:
    explicit PartitionWriter(CachedShuffleWriter* shuffle_writer_);
    virtual ~PartitionWriter() = default;

    virtual void write(const PartitionInfo& info, DB::Block & data) = 0;

    virtual void evictPartitions(bool for_memory_spill = false) = 0;

    virtual void stop() = 0;

    virtual size_t totalCacheSize() = 0;

protected:
    std::vector<ColumnsBuffer> partition_block_buffer;
    SplitOptions * options;
    CachedShuffleWriter * shuffle_writer;
};

class LocalPartitionWriter : public PartitionWriter
{
public:
    explicit LocalPartitionWriter(CachedShuffleWriter * shuffle_writer);
    void write(const PartitionInfo& info, DB::Block & data) override;
    void evictPartitions(bool for_memory_spill) override;
    void stop() override;
    std::vector<Int64> mergeSpills(DB::WriteBuffer& data_file);
    size_t totalCacheSize() override;

private:
    String getNextSpillFile();
    std::vector<SpillInfo> spill_infos;
    std::vector<std::vector<DB::Block>> partition_buffer;
    size_t total_partition_buffer_size = 0;
};
}



