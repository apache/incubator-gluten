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
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>
#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Shuffle/ShuffleSplitter.h>
#include <jni/CelebornClient.h>
#include <Parser/SerializedPlanParser.h>

namespace DB
{
class MergingSortedAlgorithm;
}

namespace local_engine
{
struct PartitionSpillInfo
{
    size_t partition_id;
    size_t start;
    size_t length; // in Bytes
};

struct SpillInfo
{
    std::string spilled_file;
    std::vector<PartitionSpillInfo> partition_spill_infos;
};

class Partition
{
public:
    Partition() = default;
    ~Partition() = default;

    Partition(Partition && other) noexcept : blocks(std::move(other.blocks)) { }

    bool empty() const { return blocks.empty(); }
    void addBlock(DB::Block block);
    size_t spill(NativeWriter & writer);
    size_t bytes() const { return cached_bytes; }

private:
    std::vector<DB::Block> blocks;
    size_t cached_bytes = 0;
};

struct PartitionWriterSettings
{
    uint64_t spill_memory_overhead = 0;

    void loadFromContext(DB::ContextPtr context);
};

class CachedShuffleWriter;
using PartitionPtr = std::shared_ptr<Partition>;
class PartitionWriter : boost::noncopyable
{
public:
    explicit PartitionWriter(CachedShuffleWriter * shuffle_writer_);
    virtual ~PartitionWriter() = default;

    virtual String getName() const = 0;

    virtual void write(const PartitionInfo & info, DB::Block & block);
    size_t evictPartitions(bool for_memory_spill = false, bool flush_block_buffer = false);
    void stop();

protected:
    size_t bytes() const;

    virtual size_t unsafeEvictPartitions(bool for_memory_spill, bool flush_block_buffer = false) = 0;

    virtual bool supportsEvictSinglePartition() const { return false; }

    virtual size_t unsafeEvictSinglePartition(bool for_memory_spill, bool flush_block_buffer, size_t partition_id)
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Evict single partition is not supported for {}", getName());
    }

    virtual void unsafeStop() = 0;

    CachedShuffleWriter * shuffle_writer;
    const SplitOptions * options;
    PartitionWriterSettings settings;

    std::vector<ColumnsBufferPtr> partition_block_buffer;
    std::vector<PartitionPtr> partition_buffer;

    /// Make sure memory spill doesn't happen while write/stop are executed.
    bool evicting_or_writing{false};

    /// Only valid in celeborn partition writer
    size_t last_partition_id;
};

class LocalPartitionWriter : public PartitionWriter
{
public:
    explicit LocalPartitionWriter(CachedShuffleWriter * shuffle_writer);
    ~LocalPartitionWriter() override = default;

    String getName() const override { return "LocalPartitionWriter"; }

protected:
    size_t unsafeEvictPartitions(bool for_memory_spill, bool flush_block_buffer) override;
    void unsafeStop() override;

    String getNextSpillFile();
    std::vector<UInt64> mergeSpills(DB::WriteBuffer & data_file);

    std::vector<SpillInfo> spill_infos;
};

class SortedPartitionDataMerger;

class ExternalSortLocalPartitionWriter : public PartitionWriter
{
public:
    struct MergeContext
    {
        CompressionCodecPtr codec;
        std::unique_ptr<SortedPartitionDataMerger> merger;
    };

    explicit ExternalSortLocalPartitionWriter(CachedShuffleWriter * shuffle_writer_) : PartitionWriter(shuffle_writer_)
    {
        max_merge_block_size = options->split_size;
        max_sort_buffer_size = options->max_sort_buffer_size;
        tmp_data = std::make_unique<TemporaryDataOnDisk>(SerializedPlanParser::global_context->getTempDataOnDisk());
    }

    ~ExternalSortLocalPartitionWriter() override = default;

    String getName() const override { return "ExternalSortLocalPartitionWriter"; }
    void write(const PartitionInfo & info, DB::Block & block) override;

protected:
    size_t unsafeEvictPartitions(bool for_memory_spill, bool flush_block_buffer) override;
    /// Prepare for data merging, spill the remaining memory data，and create a merger object.
    MergeContext prepareMerge();
    void unsafeStop() override;
    std::queue<DB::Block> mergeDataInMemory();

    size_t max_sort_buffer_size = 1_GiB;
    size_t max_merge_block_size = DB::DEFAULT_BLOCK_SIZE;
    size_t current_accumulated_bytes = 0;
    DB::Chunks accumulated_blocks;
    DB::Block output_header;
    DB::Block sort_header;
    DB::SortDescription sort_description;
    DB::TemporaryDataOnDiskPtr tmp_data;
    std::vector<DB::TemporaryFileStream *> streams;
};

class  ExternalSortCelebornPartitionWriter : public ExternalSortLocalPartitionWriter
{
public:
    explicit ExternalSortCelebornPartitionWriter(CachedShuffleWriter * shuffle_writer_, std::unique_ptr<CelebornClient> celeborn_client_)
        : ExternalSortLocalPartitionWriter(shuffle_writer_), celeborn_client(std::move(celeborn_client_))
    {
    }
protected:
    void unsafeStop() override;

private:
    std::unique_ptr<CelebornClient> celeborn_client;
};

class CelebornPartitionWriter : public PartitionWriter
{
public:
    CelebornPartitionWriter(CachedShuffleWriter * shuffleWriter, std::unique_ptr<CelebornClient> celeborn_client);
    ~CelebornPartitionWriter() override = default;

    String getName() const override { return "CelebornPartitionWriter"; }

protected:
    size_t unsafeEvictPartitions(bool for_memory_spill, bool flush_block_buffer) override;

    bool supportsEvictSinglePartition() const override { return true; }
    size_t unsafeEvictSinglePartition(bool for_memory_spill, bool flush_block_buffer, size_t partition_id) override;

    void unsafeStop() override;

    std::unique_ptr<CelebornClient> celeborn_client;
};
}
