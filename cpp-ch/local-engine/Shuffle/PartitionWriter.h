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
#include <vector>
#include <Common/GlutenConfig.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Parser/SerializedPlanParser.h>
#include <Shuffle/CachedShuffleWriter.h>
#include <Shuffle/ShuffleCommon.h>
#include <jni/CelebornClient.h>


namespace DB
{
class MergingSortedAlgorithm;
}

namespace local_engine
{

struct SpillInfo
{
    std::string spilled_file;
    std::map<size_t, std::pair<size_t, size_t>> partition_spill_infos;
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

class CachedShuffleWriter;
using PartitionPtr = std::shared_ptr<Partition>;
class PartitionWriter : boost::noncopyable
{
public:
    explicit PartitionWriter(CachedShuffleWriter * shuffle_writer_, LoggerPtr logger_);
    virtual ~PartitionWriter() = default;

    virtual String getName() const = 0;

    virtual void write(const PartitionInfo & info, DB::Block & block);
    virtual void stop() = 0;

protected:
    virtual size_t evictPartitions() = 0;

    size_t bytes() const;

    virtual bool worthToSpill(size_t cache_size) const;

    virtual bool supportsEvictSinglePartition() const { return false; }

    virtual size_t evictSinglePartition(size_t partition_id)
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Evict single partition is not supported for {}", getName());
    }

    CachedShuffleWriter * shuffle_writer;
    const SplitOptions * options;
    MemoryConfig settings;

    std::vector<ColumnsBufferPtr> partition_block_buffer;
    std::vector<PartitionPtr> partition_buffer;

    /// Only valid in celeborn partition writer
    size_t last_partition_id;
    LoggerPtr logger = nullptr;
};

class Spillable
{
public:
    struct ExtraData
    {
        std::vector<ColumnsBufferPtr> partition_block_buffer;
        std::vector<PartitionPtr> partition_buffer;
    };

    Spillable(SplitOptions options_) : split_options(std::move(options_)) {}
    virtual ~Spillable() = default;

protected:
    String getNextSpillFile();
    std::vector<UInt64> mergeSpills(CachedShuffleWriter * shuffle_writer, WriteBuffer & data_file, ExtraData extra_data = {});
    std::vector<SpillInfo> spill_infos;

private:
    const SplitOptions split_options;
};

class LocalPartitionWriter : public PartitionWriter, public Spillable
{
public:
    explicit LocalPartitionWriter(CachedShuffleWriter * shuffle_writer);
    ~LocalPartitionWriter() override = default;

    String getName() const override { return "LocalPartitionWriter"; }

    size_t evictPartitions() override;
    void stop() override;

};

class SortBasedPartitionWriter : public PartitionWriter
{
protected:
    explicit SortBasedPartitionWriter(CachedShuffleWriter * shuffle_writer_, LoggerPtr logger) : PartitionWriter(shuffle_writer_, logger)
    {
        max_merge_block_size = options->split_size;
        max_sort_buffer_size = options->max_sort_buffer_size;
        max_merge_block_bytes = SerializedPlanParser::global_context->getSettings().prefer_external_sort_block_bytes;
    }
public:
    String getName() const override { return "SortBasedPartitionWriter"; }
    void write(const PartitionInfo & info, DB::Block & block) override;
    size_t adaptiveBlockSize()
    {
        size_t res = max_merge_block_size;
        if (max_merge_block_bytes)
        {
            res = std::min(std::max(max_merge_block_bytes / (current_accumulated_bytes / current_accumulated_rows), 128UL), res);
        }
        return res;
    }

protected:
    size_t max_merge_block_size = DB::DEFAULT_BLOCK_SIZE;
    size_t max_sort_buffer_size = 1_GiB;
    size_t max_merge_block_bytes = 0;
    size_t current_accumulated_bytes = 0;
    size_t current_accumulated_rows = 0;
    Chunks accumulated_blocks;
    Block output_header;
    Block sort_header;
    SortDescription sort_description;
};

class MemorySortLocalPartitionWriter : public SortBasedPartitionWriter, public Spillable
{
public:
    explicit MemorySortLocalPartitionWriter(CachedShuffleWriter* shuffle_writer_)
        : SortBasedPartitionWriter(shuffle_writer_, getLogger("MemorySortLocalPartitionWriter")), Spillable(shuffle_writer_->options)
    {
    }

    ~MemorySortLocalPartitionWriter() override = default;
    String getName() const override { return "MemorySortLocalPartitionWriter"; }

    size_t evictPartitions() override;
    void stop() override;
};

class MemorySortCelebornPartitionWriter : public SortBasedPartitionWriter
{
public:
    explicit MemorySortCelebornPartitionWriter(CachedShuffleWriter* shuffle_writer_, std::unique_ptr<CelebornClient> celeborn_client_)
        : SortBasedPartitionWriter(shuffle_writer_, getLogger("MemorySortCelebornPartitionWriter")), celeborn_client(std::move(celeborn_client_))
    {
    }

    String getName() const override { return "MemorySortCelebornPartitionWriter"; }
    ~MemorySortCelebornPartitionWriter() override = default;

    void stop() override;

protected:
    size_t evictPartitions() override;
private:
    std::unique_ptr<CelebornClient> celeborn_client;
};

class CelebornPartitionWriter : public PartitionWriter
{
public:
    CelebornPartitionWriter(CachedShuffleWriter * shuffleWriter, std::unique_ptr<CelebornClient> celeborn_client);
    ~CelebornPartitionWriter() override = default;

    String getName() const override { return "CelebornPartitionWriter"; }
    void stop() override;
protected:
    size_t evictPartitions() override;
    bool supportsEvictSinglePartition() const override { return true; }
    size_t evictSinglePartition(size_t partition_id) override;
private:
    std::unique_ptr<CelebornClient> celeborn_client;
};
}
