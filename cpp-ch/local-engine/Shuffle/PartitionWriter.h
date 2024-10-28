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
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Shuffle/ShuffleCommon.h>
#include <jni/CelebornClient.h>
#include <Common/GlutenConfig.h>
#include <Common/QueryContext.h>

namespace DB
{
class MergingSortedAlgorithm;
namespace Setting
{
extern const SettingsUInt64 prefer_external_sort_block_bytes;
}
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
friend class Spillable;
public:
    PartitionWriter(const SplitOptions& options, LoggerPtr logger_);
    virtual ~PartitionWriter() = default;

    void initialize(SplitResult * split_result_, const DB::Block & output_header_)
    {
        if (!init)
        {
            split_result = split_result_;
            chassert(split_result != nullptr);
            split_result->partition_lengths.resize(options.partition_num);
            split_result->raw_partition_lengths.resize(options.partition_num);
            output_header = output_header_;
            init = true;
        }
    }
    virtual String getName() const = 0;

    virtual void write(const PartitionInfo & info, DB::Block & block);
    virtual bool useRSSPusher() const = 0;
    virtual size_t evictPartitions() = 0;

protected:

    size_t bytes() const;

    virtual bool worthToSpill(size_t cache_size) const;

    virtual bool supportsEvictSinglePartition() const { return false; }

    virtual size_t evictSinglePartition(size_t partition_id)
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Evict single partition is not supported for {}", getName());
    }

    const SplitOptions & options;
    MemoryConfig settings;

    std::vector<ColumnsBufferPtr> partition_block_buffer;
    std::vector<PartitionPtr> partition_buffer;

    /// Only valid in celeborn partition writer
    size_t last_partition_id;
    SplitResult * split_result = nullptr;
    DB::Block output_header;
    LoggerPtr logger = nullptr;
    bool init = false;
};

class Spillable
{
public:
    struct ExtraData
    {
        std::vector<ColumnsBufferPtr> partition_block_buffer;
        std::vector<PartitionPtr> partition_buffer;
    };

    Spillable(const SplitOptions& options_) : spill_options(options_) {}
    virtual ~Spillable() = default;
    const std::vector<SpillInfo> & getSpillInfos() const
    {
        return spill_infos;
    }

protected:
    String getNextSpillFile();
    std::vector<SpillInfo> spill_infos;
    const SplitOptions& spill_options;
};

class LocalPartitionWriter : public PartitionWriter, public Spillable
{
public:
    explicit LocalPartitionWriter(const SplitOptions& options);
    ~LocalPartitionWriter() override = default;

    String getName() const override { return "LocalPartitionWriter"; }
    ExtraData getExtraData()
    {
        return {partition_block_buffer, partition_buffer};
    }
    size_t evictPartitions() override;
    bool useRSSPusher() const override { return false; }
};

class SortBasedPartitionWriter : public PartitionWriter
{
protected:
    explicit SortBasedPartitionWriter(const SplitOptions& options, LoggerPtr logger) : PartitionWriter(options, logger)
    {
        max_merge_block_size = options.split_size;
        max_sort_buffer_size = options.max_sort_buffer_size;
        max_merge_block_bytes = QueryContext::globalContext()->getSettingsRef()[DB::Setting::prefer_external_sort_block_bytes];
    }
public:
    String getName() const override { return "SortBasedPartitionWriter"; }
    void write(const PartitionInfo & info, DB::Block & block) override;
    size_t adaptiveBlockSize() const
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
    DB::Chunks accumulated_blocks;
    DB::Block output_header;
    DB::Block sort_header;
    DB::SortDescription sort_description;
};

class MemorySortLocalPartitionWriter : public SortBasedPartitionWriter, public Spillable
{
public:
    explicit MemorySortLocalPartitionWriter(const SplitOptions& options)
        : SortBasedPartitionWriter(options, getLogger("MemorySortLocalPartitionWriter")), Spillable(options)
    {
    }

    ~MemorySortLocalPartitionWriter() override = default;
    String getName() const override { return "MemorySortLocalPartitionWriter"; }

    size_t evictPartitions() override;
    bool useRSSPusher() const override { return false; }
};

class MemorySortCelebornPartitionWriter : public SortBasedPartitionWriter
{
public:
    explicit MemorySortCelebornPartitionWriter(const SplitOptions& options, std::unique_ptr<CelebornClient> celeborn_client_)
        : SortBasedPartitionWriter(options, getLogger("MemorySortCelebornPartitionWriter")), celeborn_client(std::move(celeborn_client_))
    {
    }

    String getName() const override { return "MemorySortCelebornPartitionWriter"; }
    ~MemorySortCelebornPartitionWriter() override = default;

    bool useRSSPusher() const override { return true; }

    size_t evictPartitions() override;
private:
    std::unique_ptr<CelebornClient> celeborn_client;
};

class CelebornPartitionWriter : public PartitionWriter
{
public:
    CelebornPartitionWriter(const SplitOptions& options, std::unique_ptr<CelebornClient> celeborn_client);
    ~CelebornPartitionWriter() override = default;

    String getName() const override { return "CelebornPartitionWriter"; }
    bool useRSSPusher() const override { return true; }
    size_t evictPartitions() override;

protected:
    bool supportsEvictSinglePartition() const override { return true; }
    size_t evictSinglePartition(size_t partition_id) override;
private:
    std::unique_ptr<CelebornClient> celeborn_client;
};
}
