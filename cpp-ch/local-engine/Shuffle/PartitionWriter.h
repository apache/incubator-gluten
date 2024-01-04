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
#include <Shuffle/ShuffleSplitter.h>
#include <jni/CelebornClient.h>

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

class CachedShuffleWriter;
using PartitionPtr = std::shared_ptr<Partition>;

class IPartitionWriter : boost::noncopyable
{
public:
    explicit IPartitionWriter(CachedShuffleWriter * shuffle_writer_);
    virtual ~IPartitionWriter() = default;

    virtual String getName() const = 0;

    void write(PartitionInfo & info, DB::Block & block);
    void stop();
    size_t evictPartitions(bool for_memory_spill = false, bool flush_block_buffer = false);

protected:
    virtual size_t bytes() const = 0;
    virtual void unsafeWrite(PartitionInfo & info, DB::Block & block) = 0;
    virtual void unsafeStop() = 0;
    virtual size_t unsafeEvictPartitions(bool for_memory_spill, bool flush_block_buffer) = 0;

    CachedShuffleWriter * shuffle_writer;
    const SplitOptions * options;

private:
    /// Make sure memory spill doesn't happen while write/stop are executed.
    bool evicting_or_writing{false};
};

class IHashBasedPartitionWriter : public IPartitionWriter
{
public:
    explicit IHashBasedPartitionWriter(CachedShuffleWriter * shuffle_writer_);
    virtual ~IHashBasedPartitionWriter() override = default;

protected:
    size_t bytes() const override;

    void unsafeWrite(PartitionInfo & info, DB::Block & block) override;
    virtual bool supportsEvictSinglePartition() const { return false; }
    virtual size_t unsafeEvictSinglePartition(bool for_memory_spill, bool flush_block_buffer, size_t partition_id)
    {
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Evict single partition is not supported for {}", getName());
    }

    std::vector<ColumnsBufferPtr> partition_block_buffer;
    std::vector<PartitionPtr> partition_buffer;

private:
    /// Only used when supportsEvictSinglePartition() returns true
    size_t last_partition_id;
};

class LocalHashBasedPartitionWriter : public IHashBasedPartitionWriter
{
public:
    explicit LocalHashBasedPartitionWriter(CachedShuffleWriter * shuffle_writer_) : IHashBasedPartitionWriter(shuffle_writer_) { }
    virtual ~LocalHashBasedPartitionWriter() override = default;

    String getName() const override { return "LocalHashBasedPartitionWriter"; }

protected:
    size_t unsafeEvictPartitions(bool for_memory_spill, bool flush_block_buffer) override;
    void unsafeStop() override;

private:
    String getNextSpillFile();
    std::vector<UInt64> mergeSpills(DB::WriteBuffer & data_file);
    std::vector<SpillInfo> spill_infos;
};

class CelebornHashBasedPartitionWriter : public IHashBasedPartitionWriter
{
public:
    explicit CelebornHashBasedPartitionWriter(CachedShuffleWriter * shuffle_writer_, std::unique_ptr<CelebornClient> celeborn_client_)
        : IHashBasedPartitionWriter(shuffle_writer_), celeborn_client(std::move(celeborn_client_))
    {
    }

    virtual ~CelebornHashBasedPartitionWriter() override = default;

    String getName() const override { return "CelebornHashBasedPartitionWriter"; }

protected:
    size_t unsafeEvictPartitions(bool for_memory_spill, bool flush_block_buffer) override;
    void unsafeStop() override;

    bool supportsEvictSinglePartition() const override { return true; }
    size_t unsafeEvictSinglePartition(bool for_memory_spill, bool flush_block_buffer, size_t partition_id) override;

private:
    std::unique_ptr<CelebornClient> celeborn_client;
};

class ISortBasedPartitionWriter : public IPartitionWriter
{
public:
    explicit ISortBasedPartitionWriter(CachedShuffleWriter * shuffle_writer_) : IPartitionWriter(shuffle_writer_) { }
    virtual ~ISortBasedPartitionWriter() override = default;

protected:
    size_t bytes() const override { return sorted_block_buffer->bytes(); }
    void unsafeWrite(PartitionInfo & info, DB::Block & block) override;
    void unsafeStop() override;
    size_t unsafeEvictPartitions(bool /*for_memory_spill*/, bool flush_block_buffer) override;

    virtual size_t
    unsafeEvictSinglePartitionFromBlock(size_t partition_id, const DB::Block & block, size_t start, size_t length)
        = 0;

    size_t flushSortedBlockBuffer();

    ColumnsBufferPtr sorted_block_buffer;
};

class CelebornSortedBasedPartitionWriter : public ISortBasedPartitionWriter
{
public:
    explicit CelebornSortedBasedPartitionWriter(CachedShuffleWriter * shuffle_writer_, std::unique_ptr<CelebornClient> celeborn_client_)
        : ISortBasedPartitionWriter(shuffle_writer_), celeborn_client(std::move(celeborn_client_))
    {
    }

    virtual ~CelebornSortedBasedPartitionWriter() override = default;

    String getName() const override { return "CelebornSortedBasedPartitionWriter"; }

protected:
    size_t unsafeEvictSinglePartitionFromBlock(size_t partition_id, const DB::Block & block, size_t start, size_t length) override;

private:
    std::unique_ptr<CelebornClient> celeborn_client;
};
}
