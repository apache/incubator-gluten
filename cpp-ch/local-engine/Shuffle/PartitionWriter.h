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
#include <IO/WriteBuffer.h>
#include <Core/Block.h>
#include <Shuffle/ShuffleSplitter.h>
#include <jni/CelebornClient.h>

namespace local_engine
{
struct PartitionSpillInfo {
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
class PartitionWriter : boost::noncopyable
{
public:
    explicit PartitionWriter(CachedShuffleWriter* shuffle_writer_);
    virtual ~PartitionWriter() = default;

    virtual String getName() const = 0;

    void write(const PartitionInfo& info, DB::Block & block);
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



