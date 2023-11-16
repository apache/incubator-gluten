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

struct SpillInfo {
    std::string spilled_file;
    std::vector<PartitionSpillInfo> partition_spill_infos;
};


class Partition
{
public:
    Partition() = default;
    ~Partition() = default;

    Partition(Partition && other) noexcept : blocks(std::move(other.blocks)) { }

    void addBlock(DB::Block block);
    size_t spill(NativeWriter & writer);

private:
    std::vector<DB::Block> blocks;
};

class CachedShuffleWriter;
using PartitionPtr = std::shared_ptr<Partition>;
class PartitionWriter : boost::noncopyable
{
public:
    explicit PartitionWriter(CachedShuffleWriter* shuffle_writer_);
    virtual ~PartitionWriter() = default;

    virtual void write(const PartitionInfo& info, DB::Block & data);

    virtual void evictPartitions(bool for_memory_spill = false) = 0;

    virtual void stop() = 0;

    virtual size_t totalCacheSize()
    {
        return total_partition_buffer_size;
    }

protected:
    CachedShuffleWriter * shuffle_writer;
    SplitOptions * options;

    /// The mutex is used to protect partition_block_buffer and partition_buffer
    /// It may be accessed by multiple threads in which "CachedShuffleWriter::split" and "CachedShuffleWriter::spill" are invoked simutanously.
    ///
    /// Notice: the mutex must support being recursively acquired by the same thread, which happens during memory spilling.
    /// For more details, pls refer to https://github.com/oap-project/gluten/issues/3722
    mutable std::vector<std::recursive_mutex> mtxs;
    std::vector<ColumnsBufferPtr> partition_block_buffer;
    std::vector<PartitionPtr> partition_buffer;

    size_t total_partition_buffer_size = 0;
};

class LocalPartitionWriter : public PartitionWriter
{
public:
    explicit LocalPartitionWriter(CachedShuffleWriter * shuffle_writer);
    void evictPartitions(bool for_memory_spill) override;
    void stop() override;
    std::vector<Int64> mergeSpills(DB::WriteBuffer& data_file);

private:
    String getNextSpillFile();
    std::vector<SpillInfo> spill_infos;
};

class CelebornPartitionWriter : public PartitionWriter
{
public:
    CelebornPartitionWriter(CachedShuffleWriter * shuffleWriter, std::unique_ptr<CelebornClient> celeborn_client);
    void evictPartitions(bool for_memory_spill) override;
    void stop() override;

private:
    std::unique_ptr<CelebornClient> celeborn_client;
};
}



