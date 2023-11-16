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
#include <memory>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Storages/IO/NativeWriter.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromFile.h>
#include <Shuffle/SelectorBuilder.h>
#include <Common/PODArray.h>
#include <Common/PODArray_fwd.h>
#include <base/types.h>
#include <Shuffle/ShuffleWriterBase.h>
#include <Storages/IO/CompressedWriteBuffer.h>


namespace local_engine
{
struct SplitOptions
{
    size_t split_size = DEFAULT_BLOCK_SIZE;
    size_t io_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    std::string data_file;
    std::vector<std::string> local_dirs_list;
    int num_sub_dirs;
    int shuffle_id;
    int map_id;
    size_t partition_num;
    std::string hash_exprs;
    std::string out_exprs;
    std::string compress_method = "zstd";
    int compress_level;
    size_t spill_threshold = 300 * 1024 * 1024;
    std::string hash_algorithm;
};

class ColumnsBuffer
{
public:
    ColumnsBuffer(size_t prefer_buffer_size = 8192);
    ~ColumnsBuffer() = default;

    void add(DB::Block & columns, int start, int end);
    void appendSelective(size_t column_idx, const DB::Block & source, const DB::IColumn::Selector & selector, size_t from, size_t length);

    size_t size() const;
    bool empty() const;

    DB::Block releaseColumns();
    DB::Block getHeader();

    size_t bytes() const
    {
        size_t res = 0;
        for (const auto & col : accumulated_columns)
            res += col->byteSize();
        return res;
    }

    size_t allocatedBytes() const
    {
        size_t res = 0;
        for (const auto & col : accumulated_columns)
            res += col->allocatedBytes();
        return res;
    }

private:
    DB::MutableColumns accumulated_columns;
    DB::Block header;
    size_t prefer_buffer_size;
};
using ColumnsBufferPtr = std::shared_ptr<ColumnsBuffer>;

struct SplitResult
{
    Int64 total_compute_pid_time = 0;
    Int64 total_write_time = 0;
    Int64 total_spill_time = 0;
    Int64 total_compress_time = 0;
    Int64 total_bytes_written = 0;
    Int64 total_bytes_spilled = 0;
    std::vector<Int64> partition_length;
    std::vector<Int64> raw_partition_length;
    Int64 total_split_time = 0;
    Int64 total_disk_time = 0;
    Int64 total_serialize_time = 0;
};

class ShuffleSplitter;
using ShuffleSplitterPtr = std::unique_ptr<ShuffleSplitter>;
class ShuffleSplitter : public ShuffleWriterBase
{
private:
    inline const static std::vector<std::string> compress_methods =  {"", "ZSTD", "LZ4"};

public:
    static ShuffleSplitterPtr create(const std::string & short_name, const SplitOptions & options_);

    explicit ShuffleSplitter(const SplitOptions & options);
    virtual ~ShuffleSplitter() override
    {
        if (!stopped)
            stop();
    }

    void split(DB::Block & block) override;
    virtual void computeAndCountPartitionId(DB::Block &) { }
    std::vector<int64_t> getPartitionLength() const { return split_result.partition_length; }
    void writeIndexFile();
    SplitResult stop() override;

private:
    void init();
    void initOutputIfNeeded(DB::Block & block);
    void splitBlockByPartition(DB::Block & block);
    void spillPartition(size_t partition_id);
    std::string getPartitionTempFile(size_t partition_id);
    void mergePartitionFiles();
    std::unique_ptr<DB::WriteBuffer> getPartitionWriteBuffer(size_t partition_id);

protected:
    bool stopped = false;
    PartitionInfo partition_info;
    std::vector<ColumnsBufferPtr> partition_buffer;
    std::vector<std::unique_ptr<local_engine::NativeWriter>> partition_outputs;
    std::vector<std::unique_ptr<DB::WriteBuffer>> partition_write_buffers;
    std::vector<std::unique_ptr<DB::WriteBuffer>> partition_cached_write_buffers;
    std::vector<local_engine::CompressedWriteBuffer *> compressed_buffers;
    std::vector<size_t> output_columns_indicies;
    DB::Block output_header;
    SplitOptions options;
    SplitResult split_result;
};

class RoundRobinSplitter : public ShuffleSplitter
{
public:
    static ShuffleSplitterPtr create(const SplitOptions & options);

    explicit RoundRobinSplitter(const SplitOptions & options_);
    virtual ~RoundRobinSplitter() override = default;

    void computeAndCountPartitionId(DB::Block & block) override;

private:
    std::unique_ptr<RoundRobinSelectorBuilder> selector_builder;
};

class HashSplitter : public ShuffleSplitter
{
public:
    static ShuffleSplitterPtr create(const SplitOptions & options);

    explicit HashSplitter(SplitOptions options_);
    virtual ~HashSplitter() override = default;

    void computeAndCountPartitionId(DB::Block & block) override;

private:
    std::unique_ptr<HashSelectorBuilder> selector_builder;
};

class RangeSplitter : public ShuffleSplitter
{
public:
    static ShuffleSplitterPtr create(const SplitOptions & options);

    explicit RangeSplitter(const SplitOptions & options_);
    virtual ~RangeSplitter() override = default;

    void computeAndCountPartitionId(DB::Block & block) override;

private:
    std::unique_ptr<RangeSelectorBuilder> selector_builder;
};
struct SplitterHolder
{
    std::unique_ptr<ShuffleWriterBase> splitter;
};


}
