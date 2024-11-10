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

#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/MergeTree/SparkMergeTreeWriteSettings.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <Common/BlockTypeUtils.h>

namespace local_engine
{

struct MergeTreeTable;
using SparkStorageMergeTreePtr = std::shared_ptr<SparkStorageMergeTree>;
class SinkHelper;
using SinkHelperPtr = std::shared_ptr<SinkHelper>;

// TODO: Remove ConcurrentDeque
template <typename T>
class ConcurrentDeque
{
public:
    std::optional<T> pop_front()
    {
        std::lock_guard<std::mutex> lock(mtx);

        if (deq.empty())
            return {};

        T t = deq.front();
        deq.pop_front();
        return t;
    }

    void emplace_back(T value)
    {
        std::lock_guard<std::mutex> lock(mtx);
        deq.emplace_back(value);
    }

    void emplace_back(std::vector<T> values)
    {
        std::lock_guard<std::mutex> lock(mtx);
        deq.insert(deq.end(), values.begin(), values.end());
    }

    void emplace_front(T value)
    {
        std::lock_guard<std::mutex> lock(mtx);
        deq.emplace_front(value);
    }

    size_t size()
    {
        std::lock_guard<std::mutex> lock(mtx);
        return deq.size();
    }

    bool empty()
    {
        std::lock_guard<std::mutex> lock(mtx);
        return deq.empty();
    }

    /// !!! unsafe get, only called when background tasks are finished
    const std::deque<T> & unsafeGet() const { return deq; }

private:
    std::deque<T> deq;
    mutable std::mutex mtx;
};

class SinkHelper
{
protected:
    SparkStorageMergeTreePtr data;
    bool isRemoteStorage;

    ConcurrentDeque<DB::MergeTreeDataPartPtr> new_parts;
    std::unordered_set<String> tmp_parts{};
    ThreadPool thread_pool;

public:
    const SparkMergeTreeWriteSettings write_settings;
    const DB::StorageMetadataPtr metadata_snapshot;

protected:
    virtual SparkStorageMergeTree & dest_storage() { return *data; }

    void doMergePartsAsync(const std::vector<DB::MergeTreeDataPartPtr> & prepare_merge_parts);
    void finalizeMerge();
    virtual void cleanup() { }
    virtual void commit(const ReadSettings & read_settings, const WriteSettings & write_settings) { }
    void saveMetadata(const DB::ContextPtr & context);
    SparkWriteStorageMergeTree & dataRef() const { return assert_cast<SparkWriteStorageMergeTree &>(*data); }

public:
    const std::deque<DB::MergeTreeDataPartPtr> & unsafeGet() const { return new_parts.unsafeGet(); }

    void writeTempPart(DB::BlockWithPartition & block_with_partition, const ContextPtr & context, int part_num);
    void checkAndMerge(bool force = false);
    void finish(const DB::ContextPtr & context);

    virtual ~SinkHelper() = default;
    SinkHelper(const SparkStorageMergeTreePtr & data_, const SparkMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_);
};

class DirectSinkHelper : public SinkHelper
{
protected:
    void cleanup() override;

public:
    explicit DirectSinkHelper(
        const SparkStorageMergeTreePtr & data_, const SparkMergeTreeWriteSettings & write_settings_, bool isRemoteStorage_)
        : SinkHelper(data_, write_settings_, isRemoteStorage_)
    {
    }
};

class CopyToRemoteSinkHelper : public SinkHelper
{
    SparkStorageMergeTreePtr dest;

protected:
    void commit(const ReadSettings & read_settings, const WriteSettings & write_settings) override;
    SparkStorageMergeTree & dest_storage() override { return *dest; }
    const SparkStorageMergeTreePtr & temp_storage() const { return data; }

public:
    explicit CopyToRemoteSinkHelper(
        const SparkStorageMergeTreePtr & temp, const SparkStorageMergeTreePtr & dest_, const SparkMergeTreeWriteSettings & write_settings_)
        : SinkHelper(temp, write_settings_, true), dest(dest_)
    {
        assert(data != dest);
    }
};

class SparkMergeTreeSink : public DB::SinkToStorage
{
public:
    static SinkHelperPtr create(
        const MergeTreeTable & merge_tree_table,
        const SparkMergeTreeWriteSettings & write_settings_,
        const DB::ContextMutablePtr & context);

    explicit SparkMergeTreeSink(const SinkHelperPtr & sink_helper_, const ContextPtr & context_)
        : SinkToStorage(sink_helper_->metadata_snapshot->getSampleBlock()), context(context_), sink_helper(sink_helper_)
    {
    }
    ~SparkMergeTreeSink() override = default;

    String getName() const override { return "SparkMergeTreeSink"; }
    void consume(Chunk & chunk) override;
    void onStart() override;
    void onFinish() override;

    const SinkHelper & sinkHelper() const { return *sink_helper; }

private:
    ContextPtr context;
    SinkHelperPtr sink_helper;

    int part_num = 1;
};


class MergeTreeStats : public DB::ISimpleTransform
{
    bool all_chunks_processed_ = false; /// flag to determine if we have already processed all chunks
    const SinkHelper & sink_helper;

    static DB::Block statsHeader()
    {
        return makeBlockHeader(
            {{STRING(), "part_name"},
             {STRING(), "partition_id"},
             {BIGINT(), "record_count"},
             {BIGINT(), "marks_count"},
             {BIGINT(), "size_in_bytes"}});
    }

    DB::Chunk final_result() const
    {
        // TODO: remove it
        const std::string NO_PARTITION_ID{"__NO_PARTITION_ID__"};

        auto parts = sink_helper.unsafeGet();

        const size_t size = parts.size();
        auto file_col = STRING()->createColumn();
        file_col->reserve(size);

        auto partition_col = STRING()->createColumn();
        partition_col->reserve(size);

        auto countCol = BIGINT()->createColumn();
        countCol->reserve(size);
        auto & countColData = static_cast<DB::ColumnVector<Int64> &>(*countCol).getData();

        auto marksCol = BIGINT()->createColumn();
        marksCol->reserve(size);
        auto & marksColData = static_cast<DB::ColumnVector<Int64> &>(*marksCol).getData();

        auto bytesCol = BIGINT()->createColumn();
        bytesCol->reserve(size);
        auto & bytesColData = static_cast<DB::ColumnVector<Int64> &>(*bytesCol).getData();

        for (const auto & part : parts)
        {
            file_col->insertData(part->name.c_str(), part->name.size());
            partition_col->insertData(NO_PARTITION_ID.c_str(), NO_PARTITION_ID.size());
            countColData.emplace_back(part->rows_count);
            marksColData.emplace_back(part->getMarksCount());
            bytesColData.emplace_back(part->getBytesOnDisk());
        }
        const DB::Columns res_columns{
            std::move(file_col), std::move(partition_col), std::move(countCol), std::move(marksCol), std::move(bytesCol)};
        return DB::Chunk(res_columns, size);
    }

public:
    explicit MergeTreeStats(const DB::Block & input_header_, const SinkHelper & sink_helper_)
        : ISimpleTransform(input_header_, statsHeader(), true), sink_helper(sink_helper_)
    {
    }
    Status prepare() override
    {
        if (input.isFinished() && !output.isFinished() && !has_input && !all_chunks_processed_)
        {
            all_chunks_processed_ = true;
            /// return Ready to call transform() for generating filling rows after latest chunk was processed
            return Status::Ready;
        }

        return ISimpleTransform::prepare();
    }

    String getName() const override { return "MergeTreeStats"; }
    void transform(DB::Chunk & chunk) override
    {
        if (all_chunks_processed_)
            chunk = final_result();
        else
            chunk = {};
    }
};
}
