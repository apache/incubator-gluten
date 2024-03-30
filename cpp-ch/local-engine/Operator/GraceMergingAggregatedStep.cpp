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
#include "GraceMergingAggregatedStep.h"
#include <Interpreters/JoinUtils.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CHUtil.h>
#include <Common/CurrentThread.h>
#include <Common/formatReadable.h>
#include <Common/BitHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits
    {
        {
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

static DB::Block buildOutputHeader(const DB::Block & input_header_, const DB::Aggregator::Params params_, bool final)
{
    return params_.getHeader(input_header_, final);
}

GraceMergingAggregatedStep::GraceMergingAggregatedStep(
    DB::ContextPtr context_,
    const DB::DataStream & input_stream_,
    DB::Aggregator::Params params_,
    bool no_pre_aggregated_)
    : DB::ITransformingStep(
        input_stream_, buildOutputHeader(input_stream_.header, params_, true), getTraits())
    , context(context_)
    , params(std::move(params_))
    , no_pre_aggregated(no_pre_aggregated_)
{
}

void GraceMergingAggregatedStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
    if (params.max_bytes_before_external_group_by)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "max_bytes_before_external_group_by is not supported in GraceMergingAggregatedStep");
    }
    auto num_streams = pipeline.getNumStreams();
    auto transform_params = std::make_shared<DB::AggregatingTransformParams>(pipeline.getHeader(), params, true);
    pipeline.resize(1);
    auto build_transform = [&](DB::OutputPortRawPtrs outputs)
    {
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto op = std::make_shared<GraceMergingAggregatedTransform>(pipeline.getHeader(), transform_params, context, no_pre_aggregated);
            new_processors.push_back(op);
            DB::connect(*output, op->getInputs().front());
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
    pipeline.resize(num_streams, true);
}

void GraceMergingAggregatedStep::describeActions(DB::IQueryPlanStep::FormatSettings & settings) const
{
    return params.explain(settings.out, settings.offset);
}

void GraceMergingAggregatedStep::describeActions(DB::JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void GraceMergingAggregatedStep::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), buildOutputHeader(input_streams.front().header, params, true), getDataStreamTraits());
}

GraceMergingAggregatedTransform::GraceMergingAggregatedTransform(const DB::Block &header_, DB::AggregatingTransformParamsPtr params_, DB::ContextPtr context_, bool no_pre_aggregated_)
    : IProcessor({header_}, {params_->getHeader()})
    , header(header_)
    , params(params_)
    , context(context_)
    , key_columns(params_->params.keys_size)
    , aggregate_columns(params_->params.aggregates_size)
    , no_pre_aggregated(no_pre_aggregated_)
    , tmp_data_disk(std::make_unique<DB::TemporaryDataOnDisk>(context_->getTempDataOnDisk()))
{
    max_buckets = context->getConfigRef().getUInt64("max_grace_aggregate_merging_buckets", 32);
    throw_on_overflow_buckets = context->getConfigRef().getBool("throw_on_overflow_grace_aggregate_merging_buckets", false);
    aggregated_keys_before_extend_buckets = context->getConfigRef().getUInt64("aggregated_keys_before_extend_grace_aggregate_merging_buckets", 8196);
    aggregated_keys_before_extend_buckets = PODArrayUtil::adjustMemoryEfficientSize(aggregated_keys_before_extend_buckets);
    max_pending_flush_blocks_per_bucket = context->getConfigRef().getUInt64("max_pending_flush_blocks_per_grace_aggregate_merging_bucket", 1024 * 1024);
    max_allowed_memory_usage_ratio = context->getConfigRef().getDouble("max_allowed_memory_usage_ratio_for_aggregate_merging", 0.9);
    // bucket 0 is for in-memory data, it's just a placeholder.
    buckets.emplace(0, BufferFileStream());

    current_data_variants = std::make_shared<DB::AggregatedDataVariants>();
}

GraceMergingAggregatedTransform::~GraceMergingAggregatedTransform()
{
    LOG_INFO(
        logger,
        "Metrics. total_input_blocks: {}, total_input_rows: {}, total_output_blocks: {}, total_output_rows: {}, total_spill_disk_bytes: "
        "{}, total_spill_disk_time: {}, total_read_disk_time: {}, total_scatter_time: {}",
        total_input_blocks,
        total_input_rows,
        total_output_blocks,
        total_output_rows,
        total_spill_disk_bytes,
        total_spill_disk_time,
        total_read_disk_time,
        total_scatter_time);
}

GraceMergingAggregatedTransform::Status GraceMergingAggregatedTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }
    if (has_output)
    {
        if (output.canPush())
        {
            LOG_DEBUG(
                logger,
                "Output one chunk. rows: {}, bytes: {}, current memory usage: {}",
                output_chunk.getNumRows(),
                ReadableSize(output_chunk.bytes()),
                ReadableSize(MemoryUtil::getCurrentMemoryUsage()));
            total_output_rows += output_chunk.getNumRows();
            total_output_blocks++;
            output.push(std::move(output_chunk));
            has_output = false;
        }
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;


    if (!input_finished)
    {
        if (input.isFinished())
        {
            input_finished = true;
            return Status::Ready;
        }
        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;
        input_chunk = input.pull(true);
        LOG_DEBUG(
            logger,
            "Input one new chunk. rows: {}, bytes: {}, current memory usage: {}",
            input_chunk.getNumRows(),
            ReadableSize(input_chunk.bytes()),
            ReadableSize(MemoryUtil::getCurrentMemoryUsage()));
        total_input_rows += input_chunk.getNumRows();
        total_input_blocks++;
        has_input = true;
        return Status::Ready;
    }

    if (current_bucket_index >= getBucketsNum() && (!block_converter || !block_converter->hasNext()))
    {
        output.finish();
        return Status::Finished;
    }
    return Status::Ready;
}

void GraceMergingAggregatedTransform::work()
{
    if (has_input)
    {
        assert(!input_finished);
        auto block = header.cloneWithColumns(input_chunk.detachColumns());
        mergeOneBlock(block, true);
        has_input = false;
    }
    else
    {
        assert(input_finished);
        if (!block_converter || !block_converter->hasNext())
        {
            block_converter = nullptr;
            while (current_bucket_index < getBucketsNum())
            {
                block_converter = prepareBucketOutputBlocks(current_bucket_index);
                if (block_converter)
                    break;
                current_bucket_index++;  
            }
        }
        if (!block_converter)
        {
            return;
        }

        while(block_converter->hasNext())
        {
            auto block = block_converter->next();
            if (!block.rows())
                continue;
            output_chunk = DB::Chunk(block.getColumns(), block.rows());
            has_output = true;
            break;
        }

        if (!block_converter->hasNext())
        {
            block_converter = nullptr;
            current_bucket_index++;
        }
    }
}

bool GraceMergingAggregatedTransform::extendBuckets()
{
    if (!current_data_variants || current_data_variants->size() < aggregated_keys_before_extend_buckets)
        return false;

    auto current_size = getBucketsNum();
    auto next_size = current_size * 2;
    /// We have a soft limit on the number of buckets. When throw_on_overflow_buckets = false, we just
    /// continue to run with the current number of buckets until the executor is killed by spark scheduler.
    if (next_size > max_buckets)
    {
        if (throw_on_overflow_buckets)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Too many buckets, limit is {}. Please consider increate offhead size or partitoin number",
                max_buckets);
        else
         return false;
    }
    LOG_DEBUG(logger, "Extend buckets num from {} to {}", current_size, next_size);
    for (size_t i = current_size; i < next_size; ++i)
        buckets.emplace(i, BufferFileStream());
    return true;
}

void GraceMergingAggregatedTransform::rehashDataVariants()
{
    auto before_memoery_usage = MemoryUtil::getCurrentMemoryUsage();

    auto converter = currentDataVariantToBlockConverter(false);
    checkAndSetupCurrentDataVariants();
    size_t block_rows = 0;
    size_t block_memory_usage = 0;
    no_more_keys = false;

    size_t bucket_n = 0;
    while(converter->hasNext())
    {
        auto block = converter->next();
        if (!block.rows())
            continue;
        block_rows += block.rows();
        block_memory_usage += block.allocatedBytes();
        auto scattered_blocks = scatterBlock(block);
        block = {};
        /// the new scattered blocks from block will alway belongs to the buckets with index >= current_bucket_index
        for (size_t i = 0; i < current_bucket_index; ++i)
        {
            if (scattered_blocks[i].rows())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Scattered blocks should not belong to buckets with index({}) < current_bucket_index({})", i, current_bucket_index);
        }
        for (size_t i = current_bucket_index + 1; i < getBucketsNum(); ++i)
        {
            addBlockIntoFileBucket(i, scattered_blocks[i], false);
            scattered_blocks[i] = {};
        }

        params->aggregator.mergeOnBlock(scattered_blocks[current_bucket_index], *current_data_variants, no_more_keys, is_cancelled);
    }
    if (block_rows)
        per_key_memory_usage = block_memory_usage * 1.0 / block_rows;

    LOG_INFO(
        logger,
        "Rehash data variants. current_bucket_index: {}, buckets num: {}, memory usage change, from {} to {}",
        current_bucket_index,
        getBucketsNum(),
        ReadableSize(before_memoery_usage),
        ReadableSize(MemoryUtil::getCurrentMemoryUsage()));
};

DB::Blocks GraceMergingAggregatedTransform::scatterBlock(const DB::Block & block)
{
    if (!block.rows())
        return {};
    Stopwatch watch;
    size_t bucket_num = getBucketsNum();
    auto blocks = DB::JoinCommon::scatterBlockByHash(params->params.keys, block, bucket_num);
    for (auto & new_block : blocks)
    {
        new_block.info.bucket_num = static_cast<Int32>(bucket_num);
    }
    total_scatter_time += watch.elapsedMilliseconds();
    return blocks;
}

void GraceMergingAggregatedTransform::addBlockIntoFileBucket(size_t bucket_index, const DB::Block & block, bool is_original_block)
{
    if (!block.rows())
        return;
    if (roundUpToPowerOfTwoOrZero(bucket_index + 1) > static_cast<size_t>(block.info.bucket_num))
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Add invalid block with bucket_num {} into bucket {}", block.info.bucket_num, bucket_index);
    }
    auto & file_stream = buckets[bucket_index];
    file_stream.pending_bytes += block.allocatedBytes();
    if (is_original_block && no_pre_aggregated)
        file_stream.original_blocks.push_back(block);
    else
        file_stream.intermediate_blocks.push_back(block);
    if (file_stream.pending_bytes > max_pending_flush_blocks_per_bucket)
    {
        flushBucket(bucket_index);
        file_stream.pending_bytes = 0;
    }
}

void GraceMergingAggregatedTransform::flushBuckets()
{
    for (size_t i = current_bucket_index + 1; i < getBucketsNum(); ++i)
        flushBucket(i);
}

static size_t flushBlocksInfoDisk(DB::TemporaryFileStream * file_stream, std::list<DB::Block> & blocks)
{
    size_t flush_bytes = 0;
    DB::Blocks tmp_blocks;
    while (!blocks.empty())
    {
        while (!blocks.empty())
        {
            if (!tmp_blocks.empty() && tmp_blocks.back().info.bucket_num != blocks.front().info.bucket_num)
                break;
            tmp_blocks.push_back(blocks.front());
            blocks.pop_front();
        }
        auto bucket = tmp_blocks.front().info.bucket_num;
        auto merged_block = BlockUtil::concatenateBlocksMemoryEfficiently(std::move(tmp_blocks));
        merged_block.info.bucket_num = bucket;
        tmp_blocks.clear();
        flush_bytes += merged_block.bytes();
        if (merged_block.rows())
        {
            file_stream->write(merged_block);
        }
    }
    if (flush_bytes)
        file_stream->flush();
    return flush_bytes;
}

size_t GraceMergingAggregatedTransform::flushBucket(size_t bucket_index)
{
    Stopwatch watch;
    auto & file_stream = buckets[bucket_index];
    size_t flush_bytes = 0;
    if (!file_stream.original_blocks.empty())
    {
        if (!file_stream.original_file_stream)
            file_stream.original_file_stream = &tmp_data_disk->createStream(header);
        flush_bytes += flushBlocksInfoDisk(file_stream.original_file_stream, file_stream.original_blocks);
    }
    if (!file_stream.intermediate_blocks.empty())
    {
        if (!file_stream.intermediate_file_stream)
        {
            auto intermediate_header = params->aggregator.getHeader(false);
            file_stream.intermediate_file_stream = &tmp_data_disk->createStream(intermediate_header);
        }
        flush_bytes += flushBlocksInfoDisk(file_stream.intermediate_file_stream, file_stream.intermediate_blocks);
    }
    total_spill_disk_bytes += flush_bytes;
    total_spill_disk_time += watch.elapsedMilliseconds();
    return flush_bytes;
}

std::unique_ptr<AggregateDataBlockConverter> GraceMergingAggregatedTransform::prepareBucketOutputBlocks(size_t bucket_index)
{
    auto & buffer_file_stream = buckets[bucket_index];
    if (!current_data_variants && !buffer_file_stream.intermediate_file_stream && buffer_file_stream.intermediate_blocks.empty()
        && !buffer_file_stream.original_file_stream && buffer_file_stream.original_blocks.empty())
    {
        return nullptr;
    }

    size_t read_bytes = 0;
    size_t read_rows = 0;
    Stopwatch watch;

    checkAndSetupCurrentDataVariants();

    if (buffer_file_stream.intermediate_file_stream)
    {
        buffer_file_stream.intermediate_file_stream->finishWriting();
        while (true)
        {
            auto block = buffer_file_stream.intermediate_file_stream->read();
            if (!block.rows())
                break;
            read_bytes += block.bytes();
            read_rows += block.rows();
            mergeOneBlock(block, false);
            block = {};
        }
        buffer_file_stream.intermediate_file_stream = nullptr;
        total_read_disk_time += watch.elapsedMilliseconds();
    }
    if (!buffer_file_stream.intermediate_blocks.empty())
    {
        for (auto & block : buffer_file_stream.intermediate_blocks)
        {
            mergeOneBlock(block, false);
            block = {};
        }
    }
    
    if (buffer_file_stream.original_file_stream)
    {
        buffer_file_stream.original_file_stream->finishWriting();
        while (true)
        {
            auto block = buffer_file_stream.original_file_stream->read();
            if (!block.rows())
                break;
            read_bytes += block.bytes();
            read_rows += block.rows();
            mergeOneBlock(block, true);
            block = {};
        }
        buffer_file_stream.original_file_stream = nullptr;
        total_read_disk_time += watch.elapsedMilliseconds();
    }
    if (!buffer_file_stream.original_blocks.empty())
    {
        for (auto & block : buffer_file_stream.original_blocks)
        {
            mergeOneBlock(block, true);
            block = {};
        }
    }

    auto last_data_variants_size = current_data_variants->size();
    auto converter = currentDataVariantToBlockConverter(true);
    LOG_INFO(
        logger,
        "prepare to output bucket {}, aggregated result keys: {}, keys size: {}, read bytes from disk: {}, read rows: {}, time: {} ms",
        bucket_index,
        last_data_variants_size,
        params->params.keys_size,
        ReadableSize(read_bytes),
        read_rows,
        watch.elapsedMilliseconds());
    return std::move(converter);
}

std::unique_ptr<AggregateDataBlockConverter> GraceMergingAggregatedTransform::currentDataVariantToBlockConverter(bool final)
{
    if (!current_data_variants)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "current data variants is null");
    }
    auto converter = std::make_unique<AggregateDataBlockConverter>(params->aggregator, current_data_variants, final);
    current_data_variants = nullptr;
    return std::move(converter);
}

void GraceMergingAggregatedTransform::checkAndSetupCurrentDataVariants()
{
    if (!current_data_variants)
    {
        current_data_variants = std::make_shared<DB::AggregatedDataVariants>();
        no_more_keys = false;
    }
}

void GraceMergingAggregatedTransform::mergeOneBlock(const DB::Block &block, bool is_original_block)
{
    if (!block.rows())
        return;

    checkAndSetupCurrentDataVariants();

    // first to flush pending bytes into disk.
    if (isMemoryOverflow())
        flushBuckets();
    // then try to extend buckets.
    if (isMemoryOverflow() && extendBuckets())
    {
        rehashDataVariants();
    }

    LOG_DEBUG(
        logger,
        "merge on block, rows: {}, bytes:{}, bucket: {}. current bucket: {}, total bucket: {}, mem used: {}",
        block.rows(),
        ReadableSize(block.bytes()),
        block.info.bucket_num,
        current_bucket_index,
        getBucketsNum(),
        ReadableSize(MemoryUtil::getCurrentMemoryUsage()));

    /// the block could be one read from disk. block.info.bucket_num stores the number of buckets when it was scattered.
    /// so if the buckets number is not changed since it was scattered, we don't need to scatter it again.
    if (block.info.bucket_num == static_cast<Int32>(getBucketsNum()) || getBucketsNum() == 1)
    {
        if (is_original_block && no_pre_aggregated)
            params->aggregator.executeOnBlock(block, *current_data_variants, key_columns, aggregate_columns, no_more_keys);
        else
            params->aggregator.mergeOnBlock(block, *current_data_variants, no_more_keys, is_cancelled);
    }
    else
    {
        auto bucket_num = block.info.bucket_num;
        auto scattered_blocks = scatterBlock(block);
        for (size_t i = 0; i < current_bucket_index; ++i)
        {
            if (scattered_blocks[i].rows())
            {
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "Scattered blocks should not belong to buckets with index({}) < current_bucket_index({}). bucket_num:{}. "
                    "scattered_blocks.size: {}, total buckets: {}",
                    i,
                    current_bucket_index,
                    bucket_num,
                    scattered_blocks.size(),
                    getBucketsNum());
            }
        }
        for (size_t i = current_bucket_index + 1; i < getBucketsNum(); ++i)
        {
            addBlockIntoFileBucket(i, scattered_blocks[i], is_original_block);
        }

        if (is_original_block && no_pre_aggregated)
        {
            params->aggregator.executeOnBlock(scattered_blocks[current_bucket_index], *current_data_variants, key_columns, aggregate_columns, no_more_keys);
        }
        else
        {
            params->aggregator.mergeOnBlock(scattered_blocks[current_bucket_index], *current_data_variants, no_more_keys, is_cancelled);
        }
    }
}

bool GraceMergingAggregatedTransform::isMemoryOverflow()
{
    /// More greedy memory usage strategy.
    if (!current_data_variants)
        return false;
    if (!context->getSettingsRef().max_memory_usage)
        return false;
    auto max_mem_used = static_cast<size_t>(context->getSettingsRef().max_memory_usage * max_allowed_memory_usage_ratio);
    auto current_result_rows = current_data_variants->size();
    auto current_mem_used = MemoryUtil::getCurrentMemoryUsage();
    if (per_key_memory_usage > 0)
    {
        if (current_mem_used + per_key_memory_usage * current_result_rows >= max_mem_used)
        {
            LOG_INFO(
                logger,
                "Memory is overflow. current_mem_used: {}, max_mem_used: {}, per_key_memory_usage: {}, aggregator keys: {}, buckets: {}, hash table type: {}",
                ReadableSize(current_mem_used),
                ReadableSize(max_mem_used),
                ReadableSize(per_key_memory_usage),
                current_result_rows,
                getBucketsNum(),
                current_data_variants->type);
            return true;
        }
    }
    else
    {
        if (current_mem_used * 2 >= max_mem_used)
        {
            LOG_INFO(
                logger,
                "Memory is overflow on half of max usage. current_mem_used: {}, max_mem_used: {}, aggregator keys: {}, buckets: {}, hash table type: {}",
                ReadableSize(current_mem_used),
                ReadableSize(max_mem_used),
                current_result_rows,
                getBucketsNum(),
                current_data_variants->type);
            return true;
        }
    }
    return false;
}
}
