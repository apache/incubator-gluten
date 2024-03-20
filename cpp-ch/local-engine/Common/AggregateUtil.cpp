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

#include <Poco/Logger.h>
#include <Common/AggregateUtil.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

template <typename Method>
static Int32 extractMethodBucketsNum(Method & /*method*/)
{
    return Method::Data::NUM_BUCKETS;
}

Int32 GlutenAggregatorUtil::getBucketsNum(AggregatedDataVariants & data_variants)
{
    if (!data_variants.isTwoLevel())
    {
        return 0;
    }
    
    Int32 buckets_num = 0;
#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        buckets_num = extractMethodBucketsNum(*data_variants.NAME);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant");
    return buckets_num;
}

std::optional<Block> GlutenAggregatorUtil::safeConvertOneBucketToBlock(Aggregator & aggregator, AggregatedDataVariants & variants, Arena * arena, bool final, Int32 bucket)
{
    if (!variants.isTwoLevel())
        return {};
    if (bucket >= getBucketsNum(variants))
        return {};
    return aggregator.convertOneBucketToBlock(variants, arena, final, bucket);
}

template<typename Method>
static void releaseOneBucket(Method & method, Int32 bucket)
{
    method.data.impls[bucket].clearAndShrink();
}

void GlutenAggregatorUtil::safeReleaseOneBucket(AggregatedDataVariants & variants, Int32 bucket)
{
    if (!variants.isTwoLevel())
        return;
    if (bucket >= getBucketsNum(variants))
        return;
#define M(NAME) \
    else if (variants.type == AggregatedDataVariants::Type::NAME) \
        releaseOneBucket(*variants.NAME, bucket);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant");

}

}

namespace local_engine
{
AggregateDataBlockConverter::AggregateDataBlockConverter(DB::Aggregator & aggregator_, DB::AggregatedDataVariantsPtr data_variants_, bool final_)
    : aggregator(aggregator_), data_variants(std::move(data_variants_)), final(final_)
{
    if (data_variants->isTwoLevel())
    {
        buckets_num = DB::GlutenAggregatorUtil::getBucketsNum(*data_variants);
    }
    else if (data_variants->size())
        buckets_num = 1;
    else
        buckets_num = 0;
}

bool AggregateDataBlockConverter::hasNext()
{
    while (current_bucket < buckets_num && output_blocks.empty())
    {
        if (data_variants->isTwoLevel())
        {
            Stopwatch watch;
            auto optional_block = DB::GlutenAggregatorUtil::safeConvertOneBucketToBlock(
                aggregator, *data_variants, data_variants->aggregates_pool, final, current_bucket);
            if (!optional_block)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid bucket number {} for two-level aggregation", current_bucket);
            auto & block = *optional_block;
            LOG_DEBUG(
                &Poco::Logger::get("AggregateDataBlockConverter"),
                "Convert bucket {} into one block, rows: {}, cols: {}, bytes:{}, total bucket: {}, total rows: {}, time: {}",
                current_bucket,
                block.rows(),
                block.columns(),
                ReadableSize(block.allocatedBytes()),
                buckets_num,
                data_variants->size(),
                watch.elapsedMilliseconds());
            DB::GlutenAggregatorUtil::safeReleaseOneBucket(*data_variants, current_bucket);
            if (block.rows())
                output_blocks.emplace_back(std::move(block));
        }
        else
        {
            size_t keys = data_variants->size();
            auto blocks = aggregator.convertToBlocks(*data_variants, final, 1);
            size_t total_allocated_bytes = 0;
            size_t total_bytes = 0;
            while (!blocks.empty())
            {
                if (blocks.front().rows())
                {
                    total_allocated_bytes += blocks.front().allocatedBytes();
                    total_bytes += blocks.front().bytes();
                    output_blocks.emplace_back(std::move(blocks.front()));
                }
                blocks.pop_front();
            }
            LOG_DEBUG(
                &Poco::Logger::get("AggregateDataBlockConverter"),
                "Convert single level hash table into blocks. blocks: {}, total bytes: {}, total allocated bytes: {}, total rows: {}",
                output_blocks.size(),
                ReadableSize(total_bytes),
                ReadableSize(total_allocated_bytes),
                keys);
            data_variants = nullptr;
        }
        ++current_bucket;
    }
    return !output_blocks.empty();
}

DB::Block AggregateDataBlockConverter::next()
{
    auto block = output_blocks.front();
    output_blocks.pop_front();
    return block;
}
}
