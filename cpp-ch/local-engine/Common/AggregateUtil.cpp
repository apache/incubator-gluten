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

#include <Common/AggregateUtil.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
AggregateDataBlockConverter::AggregateDataBlockConverter(DB::Aggregator & aggregator_, DB::AggregatedDataVariantsPtr data_variants_, bool final_)
    : aggregator(aggregator_), data_variants(std::move(data_variants_)), final(final_)
{
    if (data_variants->isTwoLevel())
    {
        buckets_num = aggregator.getBucketsNum(*data_variants);
    }
    else
        buckets_num = 1;
}

bool AggregateDataBlockConverter::hasNext()
{
    return current_bucket < buckets_num;
}

DB::Block AggregateDataBlockConverter::next()
{
    DB::Block block;
    if (data_variants->isTwoLevel())
    {
        Stopwatch watch;
        auto optional_block = aggregator.safeConvertOneBucketToBlock(*data_variants, data_variants->aggregates_pool, final, current_bucket);
        if (!optional_block)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid bucket number {} for two-level aggregation", current_bucket);
        block = std::move(*optional_block);
        LOG_ERROR(
            &Poco::Logger::get("AggregateDataBlockConverter"),
            "xxx convert bucket {} into one block, rows: {}, total bucket: {}, total rows: {}, time: {}",
            current_bucket,
            block.rows(),
            buckets_num,
            data_variants->size(),
            watch.elapsedMilliseconds());
        // aggregator.safeReleaseOneBucket(*data_variants, current_bucket);
    }
    else
    {
        block = aggregator.convertToSingleBlock(*data_variants, final);
    }
    ++current_bucket;
    return block;
}
}
