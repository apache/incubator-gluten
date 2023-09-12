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
#include "CachedShuffleWriter.h"
#include <Poco/StringTokenizer.h>
#include <Common/Stopwatch.h>
#include <Shuffle/PartitionWriter.h>
#include <jni/CelebornClient.h>
#include <jni/jni_common.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}


namespace local_engine
{
using namespace DB;
CachedShuffleWriter::CachedShuffleWriter(const String & short_name, SplitOptions & options_, jobject rss_pusher)
{
    options = options_;
    if (short_name == "rr")
    {
        partitioner = std::make_unique<RoundRobinSelectorBuilder>(options.partition_nums);
    }
    else if (short_name == "hash")
    {
        Poco::StringTokenizer expr_list(options_.hash_exprs, ",");
        std::vector<size_t> hash_fields;
        for (const auto & expr : expr_list)
        {
            hash_fields.push_back(std::stoi(expr));
        }
        partitioner = std::make_unique<HashSelectorBuilder>(options.partition_nums, hash_fields, "cityHash64");
    }
    else if (short_name == "single")
    {
        options.partition_nums = 1;
        partitioner = std::make_unique<RoundRobinSelectorBuilder>(options.partition_nums);
    }
    else if (short_name == "range")
    {
        partitioner = std::make_unique<RangeSelectorBuilder>(options.hash_exprs, options.partition_nums);
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "unsupported splitter {}", short_name);
    }

    Poco::StringTokenizer output_column_tokenizer(options_.out_exprs, ",");
    for (const auto & iter : output_column_tokenizer)
    {
        output_columns_indicies.push_back(std::stoi(iter));
    }
    if (rss_pusher)
    {
        GET_JNIENV(env)
        jclass celeborn_partition_pusher_class =
            CreateGlobalClassReference(env, "Lorg/apache/spark/shuffle/CelebornPartitionPusher;");
        jmethodID celeborn_push_partition_data_method =
            GetMethodID(env, celeborn_partition_pusher_class, "pushPartitionData", "(I[BI)I");
        CLEAN_JNIENV
        auto celeborn_client = std::make_unique<CelebornClient>(rss_pusher, celeborn_push_partition_data_method);
        partition_writer = std::make_unique<CelebornPartitionWriter>(this, std::move(celeborn_client));
    }
    else
    {
        partition_writer = std::make_unique<LocalPartitionWriter>(this);
    }
    split_result.partition_length.resize(options.partition_nums, 0);
    split_result.raw_partition_length.resize(options.partition_nums, 0);
}


void CachedShuffleWriter::split(DB::Block & block)
{
    initOutputIfNeeded(block);

    Stopwatch compute_pid_time_watch;
    compute_pid_time_watch.start();
    partition_info = partitioner->build(block);
    split_result.total_compute_pid_time += compute_pid_time_watch.elapsedNanoseconds();

    DB::Block out_block;
    for (size_t col = 0; col < output_header.columns(); ++col)
    {
        out_block.insert(block.getByPosition(output_columns_indicies[col]));
    }

    partition_writer->write(partition_info, out_block);

    if (options.spill_threshold > 0 && partition_writer->totalCacheSize() > options.spill_threshold)
    {
        partition_writer->evictPartitions();
    }
}

void CachedShuffleWriter::initOutputIfNeeded(Block & block)
{
    if (output_header.columns() == 0) [[unlikely]]
    {
        output_header = block.cloneEmpty();
        if (output_columns_indicies.empty())
        {
            output_header = block.cloneEmpty();
            for (size_t i = 0; i < block.columns(); ++i)
            {
                output_columns_indicies.push_back(i);
            }
        }
        else
        {
            ColumnsWithTypeAndName cols;
            for (const auto & index : output_columns_indicies)
            {
                cols.push_back(block.getByPosition(index));
            }
            output_header = DB::Block(cols);
        }
    }
}
SplitResult CachedShuffleWriter::stop()
{
    partition_writer->stop();
    return split_result;
}
size_t CachedShuffleWriter::evictPartitions()
{
    auto size = partition_writer->totalCacheSize();
    if (size)
        partition_writer->evictPartitions(true);
    return size;
}

}
