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

#include <base/unit.h>
#include <base/types.h>
#include <Interpreters/Context.h>

namespace local_engine
{
struct MemoryConfig
{
    inline static const String EXTRA_MEMORY_HARD_LIMIT = "extra_memory_hard_limit";
    inline static const String CH_TASK_MEMORY = "off_heap_per_task";
    inline static const String SPILL_MEM_RATIO = "spill_mem_ratio";

    size_t extra_memory_hard_limit = 0;
    size_t off_heap_per_task = 0;
    double spill_mem_ratio = 0.9;

    static MemoryConfig loadFromContext(DB::ContextPtr context)
    {
        MemoryConfig config;
        config.extra_memory_hard_limit = context->getConfigRef().getUInt64(EXTRA_MEMORY_HARD_LIMIT, 0);
        config.off_heap_per_task = context->getConfigRef().getUInt64(CH_TASK_MEMORY, 0);
        config.spill_mem_ratio = context->getConfigRef().getDouble(SPILL_MEM_RATIO, 0.9);
        return config;
    }
};

struct GraceMergingAggregateConfig
{
    inline static const String MAX_GRACE_AGGREGATE_MERGING_BUCKETS = "max_grace_aggregate_merging_buckets";
    inline static const String THROW_ON_OVERFLOW_GRACE_AGGREGATE_MERGING_BUCKETS = "throw_on_overflow_grace_aggregate_merging_buckets";
    inline static const String AGGREGATED_KEYS_BEFORE_EXTEND_GRACE_AGGREGATE_MERGING_BUCKETS = "aggregated_keys_before_extend_grace_aggregate_merging_buckets";
    inline static const String MAX_PENDING_FLUSH_BLOCKS_PER_GRACE_AGGREGATE_MERGING_BUCKET = "max_pending_flush_blocks_per_grace_aggregate_merging_bucket";
    inline static const String MAX_ALLOWED_MEMORY_USAGE_RATIO_FOR_AGGREGATE_MERGING = "max_allowed_memory_usage_ratio_for_aggregate_merging";

    size_t max_grace_aggregate_merging_buckets = 32;
    bool throw_on_overflow_grace_aggregate_merging_buckets = false;
    size_t aggregated_keys_before_extend_grace_aggregate_merging_buckets = 8192;
    size_t max_pending_flush_blocks_per_grace_aggregate_merging_bucket = 1_MiB;
    double max_allowed_memory_usage_ratio_for_aggregate_merging = 0.9;

    static GraceMergingAggregateConfig loadFromContext(DB::ContextPtr context)
    {
        GraceMergingAggregateConfig config;
        config.max_grace_aggregate_merging_buckets = context->getConfigRef().getUInt64(MAX_GRACE_AGGREGATE_MERGING_BUCKETS, 32);
        config.throw_on_overflow_grace_aggregate_merging_buckets = context->getConfigRef().getBool(THROW_ON_OVERFLOW_GRACE_AGGREGATE_MERGING_BUCKETS, false);
        config.aggregated_keys_before_extend_grace_aggregate_merging_buckets = context->getConfigRef().getUInt64(AGGREGATED_KEYS_BEFORE_EXTEND_GRACE_AGGREGATE_MERGING_BUCKETS, 8192);
        config.max_pending_flush_blocks_per_grace_aggregate_merging_bucket = context->getConfigRef().getUInt64(MAX_PENDING_FLUSH_BLOCKS_PER_GRACE_AGGREGATE_MERGING_BUCKET, 1_MiB);
        config.max_allowed_memory_usage_ratio_for_aggregate_merging = context->getConfigRef().getDouble(MAX_ALLOWED_MEMORY_USAGE_RATIO_FOR_AGGREGATE_MERGING, 0.9);
        return config;
    }
};

struct StreamingAggregateConfig
{
    inline static const String AGGREGATED_KEYS_BEFORE_STREAMING_AGGREGATING_EVICT = "aggregated_keys_before_streaming_aggregating_evict";
    inline static const String MAX_MEMORY_USAGE_RATIO_FOR_STREAMING_AGGREGATING = "max_memory_usage_ratio_for_streaming_aggregating";
    inline static const String HIGH_CARDINALITY_THRESHOLD_FOR_STREAMING_AGGREGATING = "high_cardinality_threshold_for_streaming_aggregating";
    inline static const String ENABLE_STREAMING_AGGREGATING = "enable_streaming_aggregating";

    size_t aggregated_keys_before_streaming_aggregating_evict = 1024;
    double max_memory_usage_ratio_for_streaming_aggregating = 0.9;
    double high_cardinality_threshold_for_streaming_aggregating = 0.8;
    bool enable_streaming_aggregating = true;

    static StreamingAggregateConfig loadFromContext(DB::ContextPtr context)
    {
        StreamingAggregateConfig config;
        config.aggregated_keys_before_streaming_aggregating_evict = context->getConfigRef().getUInt64(AGGREGATED_KEYS_BEFORE_STREAMING_AGGREGATING_EVICT, 1024);
        config.max_memory_usage_ratio_for_streaming_aggregating = context->getConfigRef().getDouble(MAX_MEMORY_USAGE_RATIO_FOR_STREAMING_AGGREGATING, 0.9);
        config.high_cardinality_threshold_for_streaming_aggregating = context->getConfigRef().getDouble(HIGH_CARDINALITY_THRESHOLD_FOR_STREAMING_AGGREGATING, 0.8);
        config.enable_streaming_aggregating = context->getConfigRef().getBool(ENABLE_STREAMING_AGGREGATING, true);
        return config;
    }
};

struct JoinConfig
{
    /// If the join condition is like `t1.k = t2.k and (t1.id1 = t2.id2 or t1.id2 = t2.id2)`, try to join with multi
    /// join on clauses `(t1.k = t2.k and t1.id1 = t2.id2) or (t1.k = t2.k or t1.id2 = t2.id2)`
    inline static const String PREFER_MULTI_JOIN_ON_CLAUSES = "prefer_multi_join_on_clauses";
    /// Only hash join supports multi join on clauses, the right table cannot be too large. If the row number of right
    /// table is larger then this limit, this transform will not work.
    inline static const String MULTI_JOIN_ON_CLAUSES_BUILD_SIDE_ROWS_LIMIT = "multi_join_on_clauses_build_side_row_limit";

    bool prefer_multi_join_on_clauses = true;
    size_t multi_join_on_clauses_build_side_rows_limit = 10000000;

    static JoinConfig loadFromContext(DB::ContextPtr context)
    {
        JoinConfig config;
        config.prefer_multi_join_on_clauses = context->getConfigRef().getBool(PREFER_MULTI_JOIN_ON_CLAUSES, true);
        config.multi_join_on_clauses_build_side_rows_limit = context->getConfigRef().getUInt64(MULTI_JOIN_ON_CLAUSES_BUILD_SIDE_ROWS_LIMIT, 10000000);
        return config;
    }
};

struct ExecutorConfig
{
    inline static const String DUMP_PIPELINE = "dump_pipeline";
    inline static const String USE_LOCAL_FORMAT = "use_local_format";

    bool dump_pipeline = false;
    bool use_local_format = false;

    static ExecutorConfig loadFromContext(DB::ContextPtr context)
    {
        ExecutorConfig config;
        config.dump_pipeline = context->getConfigRef().getBool(DUMP_PIPELINE, false);
        config.use_local_format = context->getConfigRef().getBool(USE_LOCAL_FORMAT, false);
        return config;
    }
};

struct HdfsConfig
{
    inline static const String HDFS_ASYNC = "hdfs.enable_async_io";

    bool hdfs_async = true;

    static HdfsConfig loadFromContext(DB::ContextPtr context)
    {
        HdfsConfig config;
        config.hdfs_async = context->getConfigRef().getBool(HDFS_ASYNC, true);
        return config;
    }
};

struct S3Config
{
    inline static const String S3_LOCAL_CACHE_ENABLE = "s3.local_cache.enabled";
    inline static const String S3_LOCAL_CACHE_MAX_SIZE = "s3.local_cache.max_size";
    inline static const String S3_LOCAL_CACHE_CACHE_PATH = "s3.local_cache.cache_path";
    inline static const String S3_GCS_ISSUE_COMPOSE_REQUEST = "s3.gcs_issue_compose_request";

    bool s3_local_cache_enabled = false;
    size_t s3_local_cache_max_size = 100_GiB;
    String s3_local_cache_cache_path = "";
    bool s3_gcs_issue_compose_request = false;

    static S3Config loadFromContext(DB::ContextPtr context)
    {
        S3Config config;
        config.s3_local_cache_enabled = context->getConfigRef().getBool(S3_LOCAL_CACHE_ENABLE, false);
        config.s3_local_cache_max_size = context->getConfigRef().getUInt64(S3_LOCAL_CACHE_MAX_SIZE, 100_GiB);
        config.s3_local_cache_cache_path = context->getConfigRef().getString(S3_LOCAL_CACHE_CACHE_PATH, "");
        config.s3_gcs_issue_compose_request = context->getConfigRef().getBool(S3_GCS_ISSUE_COMPOSE_REQUEST, false);
        return config;
    }
};

struct MergeTreeConfig
{
    inline static const String TABLE_PART_METADATA_CACHE_MAX_COUNT = "table_part_metadata_cache_max_count";
    inline static const String TABLE_METADATA_CACHE_MAX_COUNT = "table_metadata_cache_max_count";

    size_t table_part_metadata_cache_max_count = 1000;
    size_t table_metadata_cache_max_count = 100;

    static MergeTreeConfig loadFromContext(DB::ContextPtr context)
    {
        MergeTreeConfig config;
        config.table_part_metadata_cache_max_count = context->getConfigRef().getUInt64(TABLE_PART_METADATA_CACHE_MAX_COUNT, 1000);
        config.table_metadata_cache_max_count = context->getConfigRef().getUInt64(TABLE_METADATA_CACHE_MAX_COUNT, 100);
        return config;
    }
};
}

