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
#include <unordered_map>
#include <Core/Block.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{
class GraceMergingAggregatedStep : public DB::ITransformingStep
{
public:
    explicit GraceMergingAggregatedStep(
        DB::ContextPtr context_,
        const DB::DataStream & input_stream_,
        DB::Aggregator::Params params_);
    ~GraceMergingAggregatedStep() override = default;

    String getName() const override { return "GraceMergingAggregatedStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &) override;

    void describeActions(DB::JSONBuilder::JSONMap & map) const override;
    void describeActions(DB::IQueryPlanStep::FormatSettings & settings) const override;
private:
    DB::ContextPtr context;
    DB::Aggregator::Params params;
    void updateOutputStream() override; 
};

class GraceMergingAggregatedTransform : public DB::IProcessor
{
public:
    static constexpr size_t max_buckets = 32;
    using Status = DB::IProcessor::Status;
    explicit GraceMergingAggregatedTransform(const DB::Block &header_, DB::AggregatingTransformParamsPtr params_, DB::ContextPtr context_);
    ~GraceMergingAggregatedTransform() override;

    Status prepare() override;
    void work() override;
    String getName() const override { return "GraceMergingAggregatedTransform"; }
private:
    DB::Block header;
    DB::AggregatingTransformParamsPtr params;
    DB::ContextPtr context;
    DB::TemporaryDataOnDiskPtr tmp_data_disk;
    DB::AggregatedDataVariantsPtr current_data_variants = nullptr;
    size_t current_bucket_index = 0;

    struct BufferFileStream
    {
        std::list<DB::Block> blocks;
        DB::TemporaryFileStream * file_stream = nullptr;
    };
    std::unordered_map<size_t, BufferFileStream> buckets;

    size_t getBucketsNum() const { return buckets.size(); }
    void extendBuckets();
    void rehashDataVariants();
    DB::Blocks scatterBlock(const DB::Block & block);
    void addBlockIntoFileBucket(size_t bucket_index, const DB::Block & block);
    void flushBuckets();
    size_t flushBucket(size_t bucket_index);
    void prepareBucketOutputBlocks();
    void mergeOneBlock(const DB::Block &block);
    size_t getMemoryUsage();
    bool isMemoryOverflow();

    bool input_finished = false;
    bool has_input = false;
    DB::Chunk input_chunk;
    bool has_output = false;
    DB::Chunk output_chunk;
    DB::BlocksList current_final_blocks;
    bool no_more_keys = false;

    double per_key_memory_usage = 0;

    // metrics
    size_t total_input_blocks = 0;
    size_t total_input_rows = 0;
    size_t total_output_blocks = 0;
    size_t total_output_rows = 0;
    size_t total_spill_disk_bytes = 0;
    size_t total_spill_disk_time = 0;
    size_t total_read_disk_time = 0;
    size_t total_scatter_time = 0;

    Poco::Logger * logger = &Poco::Logger::get("GraceMergingAggregatedTransform");
};
}
