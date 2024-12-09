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
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/RelMetric.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/BlockIterator.h>

namespace local_engine
{

struct SparkBuffer
{
    char * address;
    size_t size;
};

class LocalExecutor : public BlockIterator
{
public:
    static std::optional<LocalExecutor *> getCurrentExecutor()
    {
        if (current_executor)
            return std::optional(current_executor);
        return std::nullopt;
    }
    static void resetCurrentExecutor() { current_executor = nullptr; }
    LocalExecutor(DB::QueryPlanPtr query_plan, DB::QueryPipelineBuilderPtr pipeline, bool dump_pipeline_ = false);
    ~LocalExecutor();

    SparkRowInfoPtr next();
    DB::Block * nextColumnar();
    bool hasNext();

    bool fallbackMode() const;

    /// Stop execution, used when task receives shutdown command or executor receives SIGTERM signal
    void cancel() const;
    void setSinks(const std::function<void(DB::QueryPipelineBuilder &)> & setter) const;
    void execute();
    DB::Block getHeader();
    RelMetricPtr getMetric() const { return metric; }
    void setMetric(const RelMetricPtr & metric_) { metric = metric_; }
    void setExtraPlanHolder(std::vector<DB::QueryPlanPtr> & extra_plan_holder_) { extra_plan_holder = std::move(extra_plan_holder_); }

private:
    // In the case of fallback, there may be multiple native pipelines in one stage. Can determine whether a fallback has occurred by whether a LocalExecutor already exists.
    // Updated when the LocalExecutor is created and reset when the task ends
    static thread_local LocalExecutor * current_executor;
    std::unique_ptr<SparkRowInfo> writeBlockToSparkRow(const DB::Block & block) const;
    void initPullingPipelineExecutor();
    /// Dump processor runtime information to log
    std::string dumpPipeline() const;

    DB::QueryPipelineBuilderPtr query_pipeline_builder;
    DB::QueryPipeline query_pipeline;
    // executor for fallback or ResultTask
    std::unique_ptr<DB::PullingPipelineExecutor> executor = nullptr;
    // executor for ShuffleMapTask
    DB::PipelineExecutorPtr push_executor = nullptr;
    DB::Block header;
    bool dump_pipeline;
    std::unique_ptr<CHColumnToSparkRow> ch_column_to_spark_row;
    std::unique_ptr<SparkBuffer> spark_buffer;
    DB::QueryPlanPtr current_query_plan;
    RelMetricPtr metric;
    std::vector<DB::QueryPlanPtr> extra_plan_holder;
    bool fallback_mode = false;
};
}
