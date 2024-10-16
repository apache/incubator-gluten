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

#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/AggregatingTransform.h>


namespace local_engine
{
/**
 * GraceAggregatingStep is used for partial aggregating stages.
 */
class GraceAggregatingStep : public DB::ITransformingStep
{
public:
    explicit GraceAggregatingStep(
        DB::ContextPtr context_, const DB::Block & input_header, DB::Aggregator::Params params_, bool no_pre_aggregated_);
    ~GraceAggregatingStep() override = default;

    String getName() const override { return "GraceAggregatingStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &) override;

    void describeActions(DB::JSONBuilder::JSONMap & map) const override;
    void describeActions(DB::IQueryPlanStep::FormatSettings & settings) const override;

private:
    DB::ContextPtr context;
    DB::Aggregator::Params params;
    bool no_pre_aggregated;

protected:
    void updateOutputHeader() override;
};
}