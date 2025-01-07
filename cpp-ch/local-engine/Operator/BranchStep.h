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

#include <cstddef>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace local_engine
{

class BranchStepHelper
{
public:
    // Create a new query plan that would be used to build sub branch query plan.
    static DB::QueryPlanPtr createSubPlan(const DB::Block & header, size_t num_streams);
};

// Use to branch the query plan.
class StaticBranchStep : public DB::ITransformingStep
{
public:
    using BranchSelector = std::function<size_t(const std::list<DB::Chunk> &)>;
    explicit StaticBranchStep(
        const DB::ContextPtr & context_, const DB::Block & header, size_t branches, size_t sample_rows, BranchSelector selector);
    ~StaticBranchStep() override = default;

    String getName() const override { return "StaticBranchStep"; }

    // This will resize the num_streams to 1. You may need to resize after this.
    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override;

protected:
    void updateOutputHeader() override;

private:
    DB::ContextPtr context;
    DB::Block header;
    size_t max_sample_rows;
    size_t branches;
    BranchSelector selector;
};


// It should be better to build execution branches on QueryPlan.
class UniteBranchesStep : public DB::ITransformingStep
{
public:
    explicit UniteBranchesStep(
        const DB::ContextPtr & context_, const DB::Block & header_, std::vector<DB::QueryPlanPtr> && branch_plans_, size_t num_streams_);
    ~UniteBranchesStep() override = default;

    String getName() const override { return "UniteBranchesStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipelines, const DB::BuildQueryPipelineSettings &) override;
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override;

private:
    DB::ContextPtr context;
    DB::Block header;
    std::vector<DB::QueryPlanPtr> branch_plans;
    size_t num_streams;

    void updateOutputHeader() override { output_header = header; };
};

}
