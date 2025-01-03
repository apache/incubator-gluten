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

#include "BranchStep.h"
#include <memory>
#include <Interpreters/Context_fwd.h>
#include <Operator/BranchTransform.h>
#include <Processors/IProcessor.h>
#include <Processors/ISource.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.cpp>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{

class BranchHookSource : public DB::IProcessor
{
public:
    using Status = DB::IProcessor::Status;
    BranchHookSource(const DB::Block & header_) : DB::IProcessor({}, {header_}) { inner_inputs.emplace_back(header_, this); }
    ~BranchHookSource() override = default;

    String getName() const override { return "BranchHookSource"; }

    Status prepare() override;
    void work() override;
    void enableInputs() { inputs.swap(inner_inputs); }

private:
    DB::InputPorts inner_inputs;
    bool has_output = false;
    DB::Chunk output_chunk;
    bool has_input = false;
    DB::Chunk input_chunk;
};

BranchHookSource::Status BranchHookSource::prepare()
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
            output.push(std::move(output_chunk));
            has_output = false;
        }
        return Status::PortFull;
    }
    if (has_input)
        return Status::Ready;
    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }
    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    input_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void BranchHookSource::work()
{
    if (has_input)
    {
        output_chunk = std::move(input_chunk);
        has_output = true;
        has_input = false;
    }
}


static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

class ResizeStep : public DB::ITransformingStep
{
public:
    explicit ResizeStep(const DB::Block & header_, size_t num_streams_)
        : DB::ITransformingStep(header_, header_, getTraits()), num_streams(num_streams_)
    {
    }
    ~ResizeStep() override = default;

    String getName() const override { return "UniteBranchesStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &) override
    {
        LOG_ERROR(getLogger("ResizeStep"), "xxx num_streams: {}", num_streams);
        pipeline.resize(num_streams);
    }
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override
    {
        if (!processors.empty())
            DB::IQueryPlanStep::describePipeline(processors, settings);
    }

private:
    size_t num_streams;
    void updateOutputHeader() override { };
};

DB::QueryPlanPtr BranchStepHelper::createSubPlan(const DB::Block & header, size_t num_streams)
{
    auto source = std::make_unique<DB::ReadFromPreparedSource>(DB::Pipe(std::make_shared<BranchHookSource>(header)));
    source->setStepDescription("Hook node connected to one branch output");
    auto plan = std::make_unique<DB::QueryPlan>();
    plan->addStep(std::move(source));

    if (num_streams > 1)
    {
        auto resize_step = std::make_unique<ResizeStep>(plan->getCurrentHeader(), num_streams);
        plan->addStep(std::move(resize_step));
    }
    return std::move(plan);
}

StaticBranchStep::StaticBranchStep(
    const DB::ContextPtr & context_, const DB::Block & header_, size_t branches_, size_t sample_rows_, BranchSelector selector_)
    : DB::ITransformingStep(header_, header_, getTraits())
    , context(context_)
    , header(header_)
    , branches(branches_)
    , max_sample_rows(sample_rows_)
    , selector(selector_)
{
}

void StaticBranchStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings)
{
    auto build_transform = [&](const DB::OutputPortRawPtrs & child_outputs) -> DB::Processors
    {
        DB::Processors new_processors;
        for (auto & output : child_outputs)
        {
            if (!output)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Output port is null");
            auto branch_transform = std::make_shared<StaticBranchTransform>(header, max_sample_rows, branches, selector);
            DB::connect(*output, branch_transform->getInputs().front());
            new_processors.push_back(branch_transform);
        }
        return new_processors;
    };
    pipeline.resize(1);
    pipeline.transform(build_transform);
}

void StaticBranchStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}

void StaticBranchStep::updateOutputHeader()
{
}

UniteBranchesStep::UniteBranchesStep(
    const DB::ContextPtr & context_, const DB::Block & header_, std::vector<DB::QueryPlanPtr> && branch_plans_, size_t num_streams_)
    : DB::ITransformingStep(header_, branch_plans_[0]->getCurrentHeader(), getTraits()), context(context_), header(header_)
{
    branch_plans.swap(branch_plans_);
    size_t branches = branch_plans.size();
    num_streams = num_streams_;
}

void UniteBranchesStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
    auto add_transform = [&](DB::OutputPortRawPtrs child_outputs) -> DB::Processors
    {
        DB::Processors new_processors;
        size_t branch_index = 0;
        if (child_outputs.size() != branch_plans.size())
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Output port's size({}) is not equal to branches size({})",
                child_outputs.size(),
                branch_plans.size());
        }
        for (auto * output : child_outputs)
        {
            auto & branch_plan = branch_plans[branch_index];
            DB::QueryPlanOptimizationSettings optimization_settings{context};
            DB::BuildQueryPipelineSettings build_settings{context};
            DB::QueryPlanResourceHolder resource_holder;

            auto pipeline_builder = branch_plan->buildQueryPipeline(optimization_settings, build_settings);
            auto pipe = DB::QueryPipelineBuilder::getPipe(std::move(*pipeline_builder), resource_holder);
            DB::ProcessorPtr source_node = nullptr;
            auto processors = DB::Pipe::detachProcessors(std::move(pipe));
            for (auto processor : processors)
            {
                if (auto * source = typeid_cast<BranchHookSource *>(processor.get()))
                {
                    if (source->getInputs().empty())
                    {
                        if (source_node)
                            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "There is multi source in branch plan");
                        source->enableInputs();
                        source_node = processor;
                    }
                }
            }
            if (!source_node)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot find source node in branch plan");
            if (source_node->getInputs().empty())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Source node has no input");
            DB::connect(*output, source_node->getInputs().front());
            new_processors.insert(new_processors.end(), processors.begin(), processors.end());
            branch_index++;
        }
        return new_processors;
    };
    pipeline.transform(add_transform);
    pipeline.resize(1);
    if (num_streams > 1)
        pipeline.resize(num_streams);
}

void UniteBranchesStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}
}
