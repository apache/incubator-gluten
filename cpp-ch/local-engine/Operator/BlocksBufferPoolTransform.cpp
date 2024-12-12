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
#include "BlocksBufferPoolTransform.h"
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }};
}

BlocksBufferPoolTransform::BlocksBufferPoolTransform(const DB::Block & header, size_t buffer_size_)
    : DB::IProcessor({header}, {header})
    , buffer_size(buffer_size_)
{
}

DB::IProcessor::Status BlocksBufferPoolTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();
    if (output.isFinished() || isCancelled())
    {
        input.close();
        return Status::Finished;
    }

    bool has_output = false;
    if (output.canPush() && !pending_chunks.empty())
    {
        output.push(std::move(pending_chunks.front()));
        pending_chunks.pop_front();
        has_output = true;
    }

    if (input.isFinished())
    {
        if (!pending_chunks.empty() || has_output)
        {
            return Status::PortFull;
        }
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (input.hasData())
    {
        pending_chunks.push_back(input.pull(true));
        if (pending_chunks.size() >= buffer_size)
        {
            return Status::PortFull;
        }
        return Status::Ready;
    }
    return Status::NeedData;
}

void BlocksBufferPoolTransform::work()
{
}

BlocksBufferPoolStep::BlocksBufferPoolStep(const DB::Block & input_header, size_t buffer_size_)
    : DB::ITransformingStep(input_header, input_header, getTraits())
    , buffer_size(buffer_size_)
{
}

void BlocksBufferPoolStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    auto build_transform= [&](DB::OutputPortRawPtrs outputs){
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto buffer_pool_op = std::make_shared<BlocksBufferPoolTransform>(output->getHeader(), buffer_size);
            new_processors.push_back(buffer_pool_op);
            DB::connect(*output, buffer_pool_op->getInputs().front());
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
}

void BlocksBufferPoolStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}

void BlocksBufferPoolStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

}
