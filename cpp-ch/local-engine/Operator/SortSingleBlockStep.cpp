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

#include "SortSingleBlockStep.h"
#include <Processors/IProcessor.h>
#include <QueryPipeline/Pipe.h>

namespace local_engine
{
class SortSingleBlockTransform : public DB::IProcessor
{
public:
    using Status = DB::IProcessor::Status;
    explicit SortSingleBlockTransform(const DB::Block & header_, const DB::SortDescription & sort_desc_)
        : DB::IProcessor({header_}, {header_}), header(header_), sort_desc(sort_desc_)
    {
    }
    ~SortSingleBlockTransform() override = default;

    Status prepare() override
    {
        auto & output_port = outputs.front();
        auto & input_port = inputs.front();
        if (output_port.isFinished() || isCancelled())
        {
            input_port.close();
            return Status::Finished;
        }
        if (has_output)
        {
            if (output_port.canPush())
            {
                output_port.push(std::move(output_chunk));
                has_output = false;
            }
            return Status::PortFull;
        }

        if (has_input)
            return Status::Ready;

        if (input_port.isFinished())
        {
            output_port.finish();
            return Status::Finished;
        }

        input_port.setNeeded();
        if (input_port.hasData())
        {
            input_chunk = input_port.pull(true);
            has_input = true;
        }

        return has_input ? Status::Ready : Status::NeedData;
    }

    void work() override
    {
        if (!has_input)
            return;
        DB::Block block = header.cloneWithColumns(input_chunk.getColumns());
        DB::sortBlock(block, sort_desc);
        output_chunk = DB::Chunk(block.getColumns(), block.rows());
        has_input = false;
        has_output = true;
    }
    String getName() const override { return "SortSingleBlockTransform"; }

private:
    DB::Block header;
    DB::SortDescription sort_desc;
    bool has_input = false;
    DB::Chunk input_chunk;
    bool has_output = false;
    DB::Chunk output_chunk;
};

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

SortSingleBlockStep::SortSingleBlockStep(const DB::DataStream & input_stream_, const DB::SortDescription & sort_desc_)
    : DB::ITransformingStep(input_stream_, input_stream_.header, getTraits()), header(input_stream_.header), sort_desc(sort_desc_)
{
}

void SortSingleBlockStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
    auto build_transform = [&](DB::OutputPortRawPtrs outputs)
    {
        DB::Processors processors;
        for (auto & output : outputs)
        {
            auto processor = std::make_shared<SortSingleBlockTransform>(header, sort_desc);
            processors.push_back(processor);
            DB::connect(*output, processor->getInputs().front());
        }
        return processors;
    };
    return pipeline.transform(build_transform);
}

void SortSingleBlockStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}

void SortSingleBlockStep::updateOutputStream()
{
    createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
}

}
