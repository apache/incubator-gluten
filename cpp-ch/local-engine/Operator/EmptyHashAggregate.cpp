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
#include "EmptyHashAggregate.h"
#include <memory>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Operator/ExpandTransorm.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Poco/Logger.h>
#include <Common/CHUtil.h>
#include <Common/logger_useful.h>

namespace local_engine
{
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

/// Always return a block with one row. Don't care what is in it.
class EmptyHashAggregate : public DB::IProcessor
{
public:
    explicit EmptyHashAggregate(const DB::Block & input_) : DB::IProcessor({input_}, {BlockUtil::buildRowCountHeader()}) { }
    ~EmptyHashAggregate() override = default;

    Status prepare() override
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
                has_outputed = true;
                return Status::PortFull;
            }
        }

        if (input.isFinished())
        {
            if (has_outputed)
            {
                output.finish();
                return Status::Finished;
            }
            if (!has_output)
            {
                output_chunk = BlockUtil::buildRowCountChunk(1);
                has_output = true;
            }
            return Status::Ready;
        }

        input.setNeeded();
        if (input.hasData())
        {
            (void)input.pullData(true);
            return Status::Ready;
        }
        return Status::NeedData;
    }
    void work() override { }

    DB::String getName() const override { return "EmptyHashAggregate"; }

private:
    bool has_outputed = false;
    bool has_output = false;
    DB::Chunk output_chunk;
};

EmptyHashAggregateStep::EmptyHashAggregateStep(const DB::DataStream & input_stream_)
    : DB::ITransformingStep(input_stream_, BlockUtil::buildRowCountHeader(), getTraits())
{
}

void EmptyHashAggregateStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    auto build_transform = [&](DB::OutputPortRawPtrs outputs)
    {
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto op = std::make_shared<EmptyHashAggregate>(output->getHeader());
            new_processors.push_back(op);
            DB::connect(*output, op->getInputs().front());
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
}

void EmptyHashAggregateStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}

void EmptyHashAggregateStep::updateOutputStream()
{
    createOutputStream(input_streams.front(), BlockUtil::buildRowCountHeader(), getDataStreamTraits());
}
}
