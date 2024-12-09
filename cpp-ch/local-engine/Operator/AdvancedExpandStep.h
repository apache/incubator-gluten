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

#include <Core/Block.h>
#include <Core/Names.h>
#include <Interpreters/AggregateDescription.h>
#include <Parser/ExpandField.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace local_engine
{
// This step is used when we move the expand operator after the partial aggregator.
// To avoid increasing the overhead of shuffle when some of the grouping keys are high cardinality, we add an extra aggregate operator after
// this expand operator and aggregate the low cardinality grouping keys.
class AdvancedExpandStep : public DB::ITransformingStep
{
public:
    explicit AdvancedExpandStep(
        DB::ContextPtr context_,
        const DB::Block & input_header_,
        size_t grouping_keys_,
        const DB::AggregateDescriptions & aggregate_descriptions_,
        const ExpandField & project_set_exprs_);
    ~AdvancedExpandStep() override = default;

    String getName() const override { return "AdvancedExpandStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override;

    static DB::Block buildOutputHeader(const DB::Block & header, const ExpandField & project_set_exprs_);

protected:
    DB::ContextPtr context;
    size_t grouping_keys;
    DB::AggregateDescriptions aggregate_descriptions;
    ExpandField project_set_exprs;

    void updateOutputHeader() override;
};

class AdvancedExpandTransform : public DB::IProcessor
{
public:
    using Status = DB::IProcessor::Status;
    /// Need to ensure that the input header is [grouping keys] ++ [aggregation columns]
    explicit AdvancedExpandTransform(
        const DB::Block & inpput_header_, const DB::Block & output_header_, size_t goruping_keys_, const ExpandField & project_set_exprs_);
    ~AdvancedExpandTransform() override = default;

    Status prepare() override;
    void work() override;
    String getName() const override { return "AdvancedExpandTransform"; }

private:
    size_t grouping_keys = 0;
    ExpandField project_set_exprs;
    DB::Block input_header;
    bool has_input = false;
    bool has_output = false;
    size_t expand_expr_iterator = 0;
    std::vector<bool> need_to_aggregate;

    std::vector<DB::OutputPort *> output_ports;

    DB::Chunk input_chunk;
    DB::Chunk output_chunk;

    size_t input_blocks = 0;
    size_t input_rows = 0;
    std::vector<size_t> output_blocks = {0, 0};
    std::vector<size_t> output_rows = {0, 0};

    void expandInputChunk();
};

}
