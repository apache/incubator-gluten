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

#include <list>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace local_engine
{
class BlocksBufferPoolStep : public DB::ITransformingStep
{
public:
    explicit BlocksBufferPoolStep(const DB::Block & input_header, size_t buffer_size_ = 4);
    ~BlocksBufferPoolStep() override = default;

    String getName() const override { return "BlocksBufferPoolStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override;
private:
    size_t buffer_size;
    void updateOutputHeader() override;
};

class BlocksBufferPoolTransform  : public DB::IProcessor
{
public:
    using Status = DB::IProcessor::Status;
    explicit BlocksBufferPoolTransform(const DB::Block & header, size_t buffer_size_ = 4);
    ~BlocksBufferPoolTransform() override = default;

    Status prepare() override;
    void work() override;

    String getName() const override { return "BlocksBufferPoolTransform"; }
private:
    std::list<DB::Chunk> pending_chunks;
    size_t buffer_size;
};
}
