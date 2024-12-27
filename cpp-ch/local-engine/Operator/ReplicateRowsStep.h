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

#include <Processors/ISimpleTransform.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace local_engine
{

class ReplicateRowsStep : public DB::ITransformingStep
{
public:
    ReplicateRowsStep(const DB::Block & input_header);

    static DB::Block transformHeader(const DB::Block & input);

    String getName() const override { return "ReplicateRowsStep"; }
    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;

private:
    void updateOutputHeader() override;
};

class ReplicateRowsTransform : public DB::ISimpleTransform
{
public:
    ReplicateRowsTransform(const DB::Block & input_header_);

    String getName() const override { return "ReplicateRowsTransform"; }
    void transform(DB::Chunk &) override;
};
}
