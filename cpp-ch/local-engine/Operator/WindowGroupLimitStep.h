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
#include <vector>
#include <Core/Block.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace local_engine
{
class WindowGroupLimitStep : public DB::ITransformingStep
{
public:
    explicit WindowGroupLimitStep(
        const DB::Block & input_header_,
        const String & function_name_,
        const std::vector<size_t> & partition_columns_,
        const std::vector<size_t> & sort_columns_,
        size_t limit_);
    ~WindowGroupLimitStep() override = default;

    String getName() const override { return "WindowGroupLimitStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override;
    void updateOutputHeader() override;

private:
    // window function name, one of row_number, rank and dense_rank
    String function_name;
    std::vector<size_t> partition_columns;
    std::vector<size_t> sort_columns;
    size_t limit;
};

}
