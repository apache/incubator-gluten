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

#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}
}


namespace local_engine
{

SubstraitFileSourceStep::SubstraitFileSourceStep(DB::ContextPtr context_, DB::Pipe pipe_, const String &)
    : SourceStepWithFilter(DB::DataStream{.header = pipe_.getHeader()}), pipe(std::move(pipe_)), context(context_)
{
}

void SubstraitFileSourceStep::initializePipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void SubstraitFileSourceStep::applyFilters()
{
    for (const auto & processor : pipe.getProcessors())
    {
        if (auto * source = dynamic_cast<DB::SourceWithKeyCondition *>(processor.get()))
            source->setKeyCondition(filter_nodes.nodes, context);
    }
}
}
