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

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
}
}


namespace local_engine
{
namespace
{
/*
 * As discussed with community, we need to create a dummy storage to be used in SubstraitFileSourceStep.
 */
class SubstraitFileStorage final : public DB::IStorage
{
public:
    explicit SubstraitFileStorage(const DB::StorageID & storage_id_) : IStorage(storage_id_) { }
    bool canMoveConditionsToPrewhere() const override { return false; }
    std::string getName() const override { return "SubstraitFile"; };
};
SubstraitFileStorage dummy_storage{DB::StorageID("dummy_db", "dummy_table")};

}

SubstraitFileSourceStep::SubstraitFileSourceStep(const DB::ContextPtr & context_, DB::Pipe pipe_, const String &)
    : SourceStepWithFilter(pipe_.getHeader(), {}, {}, dummy_storage.getStorageSnapshot(nullptr, nullptr), context_), pipe(std::move(pipe_))
{
}

void SubstraitFileSourceStep::initializePipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void SubstraitFileSourceStep::applyFilters(const DB::ActionDAGNodes added_filter_nodes)
{
    filter_actions_dag = DB::ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes);
    for (const auto & processor : pipe.getProcessors())
        if (auto * source = dynamic_cast<DB::SourceWithKeyCondition *>(processor.get()))
            source->setKeyCondition(filter_actions_dag, context);
}
}
