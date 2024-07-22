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
#include "CacheManager.h"

#include <Core/Settings.h>
#include <Disks/IStoragePolicy.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Storages/Mergetree/MetaDataHelper.h>
#include <Common/ThreadPool.h>
#include <Parser/MergeTreeRelParser.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Sinks/NullSink.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_STATE;
}
}

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
extern const Metric LocalThreadScheduled;
}

namespace local_engine
{
CacheManager & CacheManager::instance()
{
    static CacheManager cache_manager;
    return cache_manager;
}

void CacheManager::initialize(DB::ContextMutablePtr context_)
{
    auto & manager = instance();
    manager.context = context_;
    manager.thread_pool = std::make_unique<ThreadPool>(
        CurrentMetrics::LocalThread,
        CurrentMetrics::LocalThreadActive,
        CurrentMetrics::LocalThreadScheduled,
        manager.context->getConfigRef().getInt("cache_sync_max_threads", 10),
        0,
        0);
}

struct CacheJobContext
{
    MergeTreeTable table;
};

void CacheManager::cachePart(const MergeTreeTable& table, const MergeTreePart& part, const std::unordered_set<String> & columns, std::shared_ptr<std::latch> latch)
{
    CacheJobContext job_context{table};
    job_context.table.parts.clear();
    job_context.table.parts.push_back(part);
    job_context.table.snapshot_id = "";
    auto job = [job_detail = job_context, context = this->context, columns = columns, latch = latch]()
    {
        try
        {
            SCOPE_EXIT({ if (latch) latch->count_down();});
            auto storage = MergeTreeRelParser::parseStorage(job_detail.table, context, true);
            auto storage_snapshot = std::make_shared<StorageSnapshot>(*storage, storage->getInMemoryMetadataPtr());
            NamesAndTypesList names_and_types_list;
            for (const auto & column : storage->getInMemoryMetadata().getColumns())
            {
                if (columns.contains(column.name))
                    names_and_types_list.push_back(NameAndTypePair(column.name, column.type));
            }

            auto query_info = buildQueryInfo(names_and_types_list);

            std::vector<DataPartPtr> selected_parts
                = StorageMergeTreeFactory::getDataPartsByNames(storage->getStorageID(), "", {job_detail.table.parts.front().name});

            auto read_step = storage->reader.readFromParts(
                selected_parts,
                /* alter_conversions = */
                {},
                names_and_types_list.getNames(),
                storage_snapshot,
                *query_info,
                context,
                context->getSettings().max_block_size,
                1);
            QueryPlan plan;
            plan.addStep(std::move(read_step));
            auto pipeline_builder = plan.buildQueryPipeline({}, {});
            pipeline_builder->setSinks([&](const auto & header, auto ) {return std::make_shared<NullSink>(header);});
            auto executor = pipeline_builder->execute();
            executor->execute(1, true);
        }
        catch (std::exception& e)
        {
            LOG_ERROR(getLogger("CacheManager"), "Load cache of table {}.{} part {} failed.\n {}", job_detail.table.database, job_detail.table.table, job_detail.table.parts.front().name, e.what());
        }
    };
    LOG_INFO(getLogger("CacheManager"), "Loading cache of table {}.{} part {}", job_context.table.database, job_context.table.table, job_context.table.parts.front().name);
    thread_pool->scheduleOrThrowOnError(std::move(job));
}

void CacheManager::cacheParts(const String& table_def, const std::unordered_set<String>& columns, bool async)
{
    auto table = parseMergeTreeTableString(table_def);
    std::shared_ptr<std::latch> latch = nullptr;
    if (!async) latch = std::make_shared<std::latch>(table.parts.size());
    for (const auto & part : table.parts)
    {
        cachePart(table, part, columns, latch);
    }
    if (latch)
        latch->wait();
}
}