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
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <ranges>

#include <jni/jni_common.h>

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

jclass CacheManager::cache_result_class = nullptr;
jmethodID CacheManager::cache_result_constructor = nullptr;

void CacheManager::initJNI(JNIEnv * env)
{
    cache_result_class = CreateGlobalClassReference(env, "Lorg/apache/gluten/execution/CacheResult;");
    cache_result_constructor = GetMethodID(env, cache_result_class, "<init>", "(ILjava/lang/String;)V");
}

CacheManager & CacheManager::instance()
{
    static CacheManager cache_manager;
    return cache_manager;
}

void CacheManager::initialize(DB::ContextMutablePtr context_)
{
    auto & manager = instance();
    manager.context = context_;
}

struct CacheJobContext
{
    MergeTreeTable table;
};

Task CacheManager::cachePart(const MergeTreeTable& table, const MergeTreePart& part, const std::unordered_set<String> & columns)
{
    CacheJobContext job_context{table};
    job_context.table.parts.clear();
    job_context.table.parts.push_back(part);
    job_context.table.snapshot_id = "";
    Task task = [job_detail = job_context, context = this->context, read_columns = columns]()
    {
        try
        {
            auto storage = MergeTreeRelParser::parseStorage(job_detail.table, context, true);
            auto storage_snapshot = std::make_shared<StorageSnapshot>(*storage, storage->getInMemoryMetadataPtr());
            NamesAndTypesList names_and_types_list;
            auto meta_columns = storage->getInMemoryMetadata().getColumns();
            for (const auto & column : meta_columns)
            {
                if (read_columns.contains(column.name))
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
                context->getSettingsRef().max_block_size,
                1);
            QueryPlan plan;
            plan.addStep(std::move(read_step));
            auto pipeline_builder = plan.buildQueryPipeline({}, {});
            auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder.get()));
            PullingPipelineExecutor executor(pipeline);
            while (true)
            {
                if (Chunk chunk; !executor.pull(chunk))
                    break;
            }
            LOG_INFO(getLogger("CacheManager"), "Load cache of table {}.{} part {} success.", job_detail.table.database, job_detail.table.table, job_detail.table.parts.front().name);
        }
        catch (std::exception& e)
        {
            LOG_ERROR(getLogger("CacheManager"), "Load cache of table {}.{} part {} failed.\n {}", job_detail.table.database, job_detail.table.table, job_detail.table.parts.front().name, e.what());
            std::rethrow_exception(std::current_exception());
        }
    };
    LOG_INFO(getLogger("CacheManager"), "Loading cache of table {}.{} part {}", job_context.table.database, job_context.table.table, job_context.table.parts.front().name);
    return std::move(task);
}

JobId CacheManager::cacheParts(const String& table_def, const std::unordered_set<String>& columns)
{
    auto table = parseMergeTreeTableString(table_def);
    JobId id = toString(UUIDHelpers::generateV4());
    Job job(id);
    for (const auto & part : table.parts)
    {
        job.addTask(cachePart(table, part, columns));
    }
    auto& scheduler = JobScheduler::instance();
    scheduler.scheduleJob(std::move(job));
    return id;
}

jobject CacheManager::getCacheStatus(JNIEnv * env, const String & jobId)
{
    auto& scheduler = JobScheduler::instance();
    auto job_status = scheduler.getJobSatus(jobId);
    int status = 0;
    String message;
    if (job_status.has_value())
    {
        switch (job_status.value().status)
        {
            case JobSatus::RUNNING:
                status = 0;
                break;
            case JobSatus::FINISHED:
                status = 1;
                break;
            case JobSatus::FAILED:
                status = 2;
                for (const auto & msg : job_status->messages)
                {
                    message.append(msg);
                    message.append(";");
                }
                break;
        }
    }
    else
    {
        status = 2;
        message = fmt::format("job {} not found", jobId);
    }
    return env->NewObject(cache_result_class, cache_result_constructor, status, charTojstring(env, message.c_str()));
}
}