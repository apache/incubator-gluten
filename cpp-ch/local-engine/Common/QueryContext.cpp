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
#include "QueryContext.h"

#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/CHUtil.h>
#include <Common/GlutenConfig.h>
#include <Common/ConcurrentMap.h>
#include <base/unit.h>
#include <sstream>
#include <iomanip>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
using namespace DB;

struct QueryContext
{
    std::shared_ptr<ThreadStatus> thread_status;
    std::shared_ptr<ThreadGroup> thread_group;
    ContextMutablePtr query_context;
};

ConcurrentMap<int64_t, std::shared_ptr<QueryContext>> query_map;

int64_t QueryContextManager::initializeQuery()
{
    std::shared_ptr<QueryContext> query_context = std::make_shared<QueryContext>();
    query_context->query_context = Context::createCopy(SerializedPlanParser::global_context);
    query_context->query_context->makeQueryContext();

    // empty input will trigger random query id to be set
    // FileCache will check if query id is set to decide whether to skip cache or not
    // check FileCache.isQueryInitialized()
    //
    // Notice:
    // this generated random query id a qualified global queryid for the spark query
    query_context->query_context->setCurrentQueryId(toString(UUIDHelpers::generateV4()));
    auto config = MemoryConfig::loadFromContext(query_context->query_context);
    query_context->thread_status = std::make_shared<ThreadStatus>(false);
    query_context->thread_group = std::make_shared<ThreadGroup>(query_context->query_context);
    CurrentThread::attachToGroup(query_context->thread_group);
    auto memory_limit = config.off_heap_per_task;

    query_context->thread_group->memory_tracker.setSoftLimit(memory_limit);
    query_context->thread_group->memory_tracker.setHardLimit(memory_limit + config.extra_memory_hard_limit);
    int64_t id = reinterpret_cast<int64_t>(query_context->thread_group.get());
    query_map.insert(id, query_context);
    return id;
}

DB::ContextMutablePtr QueryContextManager::currentQueryContext()
{
    auto thread_group = currentThreadGroup();
    int64_t id = reinterpret_cast<int64_t>(CurrentThread::getGroup().get());
    return query_map.get(id)->query_context;
}

std::shared_ptr<DB::ThreadGroup> QueryContextManager::currentThreadGroup()
{
    if (auto thread_group = CurrentThread::getGroup())
        return thread_group;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Thread group not found.");
}

void QueryContextManager::logCurrentPerformanceCounters(ProfileEvents::Counters & counters)
{
    if (!CurrentThread::getGroup())
    {
        return;
    }
    if (logger->information())
    {
        std::ostringstream msg;
        msg << "\n---------------------Task Performance Counters-----------------------------\n";
        for (ProfileEvents::Event event = ProfileEvents::Event(0); event < counters.num_counters; event++)
        {
            const auto * name = ProfileEvents::getName(event);
            const auto * doc = ProfileEvents::getDocumentation(event);
            auto & count = counters[event];
            if (count == 0)
                continue;
            msg << std::setw(50) << std::setfill(' ') << std::left << name << "|"
                << std::setw(20) << std::setfill(' ') << std::left << count.load()
                << " | (" << doc << ")\n";
        }
        LOG_INFO(logger, "{}", msg.str());
    }
}

size_t QueryContextManager::currentPeakMemory(int64_t id)
{
    if (!query_map.contains(id))
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "context released {}", id);
    return query_map.get(id)->thread_group->memory_tracker.getPeak();
}

void QueryContextManager::finalizeQuery(int64_t id)
{
    if (!CurrentThread::getGroup())
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Thread group not found.");
    }
    std::shared_ptr<QueryContext> context;
    {
        context = query_map.get(id);
    }
    auto query_context = context->thread_status->getQueryContext();
    if (!query_context)
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "query context not found");
    }
    context->thread_status->flushUntrackedMemory();
    context->thread_status->finalizePerformanceCounters();
    LOG_INFO(logger, "Task finished, peak memory usage: {} bytes", currentPeakMemory(id));

    if (currentThreadGroupMemoryUsage() > 1_MiB)
    {
        LOG_WARNING(logger, "{} bytes memory didn't release, There may be a memory leak!", currentThreadGroupMemoryUsage());
    }
    logCurrentPerformanceCounters(context->thread_group->performance_counters);
    context->thread_status->detachFromGroup();
    context->thread_group.reset();
    context->thread_status.reset();
    query_context.reset();
    {
        query_map.erase(id);
    }
}

size_t currentThreadGroupMemoryUsage()
{
    if (!CurrentThread::getGroup())
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Thread group not found, please call initializeQuery first.");
    }
    return CurrentThread::getGroup()->memory_tracker.get();
}

double currentThreadGroupMemoryUsageRatio()
{
    if (!CurrentThread::getGroup())
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Thread group not found, please call initializeQuery first.");
    }
    return static_cast<double>(CurrentThread::getGroup()->memory_tracker.get()) / CurrentThread::getGroup()->memory_tracker.getSoftLimit();
}
}