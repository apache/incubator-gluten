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
#include <Common/ConcurrentMap.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>


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
thread_local std::shared_ptr<CurrentThread::QueryScope> query_scope;
thread_local std::shared_ptr<ThreadStatus> thread_status;
ConcurrentMap<int64_t, NativeAllocatorContextPtr> allocator_map;

int64_t initializeQuery(ReservationListenerWrapperPtr listener)
{
    if (thread_status) return -1;
    auto query_context = Context::createCopy(SerializedPlanParser::global_context);
    query_context->makeQueryContext();

    // empty input will trigger random query id to be set
    // FileCache will check if query id is set to decide whether to skip cache or not
    // check FileCache.isQueryInitialized()
    //
    // Notice:
    // this generated random query id a qualified global queryid for the spark query
    query_context->setCurrentQueryId("");

    auto allocator_context = std::make_shared<NativeAllocatorContext>();
    allocator_context->thread_status = std::make_shared<ThreadStatus>(true);
    allocator_context->query_scope = std::make_shared<CurrentThread::QueryScope>(query_context);
    allocator_context->group = std::make_shared<ThreadGroup>(query_context);
    allocator_context->query_context = query_context;
    allocator_context->listener = listener;
    thread_status = allocator_context->thread_status;
    query_scope = allocator_context->query_scope;
    auto allocator_id = reinterpret_cast<int64_t>(allocator_context.get());
    CurrentMemoryTracker::before_alloc = [listener](Int64 size, bool throw_if_memory_exceed) -> void
    {
        if (throw_if_memory_exceed)
            listener->reserveOrThrow(size);
        else
            listener->reserve(size);
    };
    CurrentMemoryTracker::before_free = [listener](Int64 size) -> void { listener->free(size); };
    allocator_map.insert(allocator_id, allocator_context);
    return allocator_id;
}

void releaseAllocator(int64_t allocator_id)
{
    if (!allocator_map.get(allocator_id))
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "allocator {} not found", allocator_id);
    }
    auto status = allocator_map.get(allocator_id)->thread_status;
    status->detachFromGroup();
    auto listener = allocator_map.get(allocator_id)->listener;
    if (status->untracked_memory < 0)
        listener->free(-status->untracked_memory);
    else if (status->untracked_memory > 0)
        listener->reserve(status->untracked_memory);
    allocator_map.erase(allocator_id);
    thread_status.reset();
    query_scope.reset();
}

NativeAllocatorContextPtr getAllocator(int64_t allocator)
{
    return allocator_map.get(allocator);
}

int64_t allocatorMemoryUsage(int64_t allocator_id)
{
    return allocator_map.get(allocator_id)->thread_status->memory_tracker.get();
}

}
