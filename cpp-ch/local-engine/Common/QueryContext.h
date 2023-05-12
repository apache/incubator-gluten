#pragma once
#include <unordered_map>
#include <Interpreters/Context_fwd.h>
#include <jni/ReservationListenerWrapper.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

namespace local_engine
{
int64_t initializeQuery(ReservationListenerWrapperPtr listener);

void releaseAllocator(int64_t allocator_id);

int64_t allocatorMemoryUsage(int64_t allocator_id);

struct NativeAllocatorContext
{
    std::shared_ptr<DB::CurrentThread::QueryScope> query_scope;
    std::shared_ptr<DB::ThreadStatus> thread_status;
    DB::ContextPtr query_context;
    ReservationListenerWrapperPtr listener;
};

using NativeAllocatorContextPtr = std::shared_ptr<NativeAllocatorContext>;

NativeAllocatorContextPtr getAllocator(int64_t allocator);
}
