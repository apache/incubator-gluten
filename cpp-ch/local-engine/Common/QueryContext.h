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
#include <Interpreters/Context_fwd.h>
#include <Common/ConcurrentMap.h>
#include <Common/ThreadStatus.h>

namespace local_engine
{
struct QueryContext;

class QueryContextManager
{
public:
    static QueryContextManager & instance()
    {
        static QueryContextManager instance;
        return instance;
    }
    int64_t initializeQuery();
    DB::ContextMutablePtr currentQueryContext();
    static std::shared_ptr<DB::ThreadGroup> currentThreadGroup();
    void logCurrentPerformanceCounters(ProfileEvents::Counters & counters) const;
    size_t currentPeakMemory(int64_t id);
    void finalizeQuery(int64_t id);

private:
    QueryContextManager() = default;
    LoggerPtr logger_ = getLogger("QueryContextManager");
    ConcurrentMap<int64_t, std::shared_ptr<QueryContext>> query_map_{};
};

size_t currentThreadGroupMemoryUsage();
double currentThreadGroupMemoryUsageRatio();
}
