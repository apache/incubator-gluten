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

#include <shared_mutex>
#include <unordered_map>

#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheKey.h>

namespace local_engine
{

using Lock = std::shared_mutex;
using WriteLock = std::unique_lock<Lock>;
using ReadLock = std::shared_lock<Lock>;

class FileCacheConcurrentMap
{
public:
    void insert(const DB::FileCacheKey & key, const Int64 value)
    {
        WriteLock wLock(rw_locker);
        map.emplace(key, value);
    }

    void
    update_cache_time(const DB::FileCacheKey & key, const String & path, const Int64 accept_cache_time, const DB::FileCachePtr & file_cache)
    {
        WriteLock wLock(rw_locker);
        // double check
        auto it = map.find(key);
        if (it != map.end())
        {
            if (it->second < accept_cache_time)
            {
                // will delete cache file immediately
                file_cache->removePathIfExists(path, DB::FileCache::getCommonUser().user_id);
                // update cache time
                map[key] = accept_cache_time;
            }
        }
        else
        {
            // will delete cache file immediately
            file_cache->removePathIfExists(path, DB::FileCache::getCommonUser().user_id);
            // set cache time
            map.emplace(key, accept_cache_time);
        }
    }

    std::optional<Int64> get(const DB::FileCacheKey & key)
    {
        ReadLock rLock(rw_locker);
        auto it = map.find(key);
        if (it == map.end())
        {
            return std::nullopt;
        }
        return it->second;
    }

    bool contain(const DB::FileCacheKey & key)
    {
        ReadLock rLock(rw_locker);
        return map.contains(key);
    }

    void erase(const DB::FileCacheKey & key)
    {
        WriteLock wLock(rw_locker);
        if (map.find(key) == map.end())
        {
            return;
        }
        map.erase(key);
    }

    void clear()
    {
        WriteLock wLock(rw_locker);
        map.clear();
    }

    size_t size() const
    {
        ReadLock rLock(rw_locker);
        return map.size();
    }

private:
    std::unordered_map<DB::FileCacheKey, Int64> map;
    mutable Lock rw_locker;
};
}
