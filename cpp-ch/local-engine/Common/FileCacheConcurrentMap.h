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
    void insert(const DB::FileCacheKey & key, const size_t last_modified_time, const size_t file_size)
    {
        WriteLock wLock(rw_locker);
        if (const auto it = map.find(key); it != map.end())
            return;
        map.emplace(key, std::tuple(last_modified_time, file_size));
    }

    void update_cache_time(
        const DB::FileCacheKey & key, const Int64 new_modified_time, const size_t new_file_size, const DB::FileCachePtr & file_cache)
    {
        WriteLock wLock(rw_locker);
        auto it = map.find(key);
        if (it != map.end())
        {
            auto & [last_modified_time, file_size] = it->second;
            if (last_modified_time < new_modified_time || file_size != new_file_size)
            {
                // will delete cache file immediately
                file_cache->removeKeyIfExists(key, DB::FileCache::getCommonUser().user_id);
                // update cache time
                map[key] = std::tuple(new_modified_time, new_file_size);
            }
        }
        else
        {
            // will delete cache file immediately
            file_cache->removeKeyIfExists(key, DB::FileCache::getCommonUser().user_id);
            // set cache time
            map.emplace(key, std::tuple(new_modified_time, new_file_size));
        }
    }

    std::optional<std::tuple<size_t, size_t>> get(const DB::FileCacheKey & key)
    {
        ReadLock rLock(rw_locker);
        auto it = map.find(key);
        if (it == map.end())
            return std::nullopt;
        return it->second;
    }

    // bool contain(const DB::FileCacheKey & key)
    // {
    //     ReadLock rLock(rw_locker);
    //     return map.contains(key);
    // }

    // void erase(const DB::FileCacheKey & key)
    // {
    //     WriteLock wLock(rw_locker);
    //     if (map.find(key) == map.end())
    //     {
    //         return;
    //     }
    //     map.erase(key);
    // }
    //
    // void clear()
    // {
    //     WriteLock wLock(rw_locker);
    //     map.clear();
    // }
    //
    // size_t size() const
    // {
    //     ReadLock rLock(rw_locker);
    //     return map.size();
    // }

private:
    std::unordered_map<DB::FileCacheKey, std::tuple<size_t, size_t>> map;
    mutable Lock rw_locker;
};
}
