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

namespace local_engine
{
template <typename K, typename V>
class ConcurrentMap
{
public:
    void insert(const K & key, const V & value)
    {
        std::unique_lock lock{mutex};
        map.insert({key, value});
    }

    V get(const K & key)
    {
        std::shared_lock lock{mutex};
        auto it = map.find(key);
        if (it == map.end())
        {
            return nullptr;
        }
        return it->second;
    }

    void erase(const K & key)
    {
        std::unique_lock lock{mutex};
        map.erase(key);
    }

    void clear()
    {
        std::unique_lock lock{mutex};
        map.clear();
    }

    bool contains(const K & key)
    {
        std::shared_lock lock{mutex};
        return map.contains(key);
    }

    size_t size() const
    {
        std::shared_lock lock{mutex};
        return map.size();
    }

private:
    std::unordered_map<K, V> map;
    mutable std::shared_mutex mutex;
};
}
