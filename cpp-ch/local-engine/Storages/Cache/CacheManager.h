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
#include <Disks/IDisk.h>
#include <latch>


namespace local_engine
{
struct MergeTreePart;
struct MergeTreeTable;
/***
 * Manage the cache of the MergeTree, mainly including meta.bin, data.bin, metadata.gluten
 */
class CacheManager {
public:
    static CacheManager & instance();
    static void initialize(DB::ContextMutablePtr context);
    void cachePart(const MergeTreeTable& table, const MergeTreePart& part, const std::unordered_set<String>& columns, std::shared_ptr<std::latch> latch = nullptr);
    void cacheParts(const String& table_def, const std::unordered_set<String>& columns, bool async = true);
private:
    CacheManager() = default;

    std::unique_ptr<ThreadPool> thread_pool;
    DB::ContextMutablePtr context;
    std::unordered_map<String, DB::DiskPtr> policy_to_disk;
    std::unordered_map<DB::DiskPtr, DB::DiskPtr> disk_to_metadisk;
    std::unordered_map<String, DB::FileCachePtr> policy_to_cache;
};
}