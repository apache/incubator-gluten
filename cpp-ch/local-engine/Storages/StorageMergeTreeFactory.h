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
#include <Poco/LRUCache.h>
#include <Parser/SerializedPlanParser.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Interpreters/MergeTreeTransaction.h>

namespace local_engine
{
using CustomStorageMergeTreePtr = std::shared_ptr<CustomStorageMergeTree>;

class StorageMergeTreeFactory
{
public:
    static StorageMergeTreeFactory & instance();
    static void freeStorage(StorageID id);
    static CustomStorageMergeTreePtr
    getStorage(StorageID id, const String & snapshot_id, ColumnsDescription columns, std::function<CustomStorageMergeTreePtr()> creator);
    static DataPartsVector getDataParts(StorageID id, const String & snapshot_id, std::unordered_set<String> part_name);
    static void init_cache_map()
    {
        auto & storage_map_v = storage_map;
        if (!storage_map_v)
        {
            storage_map_v = std::make_unique<Poco::LRUCache<std::string, CustomStorageMergeTreePtr>>(
                SerializedPlanParser::global_context->getConfigRef().getInt64("table_metadata_cache_max_count", 100));
        }
        else
        {
            storage_map_v->clear();
        }
        auto & datapart_map_v = datapart_map;
        if (!datapart_map_v)
        {
            datapart_map_v = std::make_unique<Poco::LRUCache<std::string, std::shared_ptr<Poco::LRUCache<std::string, DataPartPtr>>>>(
                SerializedPlanParser::global_context->getConfigRef().getInt64("table_metadata_cache_max_count", 100));
        }
        else
        {
            datapart_map_v->clear();
        }
    }
    static void clear()
    {
        if (storage_map) storage_map->clear();
        if (datapart_map) datapart_map->clear();
    }

private:
    static std::unique_ptr<Poco::LRUCache<std::string, CustomStorageMergeTreePtr>> storage_map;
    static std::unique_ptr<Poco::LRUCache<std::string, std::shared_ptr<Poco::LRUCache<std::string, DataPartPtr>>>> datapart_map;
    static std::mutex storage_map_mutex;
    static std::mutex datapart_mutex;
};

struct TempStorageFreer
{
    StorageID id;
    ~TempStorageFreer()
    {
        StorageMergeTreeFactory::instance().freeStorage(id);
    }
};
}
