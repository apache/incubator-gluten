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
#include "StorageMergeTreeFactory.h"

namespace local_engine
{
StorageMergeTreeFactory & StorageMergeTreeFactory::instance()
{
    static StorageMergeTreeFactory ret;
    return ret;
}

void StorageMergeTreeFactory::freeStorage(StorageID id)
{
    if (!id.hasUUID())
    {
        return;
    }
    auto table_name = id.database_name + "." + id.table_name + "@" + toString(id.uuid);

    {
        std::lock_guard lock(storage_map_mutex);
        if (storage_map->has(table_name))
        {
            storage_map->remove(table_name);
        }
    }

    {
        std::lock_guard lock(datapart_mutex);
        if (datapart_map->has(table_name))
        {
            datapart_map->remove(table_name);
        }
    }
}

CustomStorageMergeTreePtr
StorageMergeTreeFactory::getStorage(StorageID id, const String & snapshot_id, ColumnsDescription columns, std::function<CustomStorageMergeTreePtr()> creator)
{
    auto table_name = id.database_name + "." + id.table_name;
    // for optimize table
    if (id.hasUUID())
    {
        table_name += "@" + toString(id.uuid);
    }
    else
    {
        table_name += "_" + snapshot_id;
    }
    std::lock_guard lock(storage_map_mutex);
    if (!storage_map->has(table_name))
    {
        storage_map->add(table_name, creator());
    }
    return *(storage_map->get(table_name));
}

DataPartsVector StorageMergeTreeFactory::getDataParts(StorageID id, const String & snapshot_id, std::unordered_set<String> part_name)
{
    DataPartsVector res;
    auto table_name = id.database_name + "." + id.table_name;
    // for optimize table
    if (id.hasUUID())
    {
        table_name += "@" + toString(id.uuid);
    }
    else
    {
        table_name += "_" + snapshot_id;
    }
    std::lock_guard lock(datapart_mutex);
    std::unordered_set<String> missing_names;
    if (!datapart_map->has(table_name)) [[unlikely]]
    {
        auto cache = std::make_shared<Poco::LRUCache<std::string, DataPartPtr>>(
            SerializedPlanParser::global_context->getConfigRef().getInt64("table_part_metadata_cache_max_count", 1000000)
            );
        datapart_map->add(table_name, cache);
    }

    // find the missing cache part name
    for (const auto & name : part_name)
    {
        if (!(*(datapart_map->get(table_name)))->has(name))
        {
            missing_names.emplace(name);
        }
        else
        {
            res.emplace_back((*((*(datapart_map->get(table_name)))->get(name))));
        }
    }

    if (!missing_names.empty())
    {
        CustomStorageMergeTreePtr storage_merge_tree;
        {
            std::lock_guard storage_lock(storage_map_mutex);
            storage_merge_tree = *(storage_map->get(table_name));
        }
        auto missing_parts = storage_merge_tree->loadDataPartsWithNames(missing_names);
        for (const auto & part : missing_parts)
        {
            res.emplace_back(part);
            (*(datapart_map->get(table_name)))->add(part->name, part);
        }
    }
    return res;
}
// will be inited in native init phase
std::unique_ptr<Poco::LRUCache<std::string, CustomStorageMergeTreePtr>> StorageMergeTreeFactory::storage_map = nullptr;
std::unique_ptr<Poco::LRUCache<std::string, std::shared_ptr<Poco::LRUCache<std::string, DataPartPtr>>>> StorageMergeTreeFactory::datapart_map = nullptr;
std::mutex StorageMergeTreeFactory::storage_map_mutex;
std::mutex StorageMergeTreeFactory::datapart_mutex;

}
