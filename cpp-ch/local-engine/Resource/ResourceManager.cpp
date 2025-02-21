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

#include "ResourceManager.h"
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <list>
#include <set>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace local_engine
{

struct ResourceDependency
{
    int remaining_count = 0;
    std::vector<DependencyResourceMetadata *> pending_resources;
};

using GetDepentedIds = std::function<const std::vector<std::string> &(const DependencyResourceMetadata & metadata)>;
static void topologySortResource(
    std::map<std::string, ResourcePtr> & resources,
    std::list<DependencyResourceMetadata *> & pending_resources,
    GetDepentedIds get_depented_groups,
    GetDepentedIds get_depented_resources,
    std::map<std::string, ResourceDependency> & group_dependency,
    std::map<std::string, ResourceDependency> & resource_dependency,
    std::vector<ResourcePtr> & result)
{
    auto is_all_dependencies_resolved = [&](const DependencyResourceMetadata & metadata)
    {
        const auto & depented_groups = get_depented_groups(metadata);
        const auto & depented_resources = get_depented_resources(metadata);

        for (const auto & group : depented_groups)
            if (group_dependency[group].remaining_count != 0)
                return false;
        for (const auto & resource : depented_resources)
            if (resource_dependency[resource].remaining_count != 0)
                return false;
        return true;
    };

    std::set<DependencyResourceMetadata *> sorted_resources;
    auto update_sorted_resource = [&](DependencyResourceMetadata * metadata)
    {
        result.push_back(resources[metadata->id]);
        if (!metadata->group_id.empty())
        {
            auto & remaining_count = group_dependency[metadata->group_id].remaining_count;
            remaining_count--;
            if (remaining_count < 0)
            {
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid resource dependency");
            }
            else if (remaining_count == 0)
            {
                for (auto * resource : group_dependency[metadata->group_id].pending_resources)
                    if (is_all_dependencies_resolved(*resource) && !sorted_resources.contains(resource))
                    {
                        pending_resources.push_back(resource);
                        sorted_resources.insert(resource);
                    }
            }
        }

        auto & remaining_count = resource_dependency[metadata->id].remaining_count;
        remaining_count--;
        if (remaining_count != 0)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid resource dependency");
        for (auto * resource : resource_dependency[metadata->id].pending_resources)
            if (is_all_dependencies_resolved(*resource) && !sorted_resources.contains(resource))
            {
                pending_resources.push_back(resource);
                sorted_resources.insert(resource);
            }
    };

    while (!pending_resources.empty())
    {
        auto * metadata = *(pending_resources.begin());
        const auto id = metadata->id;
        const auto group_id = metadata->group_id;
        if (is_all_dependencies_resolved(*metadata))
        {
            update_sorted_resource(metadata);
            pending_resources.pop_front();
        }
        else
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid resource dependency");
        }
    }

    for (const auto & dependency : group_dependency)
        if (dependency.second.remaining_count != 0)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid resource dependency");
    for (const auto & dependency : resource_dependency)
        if (dependency.second.remaining_count != 0)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid resource dependency");
}

void IDependencyResourceManager::sortResources()
{
    for (auto & metadata : resource_creators)
        resources[metadata.first] = metadata.second.creator();
    
    auto has_no_init_denpendency = [](const DependencyResourceMetadata & metadata)
    { return metadata.initialize_after_groups.empty() && metadata.initialize_after_resources.empty(); };
    
    std::map<std::string, ResourceDependency> group_dependency;
    std::map<std::string, ResourceDependency> resource_dependency;
    std::list<DependencyResourceMetadata *> pending_resources;
    for (auto & metadata : resource_creators)
    {
        if (!metadata.second.group_id.empty())
            group_dependency[metadata.second.group_id].remaining_count++;
        resource_dependency[metadata.first].remaining_count++;
        if (has_no_init_denpendency(metadata.second))
            pending_resources.push_back(&metadata.second);
        else
        {
            for (const auto & group : metadata.second.initialize_after_groups)
                group_dependency[group].pending_resources.push_back(&metadata.second);
            for (const auto & resource : metadata.second.initialize_after_resources)
                resource_dependency[resource].pending_resources.push_back(&metadata.second);
        }
    }
    topologySortResource(
        resources,
        pending_resources,
        [](const DependencyResourceMetadata & metadata) { return metadata.initialize_after_groups; },
        [](const DependencyResourceMetadata & metadata) { return metadata.initialize_after_resources; },
        group_dependency,
        resource_dependency,
        initialize_ordered_resources);

    auto has_no_destroy_denpendency = [](const DependencyResourceMetadata & metadata)
    { return metadata.initialize_after_resources.empty() && metadata.destroy_after_resources.empty(); };
    group_dependency.clear();
    resource_dependency.clear();
    pending_resources.clear();
    for (auto & metadata : resource_creators)
    {
        if (!metadata.second.group_id.empty())
            group_dependency[metadata.second.group_id].remaining_count++;
        resource_dependency[metadata.first].remaining_count++;
        if (has_no_destroy_denpendency(metadata.second))
        {
            pending_resources.push_back(&metadata.second);
        }
        else
        {
            for (const auto & group : metadata.second.destroy_after_groups)
                group_dependency[group].pending_resources.push_back(&metadata.second);
            for (const auto & resource : metadata.second.destroy_after_resources)
                resource_dependency[resource].pending_resources.push_back(&metadata.second);
        }
    }
    topologySortResource(
        resources,
        pending_resources,
        [](const DependencyResourceMetadata & metadata) { return metadata.destroy_after_groups; },
        [](const DependencyResourceMetadata & metadata) { return metadata.destroy_after_resources; },
        group_dependency,
        resource_dependency,
        destroy_ordered_resources);

}

void IDependencyResourceManager::registerResource(const DependencyResourceMetadata & metadata)
{
    if (resource_creators.contains(metadata.id))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Resource already exists: {}", metadata.id);
    resource_creators[metadata.id] = metadata;

}

ResourcePtr IDependencyResourceManager::getResourceImpl(const std::string & id) const
{
    auto it = resources.find(id);
    if (it == resources.end())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Resource not found: {}", id);
    return it->second;
}


std::string IDependencyResourceManager::dumpInitializeDependencyDAG() const
{
    DB::WriteBufferFromOwnString o_buf;
    o_buf << "digraph G{\n";
    o_buf << "  node[shape=box];\n";

    std::map<std::string, std::vector<const DependencyResourceMetadata *>> groups;
    for (const auto & metadata : resource_creators)
    {
        std::string group_id = metadata.second.group_id;
        if (group_id.empty())
            group_id = "root";
        groups[metadata.second.group_id].push_back(&metadata.second);
    }

    for (const auto & [group_id, metadatas] : groups)
    {
        o_buf << "  subgraph cluster_" << group_id << " {\n";
        o_buf << "    label = \"cluster_" << group_id << "\";\n";
        for (const auto & metadata : metadatas)
        {
            o_buf << "    " << metadata->id << " [label=\"" << metadata->id << "\"];\n";
        }
        o_buf << "  }\n";
    }

    for (const auto [id, metadata] : resource_creators)
    {
        for (const auto & group : metadata.initialize_after_groups)
            o_buf << "  " << id << " -> " << group << ";\n";
        for (const auto & resource : metadata.initialize_after_resources)
            o_buf << "  " << id << " -> " << resource << ";\n";
    }

    o_buf << "\n}";
    return o_buf.str();
}

std::string IDependencyResourceManager::dumpDestroyDependencyDAG() const
{
    DB::WriteBufferFromOwnString o_buf;
    o_buf << "digraph G{\n";
    o_buf << "  node[shape=box];\n";

    std::map<std::string, std::vector<const DependencyResourceMetadata *>> groups;
    for (const auto & metadata : resource_creators)
    {
        std::string group_id = metadata.second.group_id;
        if (group_id.empty())
            group_id = "root";
        groups[metadata.second.group_id].push_back(&metadata.second);
    }

    for (const auto & [group_id, metadatas] : groups)
    {
        o_buf << "  subgraph cluster_" << group_id << " {\n";
        o_buf << "    label = \"cluster_" << group_id << "\";\n";
        for (const auto & metadata : metadatas)
        {
            o_buf << "    " << metadata->id << " [label=\"" << metadata->id << "\"];\n";
        }
        o_buf << "  }\n";
    }

    for (const auto [id, metadata] : resource_creators)
    {
        for (const auto & group : metadata.destroy_after_groups)
            o_buf << "  " << id << " -> " << group << ";\n";
        for (const auto & resource : metadata.destroy_after_resources)
            o_buf << "  " << id << " -> " << resource << ";\n";
    }

    o_buf << "\n}";
    return o_buf.str();
}


JNIEnvResourceManager & JNIEnvResourceManager::instance()
{
    static JNIEnvResourceManager instance;
    return instance;
}

void JNIEnvResourceManager::initialize(JNIEnv * env)
{
    sortResources();
    for (auto & resource : initialize_ordered_resources)
        dynamic_cast<JNIEnvResource *>(resource.get())->initialize(env);
}

void JNIEnvResourceManager::destroy(JNIEnv * env)
{
    for (auto & resource : destroy_ordered_resources)
        dynamic_cast<JNIEnvResource *>(resource.get())->destroy(env);

}
} // namespace local_engine