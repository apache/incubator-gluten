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
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <boost/noncopyable.hpp>

#include <jni/jni_common.h>

namespace local_engine
{

class IResource
{
public:
    IResource() = default;
    virtual ~IResource() = default;
};
using ResourcePtr = std::shared_ptr<IResource>;
using ResourceCreator = std::function<ResourcePtr()>;

struct DependencyResourceMetadata
{
    // Required, a globally unique id.
    std::string id;
    // Required, the creator of the resource.
    ResourceCreator creator;

    /***********************************************************************************
     * Following are optional, to manager the initialize/release order of the resources.
     ***********************************************************************************/
    // an id descripts the group of the resource.
    std::string group_id;
    // this resource must be initialized after these groups.
    std::vector<std::string> initialize_after_groups = {};
    // this resource must be destroyed after these groups.
    std::vector<std::string> destroy_after_groups = {};
    // this resource must be initialized after these resources.
    std::vector<std::string> initialize_after_resources = {};
    // this resource must be destroyed after these resources.
    std::vector<std::string> destroy_after_resources = {};

    std::string description;
};

// Manage the life cycle of the resources.
class IDependencyResourceManager : public boost::noncopyable
{
public:
    virtual ~IDependencyResourceManager() = default;

    void registerResource(const DependencyResourceMetadata & metadata);
    ResourcePtr getResourceImpl(const std::string & id) const;

    // dump the dependency DAG of the resources.
    std::string dumpInitializeDependencyDAG() const;
    std::string dumpDestroyDependencyDAG() const;
protected:
    IDependencyResourceManager() = default;

    void sortResources();

    std::map<std::string, DependencyResourceMetadata> resource_creators;
    std::map<std::string, ResourcePtr> resources;
    // Resources will be initialized in the order of the following vectors.
    std::vector<ResourcePtr> initialize_ordered_resources;
    // Resources will be destroyed in the order of the following vectors.
    std::vector<ResourcePtr> destroy_ordered_resources;

};

class JNIEnvResource : public IResource
{
public:
    JNIEnvResource() = default;
    virtual ~JNIEnvResource() = default;
    virtual void initialize(JNIEnv * env) = 0;
    virtual void destroy(JNIEnv * env) = 0;
};
using JNIEnvResourcePtr = std::shared_ptr<JNIEnvResource>;


// A manager to manage the life cycle of the process resource.
// - init: call at JNI_Onload
// - destroy: call at JNI_OnUnload
// - get: get the resource by id
class JNIEnvResourceManager : public IDependencyResourceManager
{
public:
    static JNIEnvResourceManager & instance();
    void initialize(JNIEnv * env);
    void destroy(JNIEnv * env);

    // It throws an exception when the resource is not found.
    JNIEnvResourcePtr getResource(const std::string & id) const
    {
        return std::dynamic_pointer_cast<JNIEnvResource>(getResourceImpl(id));
    }
    

protected:
    JNIEnvResourceManager() = default;

};

}