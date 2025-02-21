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
#include <string>
#include <unordered_map>
#include <boost/core/noncopyable.hpp>

#include "ResourceManager.h"


namespace local_engine
{

struct JVMClassDescription
{
    std::string class_signature;
    std::vector<std::pair<std::string, std::string>> methods;
    std::vector<std::pair<std::string, std::string>> static_methods;
};

class JVMClassReference : public JNIEnvResource, public boost::noncopyable
{
public:
    JVMClassReference(const JVMClassDescription & description_);

    ~JVMClassReference() override = default;

    jclass operator()() const { return getClass(); }
    jclass getClass() const { return jvm_class; }

    jmethodID operator[](const std::string methodId) const { return getJMethod(methodId); }
    jmethodID getJMethod(const std::string methodId) const;

    void initialize(JNIEnv * env) override;
    void destroy(JNIEnv * env) override;

private:
    JVMClassDescription description;
    jclass jvm_class;
    std::unordered_map<std::string, jmethodID> method_ids;
};

#define JVM_CLASS_REFERENCE(id) \
    (*(dynamic_cast<local_engine::JVMClassReference *>(local_engine::JNIEnvResourceManager::instance().getResource(#id).get())))
}
