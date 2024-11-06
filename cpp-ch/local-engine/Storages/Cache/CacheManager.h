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

#include <substrait/plan.pb.h>

#include <jni.h>
#include <Disks/IDisk.h>
#include <Storages/Cache/JobScheduler.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>

namespace local_engine
{
struct MergeTreePart;
struct MergeTreeTableInstance;

/***
 * Manage the cache of the MergeTree, mainly including part_data.gluten, part_meta.gluten, metadata.gluten
 */
class CacheManager
{
public:
    static jclass cache_result_class;
    static jmethodID cache_result_constructor;
    static void initJNI(JNIEnv * env);

    static CacheManager & instance();
    static void initialize(const DB::ContextMutablePtr & context);
    JobId cacheParts(const MergeTreeTableInstance & table, const std::unordered_set<String> & columns, bool only_meta_cache);
    static jobject getCacheStatus(JNIEnv * env, const String & jobId);

    Task cacheFile(const substrait::ReadRel::LocalFiles::FileOrFiles & file, ReadBufferBuilderPtr read_buffer_builder);
    JobId cacheFiles(substrait::ReadRel::LocalFiles file_infos);
    static void removeFiles(String file, String cache_name);

private:
    Task cachePart(
        const MergeTreeTableInstance & table, const MergeTreePart & part, const std::unordered_set<String> & columns, bool only_meta_cache);
    CacheManager() = default;
    DB::ContextMutablePtr context;
};
}