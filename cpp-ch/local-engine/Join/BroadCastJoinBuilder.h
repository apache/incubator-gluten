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
#include <memory>
#include <jni.h>
#include <substrait/algebra.pb.h>

// Forward Declarations
struct JNIEnv_;
using JNIEnv = JNIEnv_;

namespace local_engine
{
class StorageJoinFromReadBuffer;
namespace BroadCastJoinBuilder
{

    std::shared_ptr<StorageJoinFromReadBuffer> buildJoin(
        const std::string & key,
        jobject input,
        const std::string & join_keys,
        substrait::JoinRel_JoinType join_type,
        const std::string & named_struct);
    void cleanBuildHashTable(const std::string & hash_table_id, jlong instance);
    std::shared_ptr<StorageJoinFromReadBuffer> getJoin(const std::string & hash_table_id);


    void init(JNIEnv *);
    void destroy(JNIEnv *);
}
}
