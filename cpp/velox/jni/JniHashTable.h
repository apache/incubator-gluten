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

#include <jni.h>
#include "memory/ColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "utils/ObjectStore.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/HashTableBuilder.h"

namespace gluten {

inline static JavaVM* vm = nullptr;

static std::unique_ptr<ObjectStore> hashTableObjStore = ObjectStore::create();

// Return the hash table builder address.
std::shared_ptr<facebook::velox::exec::HashTableBuilder> nativeHashTableBuild(
    const std::string& joinKeys,
    std::vector<std::string> names,
    std::vector<facebook::velox::TypePtr> veloxTypeList,
    int joinType,
    bool hasMixedJoinCondition,
    bool isExistenceJoin,
    bool isNullAwareAntiJoin,
    std::vector<std::shared_ptr<ColumnarBatch>>& batches,
    std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool);

long getJoin(std::string hashTableId);

void initVeloxJniHashTable(JNIEnv* env);

void finalizeVeloxJniHashTable(JNIEnv* env);

jlong callJavaGet(const std::string& id);

} // namespace gluten
