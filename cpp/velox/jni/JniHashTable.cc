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

#include <arrow/c/abi.h>

#include <jni/JniCommon.h>
#include <iostream>
#include "JniHashTable.h"
#include "folly/String.h"
#include "memory/ColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "substrait/algebra.pb.h"
#include "substrait/type.pb.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"

namespace gluten {

static jclass jniVeloxBroadcastBuildSideCache = nullptr;
static jmethodID jniGet = nullptr;

jlong callJavaGet(const std::string& id) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
    throw gluten::GlutenException("JNIEnv was not attached to current thread");
  }

  const jstring s = env->NewStringUTF(id.c_str());

  auto result = env->CallStaticLongMethod(jniVeloxBroadcastBuildSideCache, jniGet, s);
  return result;
}

// Return the velox's hash table.
std::shared_ptr<HashTableBuilder> nativeHashTableBuild(
    const std::string& joinKeys,
    std::vector<std::string> names,
    std::vector<facebook::velox::TypePtr> veloxTypeList,
    int joinType,
    bool hasMixedJoinCondition,
    bool isExistenceJoin,
    bool isNullAwareAntiJoin,
    std::vector<std::shared_ptr<ColumnarBatch>>& batches,
    std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool) {
  auto rowType = std::make_shared<facebook::velox::RowType>(std::move(names), std::move(veloxTypeList));

  auto sJoin = static_cast<substrait::JoinRel_JoinType>(joinType);
  facebook::velox::core::JoinType vJoin;
  switch (sJoin) {
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
      vJoin = facebook::velox::core::JoinType::kInner;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_OUTER:
      vJoin = facebook::velox::core::JoinType::kFull;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
      vJoin = facebook::velox::core::JoinType::kLeft;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
      vJoin = facebook::velox::core::JoinType::kRight;
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI:
      // Determine the semi join type based on extracted information.
      if (isExistenceJoin) {
        vJoin = facebook::velox::core::JoinType::kLeftSemiProject;
      } else {
        vJoin = facebook::velox::core::JoinType::kLeftSemiFilter;
      }
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI:
      // Determine the semi join type based on extracted information.
      if (isExistenceJoin) {
        vJoin = facebook::velox::core::JoinType::kRightSemiProject;
      } else {
        vJoin = facebook::velox::core::JoinType::kRightSemiFilter;
      }
      break;
    case ::substrait::JoinRel_JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_ANTI: {
      // Determine the anti join type based on extracted information.
      vJoin = facebook::velox::core::JoinType::kAnti;
      break;
    }
    default:
      VELOX_NYI("Unsupported Join type: {}", std::to_string(sJoin));
  }

  std::vector<std::string> joinKeyNames;
  folly::split(',', joinKeys, joinKeyNames);

  std::vector<std::shared_ptr<const facebook::velox::core::FieldAccessTypedExpr>> joinKeyTypes;
  joinKeyTypes.reserve(joinKeyNames.size());
  for (const auto& name : joinKeyNames) {
    joinKeyTypes.emplace_back(
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(rowType->findChild(name), name));
  }

  auto hashTableBuilder = std::make_shared<HashTableBuilder>(
      vJoin, isNullAwareAntiJoin, hasMixedJoinCondition, joinKeyTypes, rowType, memoryPool.get());

  for (auto i = 0; i < batches.size(); i++) {
    auto rowVector = VeloxColumnarBatch::from(memoryPool.get(), batches[i])->getRowVector();
    // std::cout << "the hash table rowVector is " << rowVector->toString(0, rowVector->size()) << "\n";
    hashTableBuilder->addInput(rowVector);
  }
  return hashTableBuilder;
}

long getJoin(std::string hashTableId) {
  return callJavaGet(hashTableId);
}

void initVeloxJniHashTable(JNIEnv* env) {
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  const char* classSig = "Lorg/apache/gluten/execution/VeloxBroadcastBuildSideCache;";
  jniVeloxBroadcastBuildSideCache = createGlobalClassReferenceOrError(env, classSig);
  jniGet = getStaticMethodId(env, jniVeloxBroadcastBuildSideCache, "get", "(Ljava/lang/String;)J");
}

void finalizeVeloxJniHashTable(JNIEnv* env) {
  env->DeleteGlobalRef(jniVeloxBroadcastBuildSideCache);
}

} // namespace gluten
