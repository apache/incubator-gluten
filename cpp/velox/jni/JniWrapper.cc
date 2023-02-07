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

#include <folly/system/ThreadName.h>
#include <jni.h>
#include "include/arrow/c/bridge.h"

#include <glog/logging.h>
#include <jni/JniCommon.h>
#include <exception>
#include "compute/DwrfDatasource.h"
#include "compute/RegistrationAllFunctions.h"
#include "compute/VeloxBackend.h"
#include "jni/JniErrors.h"
#include "memory/VeloxMemoryPool.h"
#include "velox/substrait/SubstraitToVeloxPlanValidator.h"

#include <iostream>

using namespace facebook::velox;

static std::unordered_map<std::string, std::string> sparkConfs_;

// Extract Spark confs from Substrait plan and set them to the conf map.
void setUpConfMap(JNIEnv* env, jobject obj, jbyteArray planArray) {
  if (sparkConfs_.size() != 0) {
    return;
  }

  auto planData = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArray, 0));
  auto planSize = env->GetArrayLength(planArray);
  ::substrait::Plan subPlan;
  gluten::ParseProtobuf(planData, planSize, &subPlan);

  if (subPlan.has_advanced_extensions()) {
    auto extension = subPlan.advanced_extensions();
    if (extension.has_enhancement()) {
      const auto& enhancement = extension.enhancement();
      ::substrait::Expression expression;
      if (!enhancement.UnpackTo(&expression)) {
        std::string error_message =
            "Can't Unapck the Any object to Expression Literal when passing the spark conf to velox";
        gluten::JniThrow(error_message);
      }
      if (expression.has_literal()) {
        auto literal = expression.literal();
        if (literal.has_map()) {
          auto literal_map = literal.map();
          auto size = literal_map.key_values_size();
          for (auto i = 0; i < size; i++) {
            ::substrait::Expression_Literal_Map_KeyValue keyValue = literal_map.key_values(i);
            sparkConfs_.emplace(keyValue.key().string(), keyValue.value().string());
          }
        }
      }
    }
  }
}

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  gluten::GetJniErrorsState()->Initialize(env);
#ifdef GLUTEN_PRINT_DEBUG
  std::cout << "Loaded Velox backend." << std::endl;
#endif
  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
}

JNIEXPORT void JNICALL Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative(
    JNIEnv* env,
    jobject obj,
    jbyteArray planArray) {
  JNI_METHOD_START
  setUpConfMap(env, obj, planArray);
  gluten::SetBackendFactory([] { return std::make_shared<gluten::VeloxBackend>(sparkConfs_); });
  static auto veloxInitializer = std::make_shared<gluten::VeloxInitializer>(sparkConfs_);
  JNI_METHOD_END()
}

JNIEXPORT jboolean JNICALL Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeDoValidate(
    JNIEnv* env,
    jobject obj,
    jbyteArray planArray) {
  JNI_METHOD_START
  auto planData = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArray, 0));
  auto planSize = env->GetArrayLength(planArray);
  ::substrait::Plan subPlan;
  gluten::ParseProtobuf(planData, planSize, &subPlan);

  // A query context used for function validation.
  core::QueryCtx queryCtx;

  auto pool = gluten::GetDefaultWrappedVeloxMemoryPool();

  // An execution context used for function validation.
  core::ExecCtx execCtx(pool, &queryCtx);

  facebook::velox::substrait::SubstraitToVeloxPlanValidator planValidator(pool, &execCtx);
  try {
    return planValidator.validate(subPlan);
  } catch (std::invalid_argument& e) {
    LOG(INFO) << "Faled to validate substrait plan because " << e.what();
    return false;
  }
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_spark_sql_execution_datasources_velox_DwrfDatasourceJniWrapper_nativeInitDwrfDatasource(
    JNIEnv* env,
    jobject obj,
    jstring file_path,
    jlong c_schema) {
  auto pool = gluten::GetDefaultWrappedVeloxMemoryPool();
  gluten::DwrfDatasource* dwrfDatasource = nullptr;
  if (c_schema == -1) {
    // Only inspect the schema and not write
    dwrfDatasource = new gluten::DwrfDatasource(JStringToCString(env, file_path), nullptr, pool);
    // dwrfDatasource->Init( );
  } else {
    auto schema = gluten::JniGetOrThrow(arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(c_schema)));
    dwrfDatasource = new gluten::DwrfDatasource(JStringToCString(env, file_path), schema, pool);
    dwrfDatasource->Init(sparkConfs_);
  }
  return (jlong)dwrfDatasource;
}

JNIEXPORT jbyteArray JNICALL
Java_io_glutenproject_spark_sql_execution_datasources_velox_DwrfDatasourceJniWrapper_inspectSchema(
    JNIEnv* env,
    jobject obj,
    jlong instanceId) {
  JNI_METHOD_START
  auto dwrfDatasource = (gluten::DwrfDatasource*)(instanceId);
  auto schema = dwrfDatasource->InspectSchema();
  return ToSchemaByteArray(env, schema);
  JNI_METHOD_END(nullptr)
}

JNIEXPORT void JNICALL Java_io_glutenproject_spark_sql_execution_datasources_velox_DwrfDatasourceJniWrapper_close(
    JNIEnv* env,
    jobject obj,
    jlong instanceId) {
  JNI_METHOD_START
  auto dwrfDatasource = (gluten::DwrfDatasource*)(instanceId);
  dwrfDatasource->Close();
  delete dwrfDatasource;
  JNI_METHOD_END()
}

JNIEXPORT void JNICALL Java_io_glutenproject_spark_sql_execution_datasources_velox_DwrfDatasourceJniWrapper_write(
    JNIEnv* env,
    jobject obj,
    jlong instanceId,
    jlong c_schema,
    jlong c_array) {
  JNI_METHOD_START
  std::shared_ptr<arrow::RecordBatch> rb = gluten::JniGetOrThrow(arrow::ImportRecordBatch(
      reinterpret_cast<struct ArrowArray*>(c_array), reinterpret_cast<struct ArrowSchema*>(c_schema)));

  auto dwrfDatasource = (gluten::DwrfDatasource*)(instanceId);
  dwrfDatasource->Write(rb);
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
