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

#include <jni.h>

#include "compute/DwrfDatasource.h"
#include "compute/VeloxPlanConverter.h"
#include "jni/jni_errors.h"
#include "velox/substrait/SubstraitToVeloxPlanValidator.h"

// #include "jni/jni_common.h"

#include <jni/dataset/jni_util.h>

static jint JNI_VERSION = JNI_VERSION_1_8;

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  gluten::GetJniErrorsState()->Initialize(env);
#ifdef DEBUG
  std::cout << "Loaded Velox backend." << std::endl;
#endif
  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative(
    JNIEnv* env, jobject obj) {
  JNI_METHOD_START
  gluten::SetBackendFactory(
      [] { return std::make_shared<::velox::compute::VeloxPlanConverter>(); });
  static auto veloxInitializer = std::make_shared<::velox::compute::VeloxInitializer>();
  JNI_METHOD_END()
}

JNIEXPORT jboolean JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeDoValidate(
    JNIEnv* env, jobject obj, jbyteArray planArray) {
  JNI_METHOD_START
  auto planData =
      reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArray, 0));
  auto planSize = env->GetArrayLength(planArray);
  ::substrait::Plan subPlan;
  ParseProtobuf(planData, planSize, &subPlan);

  // A query context used for function validation.
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  // A memory pool used for function validation.
  std::unique_ptr<memory::MemoryPool> pool_ = memory::getDefaultScopedMemoryPool();
  // An execution context used for function validation.
  std::unique_ptr<core::ExecCtx> execCtx_ =
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get());

  auto planValidator =
      std::make_shared<facebook::velox::substrait::SubstraitToVeloxPlanValidator>(
          pool_.get(), execCtx_.get());
  return planValidator->validate(subPlan);
  JNI_METHOD_END(false)
}

JNIEXPORT jlong JNICALL
Java_io_glutenproject_spark_sql_execution_datasources_velox_DwrfDatasourceJniWrapper_nativeInitDwrfDatasource(
    JNIEnv* env, jobject obj, jstring file_path) {
  auto dwrfDatasource = std::make_shared<::velox::compute::DwrfDatasource>(
      arrow::dataset::jni::JStringToCString(env, file_path));
  dwrfDatasource->Init();
  return arrow::dataset::jni::CreateNativeRef(dwrfDatasource);
}

JNIEXPORT jbyteArray JNICALL
Java_io_glutenproject_spark_sql_execution_datasources_velox_DwrfDatasourceJniWrapper_inspectSchema(
    JNIEnv* env, jobject obj, jlong instanceId) {
  auto dwrfDatasource =
      arrow::dataset::jni::RetrieveNativeInstance<::velox::compute::DwrfDatasource>(
          instanceId);
  auto schema = dwrfDatasource->InspectSchema();
  return std::move(arrow::dataset::jni::ToSchemaByteArray(env, schema)).ValueOrDie();
}

JNIEXPORT void JNICALL
Java_io_glutenproject_spark_sql_execution_datasources_velox_DwrfDatasourceJniWrapper_close(
    JNIEnv* env, jobject obj, jlong instanceId) {
  auto dwrfDatasource =
      arrow::dataset::jni::RetrieveNativeInstance<::velox::compute::DwrfDatasource>(
          instanceId);
  dwrfDatasource->Close();
  arrow::dataset::jni::ReleaseNativeRef<::velox::compute::DwrfDatasource>(instanceId);
  return;
}

#ifdef __cplusplus
}
#endif
