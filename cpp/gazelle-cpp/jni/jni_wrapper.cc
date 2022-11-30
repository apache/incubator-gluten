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

#include "compute/substrait_arrow.h"
#include "jni/jni_errors.h"

#include <google/protobuf/wrappers.pb.h>

#ifdef __cplusplus
extern "C" {
#endif

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  gluten::GetJniErrorsState()->Initialize(env);
  std::cout << "loaded gazelle_cpp" << std::endl;
  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
}

JNIEXPORT void JNICALL
Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeInitNative(JNIEnv* env, jobject obj) {
  JNI_METHOD_START
  gluten::GazelleInitialize();
  gluten::SetBackendFactory([] { return std::make_shared<gluten::ArrowExecBackend>(); });
  JNI_METHOD_END()
}

JNIEXPORT jboolean JNICALL Java_io_glutenproject_vectorized_ExpressionEvaluatorJniWrapper_nativeDoValidate(
    JNIEnv* env,
    jobject obj,
    jbyteArray planArray) {
  JNI_METHOD_START
  auto data = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArray, 0));
  auto size = env->GetArrayLength(planArray);
  ::substrait::Plan plan;
#ifdef GLUTEN_PRINT_DEBUG
  auto buf = std::make_shared<arrow::Buffer>(data, size);
  auto maybe_plan_json = SubstraitToJSON("Plan", *buf);
  if (maybe_plan_json.status().ok()) {
    std::cout << std::string(50, '#') << " received substrait::Plan:" << std::endl;
    std::cout << maybe_plan_json.ValueOrDie() << std::endl;
  } else {
    std::cout << "Error parsing substrait plan to json: " << maybe_plan_json.status().ToString() << std::endl;
  }
#endif
  ParseProtobuf(data, size, &plan);
  const auto& relation = plan.relations()[0];
  const auto& rel = relation.has_root() ? relation.root().input() : relation.rel();
  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kRead:
    case substrait::Rel::RelTypeCase::kFilter:
      return true;
    default:
      return false;
  }
  JNI_METHOD_END(false)
}

#ifdef __cplusplus
}
#endif
