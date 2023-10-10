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

#include "compute/ProtobufUtils.h"
#include "jni/JniError.h"
#include "substrait/plan.pb.h"

namespace gluten {
std::unordered_map<std::string, std::string> getConfMap(JNIEnv* env, jbyteArray planArray) {
  std::unordered_map<std::string, std::string> sparkConfs;
  auto planData = reinterpret_cast<const uint8_t*>(env->GetByteArrayElements(planArray, 0));
  auto planSize = env->GetArrayLength(planArray);
  ::substrait::Plan subPlan;
  gluten::parseProtobuf(planData, planSize, &subPlan);

  if (subPlan.has_advanced_extensions()) {
    auto extension = subPlan.advanced_extensions();
    if (extension.has_enhancement()) {
      const auto& enhancement = extension.enhancement();
      ::substrait::Expression expression;
      if (!enhancement.UnpackTo(&expression)) {
        std::string errorMessage =
            "Can't Unapck the Any object to Expression Literal when passing the spark conf to velox";
        throw gluten::GlutenException(errorMessage);
      }
      if (expression.has_literal()) {
        auto literal = expression.literal();
        if (literal.has_map()) {
          auto literalMap = literal.map();
          auto size = literalMap.key_values_size();
          for (auto i = 0; i < size; i++) {
            ::substrait::Expression_Literal_Map_KeyValue keyValue = literalMap.key_values(i);
            sparkConfs.emplace(keyValue.key().string(), keyValue.value().string());
          }
        }
      }
    }
  }
  return sparkConfs;
}
} // namespace gluten
