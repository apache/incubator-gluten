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

#include <stdexcept>

#include "utils/exception.h"
#include "jni_common.h"

#define JNI_METHOD_START try {
// macro ended

#define JNI_METHOD_END(fallback_expr)                                               \
  }                                                                                 \
  catch (std::exception & e) {                                                      \
    env->ThrowNew(gluten::GetJniErrorsState()->RuntimeExceptionClass(), e.what());  \
    return fallback_expr;                                                           \
  }
// macro ended

namespace gluten {

class JniPendingException : public std::runtime_error {
 public:
  explicit JniPendingException(const std::string& arg) : runtime_error(arg) {}
};

void ThrowPendingException(const std::string& message) {
  throw JniPendingException(message);
}

template <typename T>
T JniGetOrThrow(arrow::Result<T> result) {
  if (!result.status().ok()) {
    ThrowPendingException(result.status().message());
  }
  return std::move(result).ValueOrDie();
}

template <typename T>
T JniGetOrThrow(arrow::Result<T> result, const std::string& message) {
  if (!result.status().ok()) {
    ThrowPendingException(message + " - " + result.status().message());
  }
  return std::move(result).ValueOrDie();
}

void JniAssertOkOrThrow(arrow::Status status) {
  if (!status.ok()) {
    ThrowPendingException(status.message());
  }
}

void JniAssertOkOrThrow(arrow::Status status, const std::string& message) {
  if (!status.ok()) {
    ThrowPendingException(message + " - " + status.message());
  }
}

void JniThrow(const std::string& message) { ThrowPendingException(message); }

static struct JniErrorsGlobalState {
 public:
  virtual ~JniErrorsGlobalState() = default;

  void Initialize(JNIEnv* env) {
    std::lock_guard<std::mutex> lock_guard(mtx_);
    io_exception_class_ = CreateGlobalClassReference(env, "Ljava/io/IOException;");
    runtime_exception_class_ =
        CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");
    unsupportedoperation_exception_class_ =
        CreateGlobalClassReference(env, "Ljava/lang/UnsupportedOperationException;");
    illegal_access_exception_class_ =
        CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
    illegal_argument_exception_class_ =
        CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");
  }

  jclass RuntimeExceptionClass() {
    std::lock_guard<std::mutex> lock_guard(mtx_);
    if (runtime_exception_class_ == nullptr) {
      throw gluten::GlutenException("Fatal: JniGlobalState::Initialize(...) was not called before using the utility");
    }
    return runtime_exception_class_;
  }

  jclass IllegalAccessExceptionClass() {
    std::lock_guard<std::mutex> lock_guard(mtx_);
    if (illegal_access_exception_class_ == nullptr) {
      throw gluten::GlutenException("Fatal: JniGlobalState::Initialize(...) was not called before using the utility");
    }
    return illegal_access_exception_class_;
  }

 private:
  jclass io_exception_class_ = nullptr;
  jclass runtime_exception_class_ = nullptr;
  jclass unsupportedoperation_exception_class_ = nullptr;
  jclass illegal_access_exception_class_ = nullptr;
  jclass illegal_argument_exception_class_ = nullptr;
  std::mutex mtx_;

} jni_errors_state;

static JniErrorsGlobalState* GetJniErrorsState() {
  return &jni_errors_state;
}

}  // namespace gluten

