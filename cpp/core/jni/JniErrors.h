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

#include "JniCommon.h"
#include "utils/exception.h"

#define JNI_METHOD_START try {
// macro ended

#define JNI_METHOD_END(fallback_expr)                                              \
  }                                                                                \
  catch (std::exception & e) {                                                     \
    env->ThrowNew(gluten::getJniErrorsState()->runtimeExceptionClass(), e.what()); \
    return fallback_expr;                                                          \
  }
// macro ended

namespace gluten {

class JniPendingException final : public std::runtime_error {
 public:
  explicit JniPendingException(const std::string& arg) : runtime_error(arg) {}
};

static inline void throwPendingException(const std::string& message) {
  throw JniPendingException(message);
}

template <typename T>
inline T jniGetOrThrow(arrow::Result<T> result) {
  if (!result.status().ok()) {
    throwPendingException(result.status().message());
  }
  return std::move(result).ValueOrDie();
}

template <typename T>
inline T jniGetOrThrow(arrow::Result<T> result, const std::string& message) {
  if (!result.status().ok()) {
    ThrowPendingException(message + " - " + result.status().message());
  }
  return std::move(result).ValueOrDie();
}

static inline void jniAssertOkOrThrow(arrow::Status status) {
  if (!status.ok()) {
    throwPendingException(status.message());
  }
}

static inline void jniAssertOkOrThrow(arrow::Status status, const std::string& message) {
  if (!status.ok()) {
    throwPendingException(message + " - " + status.message());
  }
}

static inline void jniThrow(const std::string& message) {
  throwPendingException(message);
}

static struct JniErrorsGlobalState {
 public:
  virtual ~JniErrorsGlobalState() = default;

  void initialize(JNIEnv* env) {
    std::lock_guard<std::mutex> lockGuard(mtx_);
    ioExceptionClass_ = createGlobalClassReference(env, "Ljava/io/IOException;");
    runtimeExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/RuntimeException;");
    unsupportedoperationExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/UnsupportedOperationException;");
    illegalAccessExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
    illegalArgumentExceptionClass_ = createGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");
  }

  jclass runtimeExceptionClass() {
    std::lock_guard<std::mutex> lockGuard(mtx_);
    if (runtimeExceptionClass_ == nullptr) {
      throw gluten::GlutenException("Fatal: JniGlobalState::Initialize(...) was not called before using the utility");
    }
    return runtimeExceptionClass_;
  }

  jclass illegalAccessExceptionClass() {
    std::lock_guard<std::mutex> lockGuard(mtx_);
    if (illegalAccessExceptionClass_ == nullptr) {
      throw gluten::GlutenException("Fatal: JniGlobalState::Initialize(...) was not called before using the utility");
    }
    return illegalAccessExceptionClass_;
  }

 private:
  jclass ioExceptionClass_ = nullptr;
  jclass runtimeExceptionClass_ = nullptr;
  jclass unsupportedoperationExceptionClass_ = nullptr;
  jclass illegalAccessExceptionClass_ = nullptr;
  jclass illegalArgumentExceptionClass_ = nullptr;
  std::mutex mtx_;

} jniErrorsState;

static inline JniErrorsGlobalState* getJniErrorsState() {
  return &jniErrorsState;
}

} // namespace gluten
