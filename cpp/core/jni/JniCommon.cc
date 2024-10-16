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

#include "JniCommon.h"

void gluten::JniCommonState::ensureInitialized(JNIEnv* env) {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (initialized_) {
    return;
  }
  initialize(env);
  initialized_ = true;
}

void gluten::JniCommonState::assertInitialized() {
  if (!initialized_) {
    throw gluten::GlutenException("Fatal: JniCommonState::Initialize(...) was not called before using the utility");
  }
}

jmethodID gluten::JniCommonState::runtimeAwareCtxHandle() {
  assertInitialized();
  return runtimeAwareCtxHandle_;
}

void gluten::JniCommonState::initialize(JNIEnv* env) {
  runtimeAwareClass_ = createGlobalClassReference(env, "Lorg/apache/gluten/runtime/RuntimeAware;");
  runtimeAwareCtxHandle_ = getMethodIdOrError(env, runtimeAwareClass_, "rtHandle", "()J");
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  vm_ = vm;
}

void gluten::JniCommonState::close() {
  std::lock_guard<std::mutex> lockGuard(mtx_);
  if (closed_) {
    return;
  }
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  env->DeleteGlobalRef(runtimeAwareClass_);
  closed_ = true;
}

gluten::Runtime* gluten::getRuntime(JNIEnv* env, jobject runtimeAware) {
  int64_t ctxHandle = env->CallLongMethod(runtimeAware, getJniCommonState()->runtimeAwareCtxHandle());
  checkException(env);
  auto ctx = reinterpret_cast<Runtime*>(ctxHandle);
  GLUTEN_CHECK(ctx != nullptr, "FATAL: resource instance should not be null.");
  return ctx;
}

std::unique_ptr<gluten::JniColumnarBatchIterator> gluten::makeJniColumnarBatchIterator(
    JNIEnv* env,
    jobject jColumnarBatchItr,
    gluten::Runtime* runtime,
    std::shared_ptr<ArrowWriter> writer) {
  return std::make_unique<JniColumnarBatchIterator>(env, jColumnarBatchItr, runtime, writer);
}

gluten::JniColumnarBatchIterator::JniColumnarBatchIterator(
    JNIEnv* env,
    jobject jColumnarBatchItr,
    gluten::Runtime* runtime,
    std::shared_ptr<ArrowWriter> writer)
    : runtime_(runtime), writer_(writer) {
  // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
  if (env->GetJavaVM(&vm_) != JNI_OK) {
    std::string errorMessage = "Unable to get JavaVM instance";
    throw gluten::GlutenException(errorMessage);
  }
  serializedColumnarBatchIteratorClass_ =
      createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/ColumnarBatchInIterator;");
  serializedColumnarBatchIteratorHasNext_ =
      getMethodIdOrError(env, serializedColumnarBatchIteratorClass_, "hasNext", "()Z");
  serializedColumnarBatchIteratorNext_ = getMethodIdOrError(env, serializedColumnarBatchIteratorClass_, "next", "()J");
  jColumnarBatchItr_ = env->NewGlobalRef(jColumnarBatchItr);
}

gluten::JniColumnarBatchIterator::~JniColumnarBatchIterator() {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  env->DeleteGlobalRef(jColumnarBatchItr_);
  env->DeleteGlobalRef(serializedColumnarBatchIteratorClass_);
  vm_->DetachCurrentThread();
}

std::shared_ptr<gluten::ColumnarBatch> gluten::JniColumnarBatchIterator::next() {
  JNIEnv* env = nullptr;
  attachCurrentThreadAsDaemonOrThrow(vm_, &env);
  if (!env->CallBooleanMethod(jColumnarBatchItr_, serializedColumnarBatchIteratorHasNext_)) {
    checkException(env);
    return nullptr; // stream ended
  }

  checkException(env);
  jlong handle = env->CallLongMethod(jColumnarBatchItr_, serializedColumnarBatchIteratorNext_);
  checkException(env);
  auto batch = ObjectStore::retrieve<ColumnarBatch>(handle);
  if (writer_ != nullptr) {
    // save snapshot of the batch to file
    std::shared_ptr<ArrowSchema> schema = batch->exportArrowSchema();
    std::shared_ptr<ArrowArray> array = batch->exportArrowArray();
    auto rb = gluten::arrowGetOrThrow(arrow::ImportRecordBatch(array.get(), schema.get()));
    GLUTEN_THROW_NOT_OK(writer_->initWriter(*(rb->schema().get())));
    GLUTEN_THROW_NOT_OK(writer_->writeInBatches(rb));
  }
  return batch;
}
