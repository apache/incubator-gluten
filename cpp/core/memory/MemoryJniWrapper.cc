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

#include "jni/JniCommon.h"
#include "jni/JniError.h"

#ifdef __cplusplus
extern "C" {
#endif

using namespace gluten;

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_NativeMemoryJniWrapper_getAllocator( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jTypeName) {
  JNI_METHOD_START
  std::string typeName = jStringToCString(env, jTypeName);
  std::shared_ptr<MemoryAllocator>* allocator = new std::shared_ptr<MemoryAllocator>;
  if (typeName == "DEFAULT") {
    *allocator = defaultMemoryAllocator();
  } else {
    delete allocator;
    allocator = nullptr;
    throw GlutenException("Unexpected allocator type name: " + typeName);
  }
  return reinterpret_cast<jlong>(allocator);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_memory_NativeMemoryJniWrapper_releaseAllocator( // NOLINT
    JNIEnv* env,
    jclass,
    jlong allocatorId) {
  JNI_METHOD_START
  delete reinterpret_cast<std::shared_ptr<MemoryAllocator>*>(allocatorId);
  JNI_METHOD_END()
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_NativeMemoryJniWrapper_bytesAllocated( // NOLINT
    JNIEnv* env,
    jclass,
    jlong allocatorId) {
  JNI_METHOD_START
  auto* alloc = reinterpret_cast<std::shared_ptr<MemoryAllocator>*>(allocatorId);
  if (alloc == nullptr) {
    throw gluten::GlutenException("Memory allocator instance not found. It may not exist nor has been closed");
  }
  return (*alloc)->getBytes();
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_NativeMemoryJniWrapper_create( // NOLINT
    JNIEnv* env,
    jclass,
    jstring jbackendType,
    jstring jnmmName,
    jlong allocatorId,
    jlong reservationBlockSize,
    jobject jlistener) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }
  auto allocator = reinterpret_cast<std::shared_ptr<MemoryAllocator>*>(allocatorId);
  if (allocator == nullptr) {
    throw gluten::GlutenException("Allocator does not exist or has been closed");
  }

  std::unique_ptr<AllocationListener> listener = std::make_unique<SparkAllocationListener>(
      vm,
      jlistener,
      gluten::getJniCommonState()->reserveMemoryMethod,
      gluten::getJniCommonState()->unreserveMemoryMethod,
      reservationBlockSize);

  if (gluten::backtrace_allocation) {
    listener = std::make_unique<BacktraceAllocationListener>(std::move(listener));
  }

  auto name = jStringToCString(env, jnmmName);
  auto backendType = jStringToCString(env, jbackendType);
  // TODO: move memory manager into ExecutionCtx then we can use more general ExecutionCtx.
  auto executionCtx = gluten::ExecutionCtx::create(backendType);
  auto manager = executionCtx->createMemoryManager(name, *allocator, std::move(listener));
  gluten::ExecutionCtx::release(executionCtx);
  return reinterpret_cast<jlong>(manager);
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT jbyteArray JNICALL Java_io_glutenproject_memory_NativeMemoryJniWrapper_collectMemoryUsage( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);

  const MemoryUsageStats& stats = memoryManager->collectMemoryUsageStats();
  auto size = stats.ByteSizeLong();
  jbyteArray out = env->NewByteArray(size);
  uint8_t buffer[size];
  GLUTEN_CHECK(
      stats.SerializeToArray(reinterpret_cast<void*>(buffer), size),
      "Serialization failed when collecting memory usage stats");
  env->SetByteArrayRegion(out, 0, size, reinterpret_cast<jbyte*>(buffer));
  return out;
  JNI_METHOD_END(nullptr)
}

JNIEXPORT jlong JNICALL Java_io_glutenproject_memory_NativeMemoryJniWrapper_shrink( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerHandle,
    jlong size) {
  JNI_METHOD_START
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);
  return memoryManager->shrink(static_cast<int64_t>(size));
  JNI_METHOD_END(kInvalidResourceHandle)
}

JNIEXPORT void JNICALL Java_io_glutenproject_memory_NativeMemoryJniWrapper_release( // NOLINT
    JNIEnv* env,
    jclass,
    jlong memoryManagerHandle) {
  JNI_METHOD_START
  auto memoryManager = jniCastOrThrow<MemoryManager>(memoryManagerHandle);
  delete memoryManager;
  JNI_METHOD_END()
}

#ifdef __cplusplus
}
#endif
