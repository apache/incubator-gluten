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

#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <execinfo.h>
#include <jni.h>

#include "compute/ProtobufUtils.h"
#include "compute/Runtime.h"
#include "config/GlutenConfig.h"
#include "memory/AllocationListener.h"
#include "shuffle/rss/RssClient.h"
#include "utils/Compression.h"
#include "utils/exception.h"

static jint jniVersion = JNI_VERSION_1_8;

static inline std::string jStringToCString(JNIEnv* env, jstring string) {
  int32_t jlen, clen;
  clen = env->GetStringUTFLength(string);
  jlen = env->GetStringLength(string);
  char buffer[clen];
  env->GetStringUTFRegion(string, 0, jlen, buffer);
  return std::string(buffer, clen);
}

static inline void checkException(JNIEnv* env) {
  if (env->ExceptionCheck()) {
    jthrowable t = env->ExceptionOccurred();
    env->ExceptionClear();
    jclass describerClass = env->FindClass("org/apache/gluten/exception/JniExceptionDescriber");
    jmethodID describeMethod =
        env->GetStaticMethodID(describerClass, "describe", "(Ljava/lang/Throwable;)Ljava/lang/String;");
    std::string description =
        jStringToCString(env, (jstring)env->CallStaticObjectMethod(describerClass, describeMethod, t));
    if (env->ExceptionCheck()) {
      LOG(WARNING) << "Fatal: Uncaught Java exception during calling the Java exception describer method! ";
    }
    throw gluten::GlutenException("Error during calling Java code from native code: " + description);
  }
}

static inline jclass createGlobalClassReference(JNIEnv* env, const char* className) {
  jclass localClass = env->FindClass(className);
  jclass globalClass = (jclass)env->NewGlobalRef(localClass);
  env->DeleteLocalRef(localClass);
  return globalClass;
}

static inline jclass createGlobalClassReferenceOrError(JNIEnv* env, const char* className) {
  jclass globalClass = createGlobalClassReference(env, className);
  if (globalClass == nullptr) {
    std::string errorMessage = "Unable to CreateGlobalClassReferenceOrError for" + std::string(className);
    throw gluten::GlutenException(errorMessage);
  }
  return globalClass;
}

static inline jmethodID getMethodId(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  jmethodID ret = env->GetMethodID(thisClass, name, sig);
  return ret;
}

static inline jmethodID getMethodIdOrError(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  jmethodID ret = getMethodId(env, thisClass, name, sig);
  if (ret == nullptr) {
    std::string errorMessage = "Unable to find method " + std::string(name) + " within signature" + std::string(sig);
    throw gluten::GlutenException(errorMessage);
  }
  return ret;
}

static inline jmethodID getStaticMethodId(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  jmethodID ret = env->GetStaticMethodID(thisClass, name, sig);
  return ret;
}

static inline jmethodID getStaticMethodIdOrError(JNIEnv* env, jclass thisClass, const char* name, const char* sig) {
  jmethodID ret = getStaticMethodId(env, thisClass, name, sig);
  if (ret == nullptr) {
    std::string errorMessage =
        "Unable to find static method " + std::string(name) + " within signature" + std::string(sig);
    throw gluten::GlutenException(errorMessage);
  }
  return ret;
}

static inline void attachCurrentThreadAsDaemonOrThrow(JavaVM* vm, JNIEnv** out) {
  int getEnvStat = vm->GetEnv(reinterpret_cast<void**>(out), jniVersion);
  if (getEnvStat == JNI_EDETACHED) {
    DLOG(INFO) << "JNIEnv was not attached to current thread.";
    // Reattach current thread to JVM
    getEnvStat = vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(out), NULL);
    if (getEnvStat != JNI_OK) {
      throw gluten::GlutenException("Failed to reattach current thread to JVM.");
    }
    DLOG(INFO) << "Succeeded attaching current thread.";
    return;
  }
  if (getEnvStat != JNI_OK) {
    throw gluten::GlutenException("Failed to attach current thread to JVM.");
  }
}

namespace gluten {

class JniCommonState {
 public:
  virtual ~JniCommonState() = default;

  void ensureInitialized(JNIEnv* env);

  void assertInitialized();

  void close();

  jmethodID runtimeAwareCtxHandle();

 private:
  void initialize(JNIEnv* env);

  jclass runtimeAwareClass_;
  jmethodID runtimeAwareCtxHandle_;

  JavaVM* vm_;
  bool initialized_{false};
  bool closed_{false};
  std::mutex mtx_;
};

inline JniCommonState* getJniCommonState() {
  static JniCommonState jniCommonState;
  return &jniCommonState;
}

Runtime* getRuntime(JNIEnv* env, jobject runtimeAware);

// Safe version of JNI {Get|Release}<PrimitiveType>ArrayElements routines.
// SafeNativeArray would release the managed array elements automatically
// during destruction.

enum class JniPrimitiveArrayType {
  kBoolean = 0,
  kByte = 1,
  kChar = 2,
  kShort = 3,
  kInt = 4,
  kLong = 5,
  kFloat = 6,
  kDouble = 7
};

#define CONCATENATE(t1, t2, t3) t1##t2##t3

#define DEFINE_PRIMITIVE_ARRAY(PRIM_TYPE, JAVA_TYPE, JNI_NATIVE_TYPE, NATIVE_TYPE, METHOD_VAR) \
  template <>                                                                                  \
  struct JniPrimitiveArray<JniPrimitiveArrayType::PRIM_TYPE> {                                 \
    using JavaType = JAVA_TYPE;                                                                \
    using JniNativeType = JNI_NATIVE_TYPE;                                                     \
    using NativeType = NATIVE_TYPE;                                                            \
                                                                                               \
    static JniNativeType get(JNIEnv* env, JavaType javaArray) {                                \
      return env->CONCATENATE(Get, METHOD_VAR, ArrayElements)(javaArray, nullptr);             \
    }                                                                                          \
                                                                                               \
    static void release(JNIEnv* env, JavaType javaArray, JniNativeType nativeArray) {          \
      env->CONCATENATE(Release, METHOD_VAR, ArrayElements)(javaArray, nativeArray, JNI_ABORT); \
    }                                                                                          \
  };

template <JniPrimitiveArrayType TYPE>
struct JniPrimitiveArray {};

DEFINE_PRIMITIVE_ARRAY(kBoolean, jbooleanArray, jboolean*, bool*, Boolean)
DEFINE_PRIMITIVE_ARRAY(kByte, jbyteArray, jbyte*, uint8_t*, Byte)
DEFINE_PRIMITIVE_ARRAY(kChar, jcharArray, jchar*, uint16_t*, Char)
DEFINE_PRIMITIVE_ARRAY(kShort, jshortArray, jshort*, int16_t*, Short)
DEFINE_PRIMITIVE_ARRAY(kInt, jintArray, jint*, int32_t*, Int)
DEFINE_PRIMITIVE_ARRAY(kLong, jlongArray, jlong*, int64_t*, Long)
DEFINE_PRIMITIVE_ARRAY(kFloat, jfloatArray, jfloat*, float_t*, Float)
DEFINE_PRIMITIVE_ARRAY(kDouble, jdoubleArray, jdouble*, double_t*, Double)

template <JniPrimitiveArrayType TYPE>
class SafeNativeArray {
  using PrimitiveArray = JniPrimitiveArray<TYPE>;
  using JavaArrayType = typename PrimitiveArray::JavaType;
  using JniNativeArrayType = typename PrimitiveArray::JniNativeType;
  using NativeArrayType = typename PrimitiveArray::NativeType;

 public:
  virtual ~SafeNativeArray() {
    PrimitiveArray::release(env_, javaArray_, nativeArray_);
  }

  SafeNativeArray(const SafeNativeArray&) = delete;
  SafeNativeArray(SafeNativeArray&&) = delete;
  SafeNativeArray& operator=(const SafeNativeArray&) = delete;
  SafeNativeArray& operator=(SafeNativeArray&&) = delete;

  const NativeArrayType elems() const {
    return reinterpret_cast<const NativeArrayType>(nativeArray_);
  }

  const jsize length() const {
    return env_->GetArrayLength(javaArray_);
  }

  static SafeNativeArray<TYPE> get(JNIEnv* env, JavaArrayType javaArray) {
    JniNativeArrayType nativeArray = PrimitiveArray::get(env, javaArray);
    return SafeNativeArray<TYPE>(env, javaArray, nativeArray);
  }

 private:
  SafeNativeArray(JNIEnv* env, JavaArrayType javaArray, JniNativeArrayType nativeArray)
      : env_(env), javaArray_(javaArray), nativeArray_(nativeArray){};

  JNIEnv* env_;
  JavaArrayType javaArray_;
  JniNativeArrayType nativeArray_;
};

#define DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(PRIM_TYPE, JAVA_TYPE, METHOD_VAR)                         \
  inline SafeNativeArray<JniPrimitiveArrayType::PRIM_TYPE> CONCATENATE(get, METHOD_VAR, ArrayElementsSafe)( \
      JNIEnv * env, JAVA_TYPE array) {                                                                      \
    return SafeNativeArray<JniPrimitiveArrayType::PRIM_TYPE>::get(env, array);                              \
  }

DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kBoolean, jbooleanArray, Boolean)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kByte, jbyteArray, Byte)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kChar, jcharArray, Char)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kShort, jshortArray, Short)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kInt, jintArray, Int)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kLong, jlongArray, Long)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kFloat, jfloatArray, Float)
DEFINE_SAFE_GET_PRIMITIVE_ARRAY_FUNCTIONS(kDouble, jdoubleArray, Double)

} // namespace gluten

// TODO: Move the static functions to namespace gluten

static inline void backtrace() {
  void* array[1024];
  auto size = backtrace(array, 1024);
  char** strings = backtrace_symbols(array, size);
  for (size_t i = 0; i < size; ++i) {
    LOG(INFO) << strings[i];
  }
  free(strings);
}

static inline arrow::Compression::type getCompressionType(JNIEnv* env, jstring codecJstr) {
  if (codecJstr == NULL) {
    return arrow::Compression::UNCOMPRESSED;
  }
  auto codec = env->GetStringUTFChars(codecJstr, JNI_FALSE);

  // Convert codec string into lowercase.
  std::string codecLower;
  std::transform(codec, codec + std::strlen(codec), std::back_inserter(codecLower), ::tolower);
  GLUTEN_ASSIGN_OR_THROW(auto compressionType, arrow::util::Codec::GetCompressionType(codecLower));

  env->ReleaseStringUTFChars(codecJstr, codec);
  return compressionType;
}

static inline gluten::CodecBackend getCodecBackend(JNIEnv* env, jstring codecJstr) {
  if (codecJstr == nullptr) {
    return gluten::CodecBackend::NONE;
  }
  auto codecBackend = jStringToCString(env, codecJstr);
  if (codecBackend == "qat") {
    return gluten::CodecBackend::QAT;
  } else if (codecBackend == "iaa") {
    return gluten::CodecBackend::IAA;
  } else {
    throw std::invalid_argument("Not support this codec backend " + codecBackend);
  }
}

static inline gluten::CompressionMode getCompressionMode(JNIEnv* env, jstring compressionModeJstr) {
  GLUTEN_DCHECK(compressionModeJstr != nullptr, "CompressionMode cannot be null");
  auto compressionMode = jStringToCString(env, compressionModeJstr);
  if (compressionMode == "buffer") {
    return gluten::CompressionMode::BUFFER;
  } else if (compressionMode == "rowvector") {
    return gluten::CompressionMode::ROWVECTOR;
  } else {
    throw std::invalid_argument("Not support this compression mode " + compressionMode);
  }
}

class SparkAllocationListener final : public gluten::AllocationListener {
 public:
  SparkAllocationListener(
      JavaVM* vm,
      jobject jListenerLocalRef,
      jmethodID jReserveMethod,
      jmethodID jUnreserveMethod,
      int64_t blockSize)
      : vm_(vm), jReserveMethod_(jReserveMethod), jUnreserveMethod_(jUnreserveMethod), blockSize_(blockSize) {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    jListenerGlobalRef_ = env->NewGlobalRef(jListenerLocalRef);
  }

  SparkAllocationListener(const SparkAllocationListener&) = delete;
  SparkAllocationListener(SparkAllocationListener&&) = delete;
  SparkAllocationListener& operator=(const SparkAllocationListener&) = delete;
  SparkAllocationListener& operator=(SparkAllocationListener&&) = delete;

  ~SparkAllocationListener() override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      LOG(WARNING) << "SparkAllocationListener#~SparkAllocationListener(): "
                   << "JNIEnv was not attached to current thread";
      return;
    }
    env->DeleteGlobalRef(jListenerGlobalRef_);
  }

  void allocationChanged(int64_t size) override {
    updateReservation(size);
  }

 private:
  int64_t reserve(int64_t diff) {
    std::lock_guard<std::mutex> lock(mutex_);
    bytesReserved_ += diff;
    int64_t newBlockCount;
    if (bytesReserved_ == 0) {
      newBlockCount = 0;
    } else {
      // ceil to get the required block number
      newBlockCount = (bytesReserved_ - 1) / blockSize_ + 1;
    }
    int64_t bytesGranted = (newBlockCount - blocksReserved_) * blockSize_;
    blocksReserved_ = newBlockCount;
    if (bytesReserved_ > maxBytesReserved_) {
      maxBytesReserved_ = bytesReserved_;
    }
    return bytesGranted;
  }

  void updateReservation(int64_t diff) {
    int64_t granted = reserve(diff);
    if (granted == 0) {
      return;
    }
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm_, &env);
    if (granted < 0) {
      env->CallLongMethod(jListenerGlobalRef_, jUnreserveMethod_, -granted);
      checkException(env);
      return;
    }
    env->CallLongMethod(jListenerGlobalRef_, jReserveMethod_, granted);
    checkException(env);
  }

  JavaVM* vm_;
  jobject jListenerGlobalRef_;
  jmethodID jReserveMethod_;
  jmethodID jUnreserveMethod_;
  int64_t blockSize_;
  int64_t blocksReserved_ = 0L;
  int64_t bytesReserved_ = 0L;
  int64_t maxBytesReserved_ = 0L;
  std::mutex mutex_;
};

class BacktraceAllocationListener final : public gluten::AllocationListener {
 public:
  BacktraceAllocationListener(std::unique_ptr<gluten::AllocationListener> delegator)
      : delegator_(std::move(delegator)) {}

  void allocationChanged(int64_t bytes) override {
    allocationBacktrace(bytes);
    delegator_->allocationChanged(bytes);
  }

 private:
  void allocationBacktrace(int64_t bytes) {
    allocatedBytes_ += bytes;
    if (bytes > (64L << 20)) {
      backtrace();
    } else if (allocatedBytes_ >= backtraceBytes_) {
      backtrace();
      backtraceBytes_ += (1L << 30);
    }
  }

  std::unique_ptr<gluten::AllocationListener> delegator_;
  std::atomic_int64_t allocatedBytes_{};
  std::atomic_int64_t backtraceBytes_{1L << 30};
};

class CelebornClient : public RssClient {
 public:
  CelebornClient(JavaVM* vm, jobject javaCelebornShuffleWriter, jmethodID javaCelebornPushPartitionDataMethod)
      : vm_(vm), javaCelebornPushPartitionData_(javaCelebornPushPartitionDataMethod) {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }

    javaCelebornShuffleWriter_ = env->NewGlobalRef(javaCelebornShuffleWriter);
    array_ = env->NewByteArray(1024 * 1024);
    array_ = static_cast<jbyteArray>(env->NewGlobalRef(array_));
  }

  ~CelebornClient() {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      LOG(WARNING) << "CelebornClient#~CelebornClient(): "
                   << "JNIEnv was not attached to current thread";
      return;
    }
    env->DeleteGlobalRef(javaCelebornShuffleWriter_);
    jbyte* byteArray = env->GetByteArrayElements(array_, NULL);
    env->ReleaseByteArrayElements(array_, byteArray, JNI_ABORT);
    env->DeleteGlobalRef(array_);
  }

  int32_t pushPartitionData(int32_t partitionId, char* bytes, int64_t size) override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
      throw gluten::GlutenException("JNIEnv was not attached to current thread");
    }
    jint length = env->GetArrayLength(array_);
    if (size > length) {
      jbyte* byteArray = env->GetByteArrayElements(array_, NULL);
      env->ReleaseByteArrayElements(array_, byteArray, JNI_ABORT);
      env->DeleteGlobalRef(array_);
      array_ = env->NewByteArray(size);
      array_ = static_cast<jbyteArray>(env->NewGlobalRef(array_));
    }
    env->SetByteArrayRegion(array_, 0, size, reinterpret_cast<jbyte*>(bytes));
    jint celebornBytesSize =
        env->CallIntMethod(javaCelebornShuffleWriter_, javaCelebornPushPartitionData_, partitionId, array_, size);
    checkException(env);
    return static_cast<int32_t>(celebornBytesSize);
  }

  void stop() override {}

 private:
  JavaVM* vm_;
  jobject javaCelebornShuffleWriter_;
  jmethodID javaCelebornPushPartitionData_;
  jbyteArray array_;
};
