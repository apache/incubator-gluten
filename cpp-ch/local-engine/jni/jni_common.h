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
#include <exception>
#include <stdexcept>
#include <string>
#include <jni.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <mutex>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
jclass CreateGlobalExceptionClassReference(JNIEnv * env, const char * class_name);

jclass CreateGlobalClassReference(JNIEnv * env, const char * class_name);

jmethodID GetMethodID(JNIEnv * env, jclass this_class, const char * name, const char * sig);

jmethodID GetStaticMethodID(JNIEnv * env, jclass this_class, const char * name, const char * sig);

jstring charTojstring(JNIEnv * env, const char * pat);

jbyteArray stringTojbyteArray(JNIEnv * env, const std::string & str);

// A watcher to monitor JNIEnv status: it's active or not
class JniEnvStatusWatcher
{
public:
    static JniEnvStatusWatcher & instance();
    void setActive() { is_active.store(true, std::memory_order_release); }
    void setInactive() { is_active.store(false, std::memory_order_release); }
    bool isActive() const { return is_active.load(std::memory_order_acquire); }
protected:
    JniEnvStatusWatcher() = default;
    ~JniEnvStatusWatcher() = default;
    std::atomic<bool> is_active = false;
};

// A counter to track ongoing JNI method calls
// It help to ensure that no JNI method is running when we are destroying the JNI resources
class JniMethodCallCounter
{
public:
    static JniMethodCallCounter & instance();
    void increment() { counter.fetch_add(1, std::memory_order_acq_rel); }
    void decrement() {
        auto old_val = counter.fetch_sub(1, std::memory_order_acq_rel);
        if (old_val == 1)
        {
            std::lock_guard<std::mutex> lock(mutex);
            cv.notify_all();
        }
        else if (old_val < 1) [[unlikely]]
        {
            // This should never happen
            LOG_ERROR(getLogger("jni"), "JniMethodCallCounter counter underflow: {}", old_val);
        }
    }
    // This will only be called in the destroy JNI method to wait for all ongoing JNI method calls to finish
    void waitForZero()
    {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [this] {
          return counter.load(std::memory_order_acquire) <= 0;
        });
    }
protected:
    JniMethodCallCounter() = default;
    ~JniMethodCallCounter() = default;
private:
    std::atomic<int64_t> counter{0};
    std::mutex mutex;
    std::condition_variable cv;
};

class JniMethodGuard
{
public:
    JniMethodGuard();
    ~JniMethodGuard();
    bool couldInvoke();
};

#define LOCAL_ENGINE_JNI_DESTROY_METHOD_START \
    local_engine::JniEnvStatusWatcher::instance().setInactive(); \
    local_engine::JniMethodCallCounter::instance().waitForZero(); \
    do \
    { \
        try \
        {

#define LOCAL_ENGINE_JNI_METHOD_START \
    local_engine::JniMethodGuard jni_method_guard; \
    do \
    { \
        try \
        { \
            if (!jni_method_guard.couldInvoke()) \
            { \
                LOG_ERROR(getLogger("jni"), "Call JNI method {} when JNIEnv is not active!", __FUNCTION__); \
                break; \
            }   

#define LOCAL_ENGINE_JNI_METHOD_END(env, ret) \
        } \
        catch (DB::Exception & e) \
        { \
            local_engine::JniErrorsGlobalState::instance().throwException(env, e); \
            break; \
        } \
        catch (std::exception & e) \
        { \
            local_engine::JniErrorsGlobalState::instance().throwException(env, e); \
            break; \
        } \
        catch (...) \
        { \
            DB::WriteBufferFromOwnString ostr; \
            auto trace = boost::stacktrace::stacktrace(); \
            boost::stacktrace::detail::to_string(&trace.as_vector()[0], trace.size()); \
            local_engine::JniErrorsGlobalState::instance().throwRuntimeException(env, "Unknown Exception", ostr.str().c_str()); \
            break; \
        } \
    } while (0); \
    return ret;

#define LOCAL_ENGINE_JNI_JMETHOD_START
#define LOCAL_ENGINE_JNI_JMETHOD_END(env) \
    if ((env)->ExceptionCheck()) \
    { \
        LOG_ERROR(&Poco::Logger::get("local_engine"), "Enter java exception handle."); \
        auto excp = (env)->ExceptionOccurred(); \
        (env)->ExceptionDescribe(); \
        (env)->ExceptionClear(); \
        jclass cls = (env)->GetObjectClass(excp); \
        jmethodID mid = env->GetMethodID(cls, "toString", "()Ljava/lang/String;"); \
        jstring jmsg = static_cast<jstring>((env)->CallObjectMethod(excp, mid)); \
        const char * nmsg = (env)->GetStringUTFChars(jmsg, NULL); \
        std::string msg = std::string(nmsg); \
        env->ReleaseStringUTFChars(jmsg, nmsg); \
        throw DB::Exception::createRuntime(DB::ErrorCodes::LOGICAL_ERROR, msg); \
    }

#define TRY_LOCAL_ENGINE_JNI_JMETHOD_START
#define TRY_LOCAL_ENGINE_JNI_JMETHOD_END(env) \
    if ((env)->ExceptionCheck()) \
    { \
        LOG_ERROR(&Poco::Logger::get("local_engine"), "Enter java exception handle."); \
        auto excp = (env)->ExceptionOccurred(); \
        (env)->ExceptionDescribe(); \
        (env)->ExceptionClear(); \
        jclass cls = (env)->GetObjectClass(excp); \
        jmethodID mid = env->GetMethodID(cls, "toString", "()Ljava/lang/String;"); \
        jstring jmsg = static_cast<jstring>((env)->CallObjectMethod(excp, mid)); \
        const char * nmsg = (env)->GetStringUTFChars(jmsg, NULL); \
        std::string msg = std::string(nmsg); \
        env->ReleaseStringUTFChars(jmsg, nmsg); \
        LOG_WARNING(&Poco::Logger::get("local_engine"), "Ignore java exception: {}", msg); \
    }


template <typename... Args>
jobject safeCallObjectMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallObjectMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env)
    return ret;
}

template <typename... Args>
jboolean safeCallBooleanMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallBooleanMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env)
    return ret;
}

template <typename... Args>
jlong safeCallLongMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallLongMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env)
    return ret;
}

template <typename... Args>
jint safeCallIntMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallIntMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env)
    return ret;
}

template <typename... Args>
void safeCallVoidMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    env->CallVoidMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env)
}

template <typename... Args>
void tryCallVoidMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args... args)
{
    TRY_LOCAL_ENGINE_JNI_JMETHOD_START
    env->CallVoidMethod(obj, method_id, args...);
    TRY_LOCAL_ENGINE_JNI_JMETHOD_END(env)
}

template <typename... Args>
jlong safeCallStaticLongMethod(JNIEnv * env, jclass clazz, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallStaticLongMethod(clazz, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env)
    return ret;
}

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

  NativeArrayType elems() const {
    return reinterpret_cast<const NativeArrayType>(nativeArray_);
  }

  jsize length() const {
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
}
