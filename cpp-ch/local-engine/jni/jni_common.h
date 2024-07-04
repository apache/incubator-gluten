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
