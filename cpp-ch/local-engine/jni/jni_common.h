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
jlong safeCallStaticLongMethod(JNIEnv * env, jclass clazz, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallStaticLongMethod(clazz, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env)
    return ret;
}
}
