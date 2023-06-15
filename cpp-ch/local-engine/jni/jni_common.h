#pragma once
#include <exception>
#include <stdexcept>
#include <string>
#include <jni.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/ExceptionUtils.h>

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
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

template <typename... Args>
jlong safeCallLongMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallLongMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

template <typename... Args>
jint safeCallIntMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallIntMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

template <typename... Args>
void safeCallVoidMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    env->CallVoidMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
}

template <typename... Args>
jlong safeCallStaticLongMethod(JNIEnv * env, jclass clazz, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallStaticLongMethod(clazz, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

template <typename... Args>
jobject safeCallStaticObjectMethod(JNIEnv * env, jclass clazz, jmethodID method_id, Args... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallStaticObjectMethod(clazz, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

[[maybe_unused]] static std::string jstring2string(JNIEnv * env, jstring jStr)
{
    try
    {
        if (!jStr)
            return "";
        
        jclass string_class = env->GetObjectClass(jStr);
        jmethodID get_bytes = env->GetMethodID(string_class, "getBytes", "(Ljava/lang/String;)[B");
        jbyteArray string_jbytes
            = static_cast<jbyteArray>(local_engine::safeCallObjectMethod(env, jStr, get_bytes, env->NewStringUTF("UTF-8")));

        size_t length = static_cast<size_t>(env->GetArrayLength(string_jbytes));
        jbyte * p_bytes = env->GetByteArrayElements(string_jbytes, nullptr);

        std::string ret = std::string(reinterpret_cast<char *>(p_bytes), length);
        env->ReleaseByteArrayElements(string_jbytes, p_bytes, JNI_ABORT);

        env->DeleteLocalRef(string_jbytes);
        env->DeleteLocalRef(string_class);
        return ret;
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }
}

[[maybe_unused]] static jstring stringTojstring(JNIEnv * env, const char * pat)
{
    try
    {
        jclass strClass = (env)->FindClass("java/lang/String");
        jmethodID ctorID = (env)->GetMethodID(strClass, "<init>", "([BLjava/lang/String;)V");
        jbyteArray bytes = (env)->NewByteArray(strlen(pat));
        (env)->SetByteArrayRegion(bytes, 0, strlen(pat), reinterpret_cast<const jbyte *>(pat));
        jstring encoding = (env)->NewStringUTF("UTF-8");
        return static_cast<jstring>((env)->NewObject(strClass, ctorID, bytes, encoding));
    }
    catch (DB::Exception & e)
    {
        local_engine::ExceptionUtils::handleException(e);
    }
}

}
