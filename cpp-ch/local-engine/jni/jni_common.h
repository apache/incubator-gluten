#pragma once
#include <exception>
#include <stdexcept>
#include <jni.h>
#include <string>
#include <Common/Exception.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{
jclass CreateGlobalExceptionClassReference(JNIEnv *env, const char *class_name);

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name);

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig);

jmethodID GetStaticMethodID(JNIEnv * env, jclass this_class, const char * name, const char * sig);

jstring charTojstring(JNIEnv* env, const char* pat);

jbyteArray stringTojbyteArray(JNIEnv* env, const std::string & str);

#define LOCAL_ENGINE_JNI_JMETHOD_START
#define LOCAL_ENGINE_JNI_JMETHOD_END(env) \
    if ((env)->ExceptionCheck())\
    {\
        LOG_ERROR(&Poco::Logger::get("local_engine"), "Enter java exception handle.");\
        auto throwable = (env)->ExceptionOccurred();\
        jclass exceptionClass = (env)->FindClass("java/lang/Exception"); \
        jmethodID getMessageMethod = (env)->GetMethodID(exceptionClass, "getMessage", "()Ljava/lang/String;"); \
        jstring message = static_cast<jstring>((env)->CallObjectMethod(throwable, getMessageMethod)); \
        const char *messageChars = (env)->GetStringUTFChars(message, NULL); \
        LOG_ERROR(&Poco::Logger::get("jni"), "exception:{}", messageChars); \
        (env)->ReleaseStringUTFChars(message, messageChars); \
        (env)->Throw(throwable);\
    }

template <typename ... Args>
jobject safeCallObjectMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args ... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallObjectMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env)
    return ret;
}

template <typename ... Args>
jboolean safeCallBooleanMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args ... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallBooleanMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

template <typename ... Args>
jlong safeCallLongMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args ... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallLongMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

template <typename ... Args>
jint safeCallIntMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args ... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    auto ret = env->CallIntMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
    return ret;
}

template <typename ... Args>
void safeCallVoidMethod(JNIEnv * env, jobject obj, jmethodID method_id, Args ... args)
{
    LOCAL_ENGINE_JNI_JMETHOD_START
    env->CallVoidMethod(obj, method_id, args...);
    LOCAL_ENGINE_JNI_JMETHOD_END(env);
}
}
