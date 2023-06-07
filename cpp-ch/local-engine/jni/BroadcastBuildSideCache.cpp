#include <jni.h>
#include <jni/SharedPointerWrapper.h>
#include <jni/jni_common.h>
#include <Common/JNIUtils.h>

#include "BroadcastBuildSideCache.h"

static jclass Java_CHBroadcastBuildSideCache = nullptr;
static jmethodID Java_get = nullptr;
using namespace local_engine;

void BroadcastBuildSideCache::init(JNIEnv * env)
{
    /**
     * Scala object will be compiled into two classes, one is with '$' suffix which is normal class,
     * and one is utility class which only has static method.
     *
     * Here, we use utility class.
     */

    const char * classSig = "Lio/glutenproject/execution/CHBroadcastBuildSideCache;";
    Java_CHBroadcastBuildSideCache = CreateGlobalClassReference(env, classSig);
    Java_get = GetStaticMethodID(env, Java_CHBroadcastBuildSideCache, "get", "(Ljava/lang/String;)J");
}

void BroadcastBuildSideCache::destroy(JNIEnv * env)
{
    env->DeleteGlobalRef(Java_CHBroadcastBuildSideCache);
}


std::shared_ptr<StorageJoinWrapper> BroadcastBuildSideCache::get(const std::string & id)
{
    GET_JNIENV(env)
    jstring s = charTojstring(env, id.c_str());
    auto result = safeCallStaticLongMethod(env, Java_CHBroadcastBuildSideCache, Java_get, s);
    CLEAN_JNIENV

    return SharedPointerWrapper<StorageJoinWrapper>::get(result)->get();
}
