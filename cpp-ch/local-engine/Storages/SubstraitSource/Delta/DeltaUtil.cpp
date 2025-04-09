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

#include "DeltaUtil.h"

#include <Compression/CompressedReadBuffer.h>
#include <Core/Block.h>
#include <IO/ReadBuffer.h>
#include <jni/jni_common.h>
#include <Common/JNIUtils.h>

namespace local_engine::delta
{

jclass DeltaUtil::delta_jni_class = nullptr;
jmethodID DeltaUtil::delta_jni_encode_uuid = nullptr;
jmethodID DeltaUtil::delta_jni_decode_uuid = nullptr;

void DeltaUtil::initJavaCallerReference(JNIEnv * env)
{
    delta_jni_class = CreateGlobalClassReference(env, "Lorg/apache/gluten/vectorized/DeltaWriterJNIWrapper;");
    delta_jni_encode_uuid
        = GetStaticMethodID(env, delta_jni_class, "encodeUUID", "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");
    delta_jni_decode_uuid = GetStaticMethodID(env, delta_jni_class, "decodeUUID", "(Ljava/lang/String;)Ljava/lang/String;");
}

void DeltaUtil::releaseJavaCallerReference(JNIEnv * env)
{
    if (delta_jni_class)
        env->DeleteGlobalRef(delta_jni_class);
}

String DeltaUtil::encodeUUID(String uuid, String prefix)
{
    GET_JNIENV(env)
    jstring jUuid = env->NewStringUTF(uuid.c_str());
    jstring jRandomPrefix = env->NewStringUTF(prefix.c_str());

    // Call the static Java method
    jstring jResult = (jstring)env->CallStaticObjectMethod(delta_jni_class, delta_jni_encode_uuid, jUuid, jRandomPrefix);
    const char * resultCStr = env->GetStringUTFChars(jResult, nullptr);
    std::string encode(resultCStr);
    env->ReleaseStringUTFChars(jResult, resultCStr);
    env->DeleteLocalRef(jUuid);
    env->DeleteLocalRef(jRandomPrefix);
    env->DeleteLocalRef(jResult);
    CLEAN_JNIENV
    return encode;
}

String DeltaUtil::decodeUUID(String encodedUuid)
{
    GET_JNIENV(env)
    jstring j_encoded_uuid = env->NewStringUTF(encodedUuid.c_str());
    jstring jResult = (jstring)env->CallStaticObjectMethod(delta_jni_class, delta_jni_decode_uuid, j_encoded_uuid);
    const char * resultCStr = env->GetStringUTFChars(jResult, nullptr);
    std::string decode_uuid(resultCStr);

    env->ReleaseStringUTFChars(jResult, resultCStr);
    env->DeleteLocalRef(j_encoded_uuid);
    env->DeleteLocalRef(jResult);
    CLEAN_JNIENV
    return decode_uuid;
}

}