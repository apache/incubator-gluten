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
#include <jni.h>
#include <Common/JNIUtils.h>

namespace local_engine
{
class CelebornClient {
public:
    CelebornClient(jobject java_celeborn_pusher_, jmethodID java_celeborn_push_partition_data_method_)
        : java_celeborn_push_partition_data_method(java_celeborn_push_partition_data_method_) {
        GET_JNIENV(env)
        java_celeborn_pusher = env->NewGlobalRef(java_celeborn_pusher_);
        CLEAN_JNIENV
    }

    ~CelebornClient() {
        GET_JNIENV(env)
        env->DeleteGlobalRef(java_celeborn_pusher);
        CLEAN_JNIENV
    }

    size_t pushPartitionData(size_t partitionId, char* bytes, size_t size) const {
        GET_JNIENV(env)
        // TODO should reuse jbyteArray
        auto int_size = static_cast<jint>(size);
        jbyteArray array = env->NewByteArray(int_size);
        env->SetByteArrayRegion(array, 0, int_size, reinterpret_cast<jbyte*>(bytes));
        jint celeborn_bytes =
            env->CallIntMethod(java_celeborn_pusher, java_celeborn_push_partition_data_method, partitionId, array);
        CLEAN_JNIENV
        return celeborn_bytes;
    }

    jobject java_celeborn_pusher;
    jmethodID java_celeborn_push_partition_data_method;
};
}

