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
#include "JNIUtils.h"

namespace local_engine
{
JNIEnv * JNIUtils::getENV(int * attach)
{
    if (vm == nullptr)
        return nullptr;

    *attach = 0;
    JNIEnv * jni_env = nullptr;

    int status = vm->GetEnv(reinterpret_cast<void **>(&jni_env), JNI_VERSION_1_8);

    if (status == JNI_EDETACHED || jni_env == nullptr)
    {
        status = vm->AttachCurrentThread(reinterpret_cast<void **>(&jni_env), nullptr);
        if (status < 0)
        {
            jni_env = nullptr;
        }
        else
        {
            *attach = 1;
        }
    }
    return jni_env;
}

void JNIUtils::detachCurrentThread()
{
    vm->DetachCurrentThread();
}

}
