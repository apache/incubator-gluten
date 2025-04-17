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
#include <jni/jni_common.h>


namespace local_engine::delta
{
struct Codec
{
    struct Base85Codec
    {
        static constexpr Int32 ENCODED_UUID_LENGTH = 20;
    };
};


static constexpr String UUID_DV_MARKER = "u";

class DeltaUtil
{
public:
    static void initJavaCallerReference(JNIEnv * env);
    static void releaseJavaCallerReference(JNIEnv * env);

    static String encodeUUID(String uuid, String prefix);
    static String decodeUUID(String encodedUuid);

private:
    static jclass delta_jni_class;
    static jmethodID delta_jni_encode_uuid;
    static jmethodID delta_jni_decode_uuid;
};

}
