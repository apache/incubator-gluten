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
#include "SparkBackendSettings.h"
#include "jni_common.h"
#include <Common/JNIUtils.h>

namespace local_engine
{
jclass SparkBackendSettings::backend_settings_class = nullptr;
jmethodID SparkBackendSettings::get_long_conf_method = nullptr;

long SparkBackendSettings::getLongConf(const std::string & key, long default_value)
{
    GET_JNIENV(env)
    auto res = safeCallStaticLongMethod(env, backend_settings_class, get_long_conf_method, charTojstring(env, key.c_str()), default_value);
    CLEAN_JNIENV
    return res;
}
}
