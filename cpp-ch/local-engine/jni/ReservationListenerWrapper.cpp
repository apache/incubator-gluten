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
#include "ReservationListenerWrapper.h"
#include <jni/jni_common.h>
#include <Common/JNIUtils.h>

namespace local_engine
{
jclass ReservationListenerWrapper::reservation_listener_class = nullptr;
jmethodID ReservationListenerWrapper::reservation_listener_reserve = nullptr;
jmethodID ReservationListenerWrapper::reservation_listener_reserve_or_throw = nullptr;
jmethodID ReservationListenerWrapper::reservation_listener_unreserve = nullptr;

ReservationListenerWrapper::ReservationListenerWrapper(jobject listener_) : listener(listener_)
{
}

ReservationListenerWrapper::~ReservationListenerWrapper()
{
    GET_JNIENV(env)
    env->DeleteGlobalRef(listener);
    CLEAN_JNIENV
}

void ReservationListenerWrapper::reserve(int64_t size)
{
    GET_JNIENV(env)
    safeCallVoidMethod(env, listener, reservation_listener_reserve, size);
    CLEAN_JNIENV
}

void ReservationListenerWrapper::reserveOrThrow(int64_t size)
{
    GET_JNIENV(env)
    safeCallVoidMethod(env, listener, reservation_listener_reserve_or_throw, size);
    CLEAN_JNIENV
}

void ReservationListenerWrapper::free(int64_t size)
{
    GET_JNIENV(env)
    safeCallVoidMethod(env, listener, reservation_listener_unreserve, size);
    CLEAN_JNIENV
}
}
