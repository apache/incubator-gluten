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
#include <stdexcept>
#include <jni.h>
#include <jni/jni_common.h>
#include <jni/jni_error.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Resource/JVMClassReference.h>

namespace local_engine
{

jclass JniErrorsGlobalState::getIOExceptionClass()
{
    return JVM_CLASS_REFERENCE(io_exception_class)();
}
jclass JniErrorsGlobalState::getRuntimeExceptionClass()
{
    return JVM_CLASS_REFERENCE(runtime_exception_class)();
}
jclass JniErrorsGlobalState::getUnsupportedOperationExceptionClass()
{
    return JVM_CLASS_REFERENCE(unsupportedoperation_exception_class)();
}
jclass JniErrorsGlobalState::getIllegalAccessExceptionClass()
{
    return JVM_CLASS_REFERENCE(illegal_access_exception_class)();
}
jclass JniErrorsGlobalState::getIllegalArgumentExceptionClass()
{
    return JVM_CLASS_REFERENCE(illegal_argument_exception_class)();
}

void JniErrorsGlobalState::throwException(JNIEnv * env, const DB::Exception & e)
{
    throwRuntimeException(env, e.message(), e.getStackTraceString());
}

void JniErrorsGlobalState::throwException(JNIEnv * env, const std::exception & e)
{
    throwRuntimeException(env, e.what(), DB::getExceptionStackTraceString(e));
}

void JniErrorsGlobalState::throwException(
    JNIEnv * env, jclass exception_class, const std::string & message, const std::string & stack_trace)
{
    if (exception_class)
    {
        std::string error_msg = message + "\n" + stack_trace;
        env->ThrowNew(exception_class, error_msg.c_str());
    }
    else
    {
        // This will cause a coredump
        throw std::runtime_error("Not found java runtime exception class");
    }
}

void JniErrorsGlobalState::throwRuntimeException(JNIEnv * env, const std::string & message, const std::string & stack_trace)
{
    throwException(env, JVM_CLASS_REFERENCE(runtime_exception_class)(), message, stack_trace);
}


}
