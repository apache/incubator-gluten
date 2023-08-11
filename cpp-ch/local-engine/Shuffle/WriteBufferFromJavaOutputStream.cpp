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
#include "WriteBufferFromJavaOutputStream.h"
#include <jni/jni_common.h>
#include <Common/JNIUtils.h>

namespace local_engine
{
jclass WriteBufferFromJavaOutputStream::output_stream_class = nullptr;
jmethodID WriteBufferFromJavaOutputStream::output_stream_write = nullptr;
jmethodID WriteBufferFromJavaOutputStream::output_stream_flush = nullptr;

void WriteBufferFromJavaOutputStream::nextImpl()
{
    GET_JNIENV(env)
    size_t bytes_write = 0;
    while (offset() - bytes_write > 0)
    {
        jint copy_num = static_cast<jint>(std::min(offset() - bytes_write, buffer_size));
        env->SetByteArrayRegion(buffer, 0, copy_num, reinterpret_cast<const jbyte *>(this->working_buffer.begin() + bytes_write));
        safeCallVoidMethod(env, output_stream, output_stream_write, buffer, 0, copy_num);
        bytes_write += copy_num;
    }
    CLEAN_JNIENV
}
WriteBufferFromJavaOutputStream::WriteBufferFromJavaOutputStream(jobject output_stream_, jbyteArray buffer_, size_t customize_buffer_size)
{
    GET_JNIENV(env)
    buffer = static_cast<jbyteArray>(env->NewGlobalRef(buffer_));
    output_stream = env->NewGlobalRef(output_stream_);
    buffer_size = customize_buffer_size;
    CLEAN_JNIENV
}
void WriteBufferFromJavaOutputStream::finalizeImpl()
{
    next();
    GET_JNIENV(env)
    safeCallVoidMethod(env, output_stream, output_stream_flush);
    CLEAN_JNIENV
}
WriteBufferFromJavaOutputStream::~WriteBufferFromJavaOutputStream()
{
    GET_JNIENV(env)
    env->DeleteGlobalRef(output_stream);
    env->DeleteGlobalRef(buffer);
    CLEAN_JNIENV
}
}
