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
#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeReader.h>
#include <IO/ReadBuffer.h>
#include <Common/BlockIterator.h>


namespace local_engine
{
class ReadBufferFromJavaInputStream;
class ShuffleReader : BlockIterator
{
public:
    explicit ShuffleReader(std::unique_ptr<DB::ReadBuffer> in_, bool compressed);
    DB::Block * read();
    ~ShuffleReader();
    static jclass input_stream_class;
    static jmethodID input_stream_read;
    std::unique_ptr<DB::ReadBuffer> in;

private:
    std::unique_ptr<DB::CompressedReadBuffer> compressed_in;
    std::unique_ptr<DB::NativeReader> input_stream;
    DB::Block header;
};


class ReadBufferFromJavaInputStream : public DB::BufferWithOwnMemory<DB::ReadBuffer>
{
public:
    explicit ReadBufferFromJavaInputStream(jobject input_stream, size_t customize_buffer_size);
    ~ReadBufferFromJavaInputStream() override;

private:
    jobject java_in;
    size_t buffer_size;
    int readFromJava();
    bool nextImpl() override;
};

}
