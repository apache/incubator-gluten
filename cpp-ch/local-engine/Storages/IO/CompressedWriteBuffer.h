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

#include <memory>

#include <Common/PODArray.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>


namespace local_engine
{

class CompressedWriteBuffer final : public DB::BufferWithOwnMemory<DB::WriteBuffer>
{
public:
    explicit CompressedWriteBuffer(
        DB::WriteBuffer & out_,
        DB::CompressionCodecPtr codec_ = DB::CompressionCodecFactory::instance().getDefaultCodec(),
        size_t buf_size = DB::DBMS_DEFAULT_BUFFER_SIZE,
        bool checksum = false);

    ~CompressedWriteBuffer() override;

    /// The amount of compressed data
    size_t getCompressedBytes()
    {
        nextIfAtEnd();
        return out.count();
    }

    /// How many uncompressed bytes were written to the buffer
    size_t getUncompressedBytes()
    {
        return count();
    }

    /// How many bytes are in the buffer (not yet compressed)
    size_t getRemainingBytes()
    {
        nextIfAtEnd();
        return offset();
    }

    size_t getCompressTime() const
    {
        return compress_time;
    }

    size_t getWriteTime() const
    {
        return write_time;
    }

private:
    void nextImpl() override;

    WriteBuffer & out;
    DB::CompressionCodecPtr codec;

    DB::PODArray<char> compressed_buffer;
    bool checksum;
    size_t compress_time = 0;
    size_t write_time = 0;
};

}
