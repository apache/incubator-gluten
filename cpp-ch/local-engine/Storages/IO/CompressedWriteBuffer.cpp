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
#include <city.h>
#include <cstring>

#include <base/types.h>
#include <base/unaligned.h>
#include <base/defines.h>

#include <IO/WriteHelpers.h>

#include <Compression/CompressionFactory.h>
#include <Common/Stopwatch.h>
#include "CompressedWriteBuffer.h"

using namespace DB;

namespace local_engine
{

void CompressedWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    chassert(offset() <= INT_MAX);
    UInt32 decompressed_size = static_cast<UInt32>(offset());
    UInt32 compressed_reserve_size = codec->getCompressedReserveSize(decompressed_size);

    /** During compression we need buffer with capacity >= compressed_reserve_size + CHECKSUM_SIZE.
      *
      * If output buffer has necessary capacity, we can compress data directly into the output buffer.
      * Then we can write checksum at the output buffer begin.
      *
      * If output buffer does not have necessary capacity. Compress data into a temporary buffer.
      * Then we can write checksum and copy the temporary buffer into the output buffer.
      */
    Stopwatch compress_time_watch;
    if (out.available() >= compressed_reserve_size + sizeof(CityHash_v1_0_2::uint128))
    {
        char * out_compressed_ptr = out.position() + sizeof(CityHash_v1_0_2::uint128);
        UInt32 compressed_size = codec->compress(working_buffer.begin(), decompressed_size, out_compressed_ptr);
        compress_time += compress_time_watch.elapsedNanoseconds();
        CityHash_v1_0_2::uint128 checksum_(0,0);
        if (checksum)
        {
            checksum_ = CityHash_v1_0_2::CityHash128(out_compressed_ptr, compressed_size);


        }
        writeBinaryLittleEndian(checksum_.low64, out);
        writeBinaryLittleEndian(checksum_.high64, out);

        out.position() += compressed_size;
    }
    else
    {
        compressed_buffer.resize_exact(compressed_reserve_size);
        UInt32 compressed_size = codec->compress(working_buffer.begin(), decompressed_size, compressed_buffer.data());
        compress_time += compress_time_watch.elapsedNanoseconds();
        CityHash_v1_0_2::uint128 checksum_(0,0);
        if (checksum)
        {
            checksum_ = CityHash_v1_0_2::CityHash128(compressed_buffer.data(), compressed_size);
        }
        writeBinaryLittleEndian(checksum_.low64, out);
        writeBinaryLittleEndian(checksum_.high64, out);

        Stopwatch write_time_watch;
        out.write(compressed_buffer.data(), compressed_size);
        write_time += write_time_watch.elapsedNanoseconds();
    }
}

CompressedWriteBuffer::~CompressedWriteBuffer()
{
    finalize();
}

CompressedWriteBuffer::CompressedWriteBuffer(WriteBuffer & out_, CompressionCodecPtr codec_, size_t buf_size, bool checksum_)
    : BufferWithOwnMemory<WriteBuffer>(buf_size), out(out_), codec(std::move(codec_)), checksum(checksum_)
{
}

}
