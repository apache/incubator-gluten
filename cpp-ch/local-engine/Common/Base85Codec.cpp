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
#include "Base85Codec.h"

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace local_engine
{
String uuidToByteBuffer(const DB::UUID & uuid)
{
    const UInt128 under_type = uuid.toUnderType();
    long low = under_type.items[0];
    long high = under_type.items[1];

    String result(16, '\0');

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    const Int64 low_swap = __builtin_bswap64(low);
    const Int64 high_swap = __builtin_bswap64(high);
    memcpy(result.data(), &low_swap, 8);
    memcpy(result.data() + 8, &high_swap, 8);
#else
    memcpy(result.data(), &high, 8);
    memcpy(result.data() + 8, &low, 8);
#endif

    return result;
}

DB::UUID uuidFromByteBuffer(const String & buffer)
{
    chassert(buffer.size() >= 16);
    DB::ReadBufferFromString buf(buffer);
    Int64 lowBits;
    Int64 highBits;
    DB::readBinaryBigEndian(lowBits, buf);
    DB::readBinaryBigEndian(highBits, buf);

    DB::UUID uuid;
    uuid.toUnderType().items[0] = lowBits;
    uuid.toUnderType().items[1] = highBits;
    return uuid;
}


String Base85Codec::encodeUUID(const DB::UUID & uuid)
{
    const String blocks = uuidToByteBuffer(uuid);
    return encodeBlocks(blocks);
}

DB::UUID Base85Codec::decodeUUID(const String & encoded)
{
    const String blocks = decodeBlocks(encoded);
    return uuidFromByteBuffer(blocks);
}

String Base85Codec::encodeBlocks(const String & blocks)
{
    chassert(blocks.size() % 4 == 0);
    auto numBlocks = blocks.size() / 4;
    // Every 4 byte block gets encoded into 5 bytes/chars
    const auto outputLength = numBlocks * 5;
    String output(outputLength, '\0');
    size_t outputIndex = 0;

    DB::ReadBufferFromString rb(blocks);
    while (!rb.eof())
    {
        Int32 readInt;
        DB::readBinaryBigEndian(readInt, rb);
        Int64 sum = readInt & 0x00000000ffffffffL;
        output[outputIndex] = ENCODE_MAP[static_cast<Int32>(sum / BASE_4TH_POWER)];
        sum %= BASE_4TH_POWER;

        output[outputIndex + 1] = ENCODE_MAP[static_cast<Int32>(sum / BASE_3RD_POWER)];
        sum %= BASE_3RD_POWER;
        output[outputIndex + 2] = ENCODE_MAP[static_cast<Int32>(sum / BASE_2ND_POWER)];
        sum %= BASE_2ND_POWER;
        output[outputIndex + 3] = ENCODE_MAP[static_cast<Int32>(sum / BASE)];
        output[outputIndex + 4] = ENCODE_MAP[static_cast<Int32>(sum % BASE)];
        outputIndex += 5;
    }

    return output;
}

String Base85Codec::decodeBlocks(const String & encoded)
{
    chassert(encoded.size() % 5 == 0);
    String result(encoded.size() / 5 * 4, '\0');

    // A mechanism to detect invalid characters in the input while decoding, that only has a
    // single conditional at the very end, instead of branching for every character.
    Int32 canary = 0;
    auto decodeInputChar = [&encoded, &canary](Int32 i) -> Int64
    {
        const auto c = encoded[i];

        canary |= c; // non-ascii char has bits outside of ASCII_BITMASK
        const auto b = DECODE_MAP[c & ASCII_BITMASK];
        canary |= b; // invalid char maps to -1, which has bits outside ASCII_BITMASK
        return static_cast<Int64>(b);
    };

    Int32 inputIndex = 0;
    DB::WriteBufferFromString buf(result);
    while (buf.hasPendingData())
    {
        Int64 sum = 0L;
        sum += decodeInputChar(inputIndex) * BASE_4TH_POWER;
        sum += decodeInputChar(inputIndex + 1) * BASE_3RD_POWER;
        sum += decodeInputChar(inputIndex + 2) * BASE_2ND_POWER;
        sum += decodeInputChar(inputIndex + 3) * BASE;
        sum += decodeInputChar(inputIndex + 4);
        DB::writeBinaryBigEndian(static_cast<Int32>(sum), buf);
        inputIndex += 5;
    }
    buf.finalize();
    return result;
}

}