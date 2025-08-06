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

#include <Core/Types.h>
#include <Core/Types_fwd.h>

namespace local_engine
{

static constexpr char * ENCODE_MAP = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
static constexpr Int64 BASE = 85L;
static constexpr Int64 BASE_2ND_POWER = 7225L; // 85^2
static constexpr Int64 BASE_3RD_POWER = 614125L; // 85^3
static constexpr Int64 BASE_4TH_POWER = 52200625L; // 85^4
static constexpr Int32 ASCII_BITMASK = 0x7F;

static String generateDecodeMap()
{
    // The bitmask is the same as largest possible value, so the length of the array must
    // be one greater.
    String result(ASCII_BITMASK + 1, 0xFF);
    for (UInt8 i = 0; ENCODE_MAP[i] != '\0'; ++i)
        result[ENCODE_MAP[i]] = i;

    return result;
}

static const String DECODE_MAP = generateDecodeMap();

class Base85Codec
{
public:
    static constexpr size_t ENCODED_UUID_LENGTH = 20;
    static String encodeUUID(const DB::UUID & uuid);
    static DB::UUID decodeUUID(const String & encoded);

private:
    static String encodeBlocks(const String & blocks);
    static String decodeBlocks(const String & encoded);
};
};
