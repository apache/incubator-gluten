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
#include <base/find_symbols.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Parser/AdvancedParametersParseUtil.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace local_engine
{

template<typename T>
void tryAssign(const std::unordered_map<String, String> & kvs, const String & key, T & v);

template<>
void tryAssign<String>(const std::unordered_map<String, String> & kvs, const String & key, String & v)
{
    auto it = kvs.find(key);
    if (it != kvs.end())
        v = it->second;
}

template<>
void tryAssign<bool>(const std::unordered_map<String, String> & kvs, const String & key, bool & v)
{
    auto it = kvs.find(key);
    if (it != kvs.end())
    {
        if (it->second == "0" || it->second == "false" || it->second == "FALSE")
        {
            v = false;
        }
        else
        {
            v = true;
        }
    }
}

template <char... chars>
void readStringUntilCharsInto(String & s, DB::ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<chars...>(buf.position(), buf.buffer().end());

        s.append(buf.position(), next_pos - buf.position());
        buf.position() = next_pos;

        if (buf.hasPendingData())
            return;
    }
}

/// In the format: Seg1:k1=v1\nk2=v2\n..\nSeg2:k1=v1\n...
std::unordered_map<String, std::unordered_map<String, String>> convertToKVs(const String & advance)
{
    std::unordered_map<String, std::unordered_map<String, String>> res;
    std::unordered_map<String, String> *kvs;
    DB::ReadBufferFromString in(advance);
    while(!in.eof())
    {
        String key;
        readStringUntilCharsInto<'=', '\n', ':'>(key, in);
        if (key.empty())
        {
            if (!in.eof())
            {
                char c;
                DB::readChar(c, in);
            }
            continue;
        }

        char c;
        DB::readChar(c, in);
        if (c == ':')
        {
            res[key] = {};
            kvs = &res[key];
            continue;
        }

        if (c != '=')
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid format, = is expected: {}", advance);

        String value;
        readStringUntilCharsInto<'\n'>(value, in);
        (*kvs)[key] = value;
    }
    return res;
}

JoinOptimizationInfo JoinOptimizationInfo::parse(const String & advance)
{
    JoinOptimizationInfo info;
    auto kkvs = convertToKVs(advance);
    auto & kvs = kkvs["JoinParameters"];
    tryAssign(kvs, "isBHJ", info.is_broadcast);
    tryAssign(kvs, "isSMJ", info.is_smj);
    tryAssign(kvs, "buildHashTableId", info.storage_join_key);
    tryAssign(kvs, "isNullAwareAntiJoin", info.is_null_aware_anti_join);
    tryAssign(kvs, "isExistenceJoin", info.is_existence_join);
    return info;
}
}

