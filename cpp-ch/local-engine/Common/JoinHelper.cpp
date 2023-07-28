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
#include "JoinHelper.h"
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

using namespace DB;

namespace local_engine
{
JoinOptimizationInfo parseJoinOptimizationInfo(const std::string & optimization)
{
    JoinOptimizationInfo info;
    ReadBufferFromString in(optimization);
    assertString("JoinParameters:", in);
    assertString("isBHJ=", in);
    readBoolText(info.is_broadcast, in);
    assertChar('\n', in);
    if (info.is_broadcast)
    {
        assertString("isNullAwareAntiJoin=", in);
        readBoolText(info.is_null_aware_anti_join, in);
        assertChar('\n', in);
        assertString("buildHashTableId=", in);
        readString(info.storage_join_key, in);
        assertChar('\n', in);
    }
    return info;
}
}
