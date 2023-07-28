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
#include <Functions/SparkFunctionHashingExtended.h>
#include <base/types.h>
#include <gtest/gtest.h>
#include <MurmurHash3.h>

using namespace local_engine;
using namespace DB;

TEST(Hash, SparkMurmurHash3_32)
{
    char buf[2] = {0, static_cast<char>(0xc8)};
    {
        UInt32 result = SparkMurmurHash3_32::apply(buf, sizeof(buf), 42);
        EXPECT_EQ(static_cast<Int32>(result), -424716282);
    }

    {
        UInt32 result = 0;
        MurmurHash3_x86_32(buf, 2, 42, &result);
        EXPECT_EQ(static_cast<Int32>(result), -1346355085);
    }
}
