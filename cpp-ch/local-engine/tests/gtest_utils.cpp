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
#include <gtest/gtest.h>
#include <Common/StringUtils.h>

using namespace local_engine;

TEST(TestStringUtils, TestExtractPartitionValues)
{
    std::string path = "/tmp/col1=1/col2=test/a.parquet";
    auto values = StringUtils::parsePartitionTablePath(path);
    ASSERT_EQ(2, values.size());
    ASSERT_EQ("col1", values[0].first);
    ASSERT_EQ("1", values[0].second);
    ASSERT_EQ("col2", values[1].first);
    ASSERT_EQ("test", values[1].second);
}
