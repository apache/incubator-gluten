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

#include "utils/ConfigExtractor.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"

namespace gluten {

TEST(S3ConfigTest, defaultConfig) {
    auto s3Config = facebook::velox::filesystems::S3Config("", getHiveConfig({}));
    ASSERT_EQ(s3Config.useVirtualAddressing(), true);
    ASSERT_EQ(s3Config.useSSL(), false);
    ASSERT_EQ(s3Config.useInstanceCredentials(), false);
    ASSERT_EQ(s3Config.endpoint(), "");
    ASSERT_EQ(s3Config.accessKey(), std::nullopt);
    ASSERT_EQ(s3Config.secretKey(), std::nullopt);
    ASSERT_EQ(s3Config.iamRole(), std::nullopt);
    ASSERT_EQ(s3Config.iamRoleSessionName(), "spark-session");
}

} // namespace gluten
