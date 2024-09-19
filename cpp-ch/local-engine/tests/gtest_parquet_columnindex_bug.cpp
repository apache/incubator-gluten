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
#include <gluten_test_util.h>
#include <incbin.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parser/LocalExecutor.h>
#include <Parser/SubstraitParserUtils.h>
#include <gtest/gtest.h>
#include <substrait/plan.pb.h>
#include <Common/DebugUtils.h>
#include <Common/GlutenConfig.h>
#include <Common/QueryContext.h>

using namespace local_engine;

using namespace DB;

INCBIN(_pr_18_2, SOURCE_DIR "/utils/extern-local-engine/tests/decimal_filter_push_down/18_2.json");
TEST(ColumnIndex, Decimal182)
{
    // [precision,scale] = [18,2]
    const auto context1 = DB::Context::createCopy(QueryContext::globalMutableContext());
    const auto config = ExecutorConfig::loadFromContext(context1);
    EXPECT_TRUE(config.use_local_format) << "gtest need set use_local_format to true";

    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","partitionIndex":"0","length":"488","parquet":{},"schema":{},"metadataColumns":[{}]}]})";
    constexpr std::string_view file{GLUTEN_DATA_DIR("/utils/extern-local-engine/tests/decimal_filter_push_down/18_2_flba.snappy.parquet")};
    auto [_, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(_pr_18_2), split_template, file, context1);

    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x);
}