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

#include <Parser/SerializedPlanParser.h>
#include <gtest/gtest.h>
#include <Common/DebugUtils.h>


using namespace local_engine;

using namespace DB;

// Plan for https://github.com/ClickHouse/ClickHouse/pull/54881
INCBIN(resource_embedded_pr_54881_json, SOURCE_DIR "/utils/extern-local-engine/tests/json/clickhouse_pr_54881.json");

TEST(Clickhouse, PR54881)
{
    const auto context1 = DB::Context::createCopy(SerializedPlanParser::global_context);
    // context1->setSetting("enable_named_columns_in_function_tuple", DB::Field(true));
    auto settingxs = context1->getSettingsRef();
    EXPECT_FALSE(settingxs.enable_named_columns_in_function_tuple) <<
        "GLUTEN NEED set enable_named_columns_in_function_tuple to false";

    const std::string split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","partitionIndex":"0","length":"1529","parquet":{},"schema":{},"metadataColumns":[{}]}]})";
    const std::string split
        = replaceLocalFilesWildcards(split_template, GLUTEN_DATA_DIR("/utils/extern-local-engine/tests/data/54881.snappy.parquet"));

    SerializedPlanParser parser(context1);
    parser.addSplitInfo(test::pb_util::JsonStringToBinary<substrait::ReadRel::LocalFiles>(split));

    const auto local_executor = parser.createExecutor<true>(
       {reinterpret_cast<const char *>(gresource_embedded_pr_54881_jsonData), gresource_embedded_pr_54881_jsonSize});

    EXPECT_TRUE(local_executor->hasNext());
    const Block & block = *local_executor->nextColumnar();

    debug::headBlock(block);

    EXPECT_EQ(2, block.columns());
    const auto & col_0 = *(block.getColumns()[0]);
    EXPECT_EQ(col_0.getInt(0), 9);
    EXPECT_EQ(col_0.getInt(1),10);

    Field field;
    const auto & col_1 = *(block.getColumns()[1]);
    col_1.get(0, field);
    const Tuple& row_0 = field.get<DB::Tuple>();
    EXPECT_EQ(2, row_0.size());

    Int64 actual{-1};
    EXPECT_TRUE(row_0[0].tryGet<Int64>(actual));
    EXPECT_EQ(9, actual);

    EXPECT_TRUE(row_0[1].tryGet<Int64>(actual));
    EXPECT_EQ(10, actual);

    col_1.get(1, field);
    const Tuple& row_1 = field.get<DB::Tuple>();
    EXPECT_EQ(2, row_1.size());
    EXPECT_TRUE(row_1[0].tryGet<Int64>(actual));
    EXPECT_EQ(10, actual);

    EXPECT_TRUE(row_1[1].tryGet<Int64>(actual));
    EXPECT_EQ(11, actual);

    EXPECT_FALSE(local_executor->hasNext());
}
