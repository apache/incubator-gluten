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
#include <Parser/SerializedPlanParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <gtest/gtest.h>
#include <Common/DebugUtils.h>
#include <Common/GlutenConfig.h>


using namespace local_engine;

using namespace DB;

INCBIN(resource_embedded_pr_18_2_json, SOURCE_DIR "/utils/extern-local-engine/tests/decmial_filter_push_down/18_2.json");
TEST(ColumnIndex, Deciaml182)
{
    // [precision,scale] = [18,2]
    const auto context1 = DB::Context::createCopy(SerializedPlanParser::global_context);

    auto config = ExecutorConfig::loadFromContext(context1);
    EXPECT_TRUE(config.use_local_format) << "gtest need set use_local_format to true";

    const std::string split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","partitionIndex":"0","length":"488","parquet":{},"schema":{},"metadataColumns":[{}]}]})";
    const std::string split = replaceLocalFilesWildcards(
        split_template, GLUTEN_DATA_DIR("/utils/extern-local-engine/tests/decmial_filter_push_down/18_2_flba.snappy.parquet"));

    SerializedPlanParser parser(context1);
    parser.addSplitInfo(local_engine::JsonStringToBinary<substrait::ReadRel::LocalFiles>(split));

    const auto plan = local_engine::JsonStringToMessage<substrait::Plan>(
        {reinterpret_cast<const char *>(gresource_embedded_pr_18_2_jsonData), gresource_embedded_pr_18_2_jsonSize});

    auto local_executor = parser.createExecutor(plan);
    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x);
}