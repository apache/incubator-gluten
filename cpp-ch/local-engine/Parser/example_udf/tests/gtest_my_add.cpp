#include <iostream>
#include <Parser/FunctionExecutor.h>
#include <Parser/FunctionParser.h>
#include <gtest/gtest.h>

using namespace DB;
using namespace local_engine;

TEST(MyAdd, Common)
{
    auto context = local_engine::SerializedPlanParser::global_context;
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    FunctionExecutor executor("my_add", {type, type}, type, context);

    std::vector<FunctionExecutor::TestCase> cases = {
        {{Int64(1), Int64(2)}, Int64(3)},
        {{{}, Int64(2)}, {}},
        {{Int64(1), {}}, {}},
    };

    auto ok = executor.executeAndCompare(cases);
    ASSERT_TRUE(ok);
}
