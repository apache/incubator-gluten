#include <iostream>
#include <Parser/FunctionExecutor.h>
#include <Parser/FunctionParser.h>
#include <gtest/gtest.h>

using namespace DB;
using namespace local_engine;

TEST(MyMd5, Common)
{
    auto context = local_engine::SerializedPlanParser::global_context;
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    FunctionExecutor executor("my_md5", {type}, type, context);

    std::vector<FunctionExecutor::TestCase> cases = {
        {{"spark"}, "98f11b7a7880169c3bd62a5a507b3965"},
    };

    auto ok = executor.executeAndCompare(cases);
    ASSERT_TRUE(ok);
}
