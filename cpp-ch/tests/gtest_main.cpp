#include <Common/Logger.h>
#include <Interpreters/Context.h>
#include <Poco/Util/MapConfiguration.h>
#include <Parser/SerializedPlanParser.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <gtest/gtest.h>

using namespace local_engine;
using namespace DB;

int main(int argc, char ** argv)
{
    local_engine::Logger::initConsoleLogger();

    SharedContextHolder shared_context = Context::createShared();
    local_engine::SerializedPlanParser::global_context = Context::createGlobal(shared_context.get());
    local_engine::SerializedPlanParser::global_context->makeGlobalContext();
    auto config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
    local_engine::SerializedPlanParser::global_context->setConfig(config);
    local_engine::SerializedPlanParser::global_context->setPath("/tmp");
    local_engine::SerializedPlanParser::global_context->getDisksMap().emplace();
    local_engine::SerializedPlanParser::initFunctionEnv();
    auto & factory = local_engine::ReadBufferBuilderFactory::instance();
    registerReadBufferBuildes(factory);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
