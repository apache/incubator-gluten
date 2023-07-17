#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>

#include <filesystem>

using namespace DB;

#ifdef __cplusplus
extern "C" {
#endif

char * createExecutor(const std::string & plan_string)
{
    auto context = Context::createCopy(local_engine::SerializedPlanParser::global_context);
    local_engine::SerializedPlanParser parser(context);
    auto query_plan = parser.parse(plan_string);
    local_engine::LocalExecutor * executor = new local_engine::LocalExecutor(parser.query_context, context);
    executor->execute(std::move(query_plan));
    return reinterpret_cast<char *>(executor);
}

bool executorHasNext(char * executor_address)
{
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    return executor->hasNext();
}

#ifdef __cplusplus
}
#endif
