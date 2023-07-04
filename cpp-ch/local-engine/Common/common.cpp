#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>

#include <filesystem>

using namespace DB;

#ifdef __cplusplus
extern "C" {
#endif

bool executorHasNext(char * executor_address)
{
    local_engine::LocalExecutor * executor = reinterpret_cast<local_engine::LocalExecutor *>(executor_address);
    return executor->hasNext();
}

#ifdef __cplusplus
}
#endif
