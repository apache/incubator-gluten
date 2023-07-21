
#include <base/sleep.h>
#include <Common/GlutenSignalHandler.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <IO/WriteHelpers.h>


using namespace DB;
using namespace local_engine;


int main(int  /*argc*/, char * /*argv*/[])
{
    local_engine::Logger::initConsoleLogger("trace");
    Poco::Logger * logger = &Poco::Logger::get("signal_demo");
    SignalHandler::instance().init();

    for (int j = 0; j < 10 ; j++) {
        LOG_TRACE(logger, "counter {}", j);

        if( j ){
            int *x = nullptr;
            *x = 1;
        }
        sleepForSeconds(3);
    }

    LOG_TRACE(logger, "byb bye!");
    return 0;
}
