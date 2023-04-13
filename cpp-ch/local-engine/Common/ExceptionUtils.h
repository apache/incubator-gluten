#pragma once
#include <Common/logger_useful.h>
#include <Common/Exception.h>

namespace local_engine
{
class ExceptionUtils
{
public:
    static void handleException(const DB::Exception & exception);
};
}
