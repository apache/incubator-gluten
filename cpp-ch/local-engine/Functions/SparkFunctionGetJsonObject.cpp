#include "SparkFunctionGetJsonObject.h"
#include <Functions/FunctionFactory.h>


namespace local_engine
{
REGISTER_FUNCTION(GetJsonObject)
{
    factory.registerFunction<DB::FunctionSQLJSON<GetJsonObject, GetJsonObjectImpl>>();
}
}
