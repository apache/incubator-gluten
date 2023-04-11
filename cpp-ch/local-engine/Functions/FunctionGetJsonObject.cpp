#include "FunctionGetJsonObject.h"
#include <Functions/FunctionFactory.h>


using DB::Token;
using DB::TokenType;

namespace local_engine
{

REGISTER_FUNCTION(GetJsonObject)
{
    factory.registerFunction<DB::FunctionSQLJSON<GetJsonOject, GetJsonObjectImpl>>();
}
}
