#include "SparkFunctionHashingExtended.h"

#include <Functions/FunctionFactory.h>

namespace local_engine
{

REGISTER_FUNCTION(HashingExtended)
{
    factory.registerFunction<FunctionXxHashSpark64>();
    factory.registerFunction<FunctionMurmurHashSpark3_32>();
}

}
