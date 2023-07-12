#include "SparkFunctionHashingExtended.h"

#include <Functions/FunctionFactory.h>

namespace local_engine
{

REGISTER_FUNCTION(HashingExtended)
{
    factory.registerFunction<SparkFunctionXxHash64>();
    factory.registerFunction<SparkFunctionMurmurHash3_32>();
}

}
