#include "FunctionsHashingExtended.h"

#include <Functions/FunctionFactory.h>

namespace local_engine
{
void registerFunctionsHashingExtended(FunctionFactory & factory)
{
    factory.registerFunction<FunctionXxHashSpark64>();
    factory.registerFunction<FunctionMurmurHashSpark3_32>();
}

}
