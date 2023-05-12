#include "Functions/FunctionsRoundHalfUp.h"
#include <Functions/FunctionFactory.h>


namespace local_engine
{
void registerFunctionsRoundHalfUp(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRoundHalfUp>();
}

}
