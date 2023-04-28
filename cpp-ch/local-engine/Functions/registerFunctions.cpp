#include <Functions/FunctionFactory.h>

namespace local_engine
{

using namespace DB;
void registerFunctionSparkTrim(FunctionFactory &);
void registerFunctionsHashingExtended(FunctionFactory &);
void registerFunctionPositionUTF8Spark(FunctionFactory &);
void registerFunctionReinterpretAsStringSpark(FunctionFactory &);
void registerFunctionRegexpExtractAllSpark(FunctionFactory &);

void registerFunctions(FunctionFactory  & factory)
{
    registerFunctionSparkTrim(factory);
    registerFunctionsHashingExtended(factory);
    registerFunctionPositionUTF8Spark(factory);
    registerFunctionReinterpretAsStringSpark(factory);
    registerFunctionRegexpExtractAllSpark(factory);
}

}
