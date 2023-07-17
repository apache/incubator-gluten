#include "Functions/SparkFunctionRoundHalfUp.h"
#include <Functions/FunctionFactory.h>


namespace local_engine
{
REGISTER_FUNCTION(RoundSpark)
{
    factory.registerFunction<FunctionRoundHalfUp>(
        FunctionDocumentation{
            .description=R"(
Similar to function round,except that in case when given number has equal distance to surrounding numbers, the function rounds away from zero(towards +inf/-inf).
        )",
            .examples{{"roundHalfUp", "SELECT roundHalfUp(3.165,2)", "3.17"}},
            .categories{"Rounding"}
        }, FunctionFactory::CaseInsensitive);

}
}
