#include <Parser/FunctionParser.h>

namespace local_engine
{

class FunctionParserMyAdd: public FunctionParser
{
public:
    explicit FunctionParserMyAdd(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }

    static constexpr auto name = "my_add";

    String getName() const override { return name; }

private:
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return "plus"; }
};

static FunctionParserRegister<FunctionParserMyAdd> register_my_add;
}
