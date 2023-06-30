#include <Parser/FunctionParser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

class FunctionParserMyMd5 : public FunctionParser
{
public:
    explicit FunctionParserMyMd5(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }

    static constexpr auto name = "my_md5";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        // In Spark: md5(str)
        // In CH: lower(hex(MD5(str)))
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires exactly one arguments", getName());

        const auto * str_arg = parsed_args[0];
        const auto * md5_node = toFunctionNode(actions_dag, "MD5", {str_arg});
        const auto * hex_node = toFunctionNode(actions_dag, "hex", {md5_node});
        const auto * lower_node = toFunctionNode(actions_dag, "lower", {hex_node});
        return convertNodeTypeIfNeeded(substrait_func, lower_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserMyMd5> register_my_md5;
}
