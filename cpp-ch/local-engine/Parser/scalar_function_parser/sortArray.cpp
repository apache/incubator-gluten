#include <Parser/FunctionParser.h>
#include <Common/CHUtil.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionParserSortArray : public FunctionParser
{
public:
    explicit FunctionParserSortArray(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionParserSortArray() override = default;

    static constexpr auto name = "sort_array";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * array_arg = parsed_args[0];
        const auto * order_arg = parsed_args[1];

        const auto * sort_node = toFunctionNode(actions_dag, "arraySortSpark", {array_arg});
        const auto * reverse_sort_node = toFunctionNode(actions_dag, "arrayReverseSortSpark", {array_arg});

        const auto * result_node = toFunctionNode(actions_dag, "if", {order_arg, sort_node, reverse_sort_node});
        return result_node;
    }
};

static FunctionParserRegister<FunctionParserSortArray> register_sort_array;

}
