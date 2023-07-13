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

class FunctionParserArrayUnion : public FunctionParser
{
public:
    explicit FunctionParserArrayUnion(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }

    static constexpr auto name = "array_union";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        /// parse array_union(a, b) as arrayDistinctSpark(arrayConcat(a, b))
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const auto * left_arg = parsed_args[0];
        const auto * right_arg = parsed_args[1];

        const auto * array_concat_node = toFunctionNode(actions_dag, "arrayConcat", {left_arg, right_arg});
        const auto * result_node = toFunctionNode(actions_dag, "arrayDistinctSpark", {array_concat_node});
        /// We cann not call convertNodeTypeIfNeeded which will fail when cast Array(Array(xx)) to Array(Nullable(Array(xx)))
        return result_node;
    }
};

static FunctionParserRegister<FunctionParserArrayUnion> register_array_union;
}
