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

class FunctionParserArrayDistinct : public FunctionParser
{
public:
    explicit FunctionParserArrayDistinct(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }

    static constexpr auto name = "array_distinct";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one arguments", getName());

        const auto * array_distinct_node = toFunctionNode(actions_dag, "arrayDistinctSpark", {parsed_args[0]});
        /// We cann not call convertNodeTypeIfNeeded which will fail when cast Array(Array(xx)) to Array(Nullable(Array(xx)))
        return array_distinct_node;
    }
};

static FunctionParserRegister<FunctionParserArrayDistinct> register_array_distinct;
}
