#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Parser/FunctionParser.h>
#include <Common/CHUtil.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

class FunctionParserEmpty2Null : public FunctionParser
{
public:
    explicit FunctionParserEmpty2Null(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }

    static constexpr auto name = "empty2null";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAGPtr & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires exactly one arguments", getName());
        if (parsed_args.at(0)->result_type->getName() != "Nullable(String)")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {}'s argument has to be Nullable(String)", getName());

        const auto * null_node = addColumnToActionsDAG(actions_dag, makeNullableSafe(std::make_shared<DataTypeString>()), Field());

        auto cond = toFunctionNode(actions_dag, "empty", {parsed_args.at(0)});
        auto * if_function = toFunctionNode(actions_dag, "if", {cond, null_node, parsed_args.at(0)});

        return convertNodeTypeIfNeeded(substrait_func, if_function, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserEmpty2Null> register_emtpy2null;
}
