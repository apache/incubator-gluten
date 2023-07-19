#include <Parser/FunctionParser.h>
#include <Common/CHUtil.h>
#include "Columns/IColumn.h"
#include "Interpreters/ActionsDAG.h"
#include "base/types.h"
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

class FunctionParserMonthsBetween : public FunctionParser
{
public:
    explicit FunctionParserMonthsBetween(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }

    static constexpr auto name = "months_between";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        // Parse months_between(date1, date2, roundOff=True) as
        // if roundOff = True:
        //     round(devide(date_diff('d', toDate(date2), toDate(date1)), 30.636038795303726), 8)
        // else:
        //     devide(date_diff('d', toDate(date2), toDate(date1)), 30.636038795303726)
        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() <= 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires at least two arguments", getName());

        const auto * round_arg = parsed_args.size() >= 3 ? parsed_args[2] : nullptr;
        const auto * timezone_arg = parsed_args.size() >= 4 ? parsed_args[3] : nullptr;
        const auto * unit_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), "second");
        const auto * magic_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeFloat64>(), 2618099.998848);
        const auto * date_diff_node = toFunctionNode(actions_dag, "date_diff", {unit_const_node, parsed_args[1], parsed_args[0], timezone_arg});
        const auto * devide_node = toFunctionNode(actions_dag, "divide", {date_diff_node, magic_const_node});
        const auto * round_node = toFunctionNode(actions_dag, "round", {devide_node, addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeInt32>(), 8)});
        const auto * ifnode = toFunctionNode(actions_dag, "if", {round_arg, round_node, devide_node});

        return convertNodeTypeIfNeeded(substrait_func, ifnode, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserMonthsBetween> register_function_parser_months_between;

} // namespace local_engine
