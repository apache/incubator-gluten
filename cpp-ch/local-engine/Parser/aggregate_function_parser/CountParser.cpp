#include <Parser/aggregate_function_parser/CountParser.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/CHUtil.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

String CountParser::getCHFunctionName(const CommonFunctionInfo &) const
{
    return "count";
}
String CountParser::getCHFunctionName(const DB::DataTypes &) const
{
    return "count";
}
DB::ActionsDAG::NodeRawConstPtrs CountParser::parseFunctionArguments(
    const CommonFunctionInfo & func_info, const String & /*ch_func_name*/, DB::ActionsDAGPtr & actions_dag) const
{
    if (func_info.arguments.size() < 1)
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Function {} requires at least one argument", getName());
    }

    const DB::ActionsDAG::Node * last_arg_node = nullptr;
    for (const auto & arg : func_info.arguments)
    {
        auto arg_value = arg.value();
        const DB::ActionsDAG::Node * current_arg_node = parseExpression(actions_dag, arg_value);
        if (!last_arg_node)
        {
            last_arg_node = current_arg_node;
            continue;
        }
        else
        {
            if (current_arg_node->result_type->isNullable())
            {
                const auto * not_null_node = toFunctionNode(actions_dag, "isNotNull", {current_arg_node});
                last_arg_node = toFunctionNode(actions_dag, "if", {not_null_node, last_arg_node, current_arg_node});     
            }
        }
    }
    if (func_info.has_filter)
    {
        // With `If` combinator, the function take one more argument which refers to the condition.
        return {last_arg_node, parseExpression(actions_dag, func_info.filter)};
    }
    return {last_arg_node};
}
static const FunctionParserRegister<CountParser> register_count;
}
