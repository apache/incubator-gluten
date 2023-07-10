#include "LeadLagParser.h"
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/CHUtil.h>

namespace local_engine
{
DB::ActionsDAG::NodeRawConstPtrs
LeadParser::parseFunctionArguments(const CommonFunctionInfo & func_info, const String & /*ch_func_name*/, DB::ActionsDAGPtr & actions_dag) const
{
    DB::ActionsDAG::NodeRawConstPtrs args;
    const auto & arg0 = func_info.arguments[0].value();
    const auto & arg1 = func_info.arguments[1].value();
    /// The 3rd arg is default value
    /// when it is set to null, the 1st arg must be nullable
    const auto & arg2 = func_info.arguments[2].value();
    const auto * arg0_col = actions_dag->getInputs()[arg0.selection().direct_reference().struct_field().field()];
    auto arg0_col_name = arg0_col->result_name;
    auto arg0_col_type= arg0_col->result_type;
    const DB::ActionsDAG::Node * node = nullptr;
    if (arg2.has_literal() && arg2.literal().has_null() && !arg0_col_type->isNullable())
    {
        node = ActionsDAGUtil::convertNodeType(
            actions_dag,
            &actions_dag->findInOutputs(arg0_col_name),
            DB::makeNullable(arg0_col_type)->getName(),
            arg0_col_name);
        actions_dag->addOrReplaceInOutputs(*node);
        args.push_back(node);
    }
    else
    {
        args.push_back(arg0_col);
    }

    node = parseExpression(actions_dag, arg1);
    node = ActionsDAGUtil::convertNodeType(actions_dag, node, DB::DataTypeInt64().getName());
    actions_dag->addOrReplaceInOutputs(*node);
    args.push_back(node);

    if (arg2.has_literal() && !arg2.literal().has_null())
    {
        node = parseExpression(actions_dag, arg2);
        actions_dag->addOrReplaceInOutputs(*node);
        args.push_back(node);
    }    
    return args;
}
FunctionParserRegister<LeadParser> lead_register;

DB::ActionsDAG::NodeRawConstPtrs
LagParser::parseFunctionArguments(const CommonFunctionInfo & func_info, const String & /*ch_func_name*/, DB::ActionsDAGPtr & actions_dag) const
{
    DB::ActionsDAG::NodeRawConstPtrs args;
    const auto & arg0 = func_info.arguments[0].value();
    const auto & arg1 = func_info.arguments[1].value();
    /// The 3rd arg is default value
    /// when it is set to null, the 1st arg must be nullable
    const auto & arg2 = func_info.arguments[2].value();
    const auto * arg0_col = actions_dag->getInputs()[arg0.selection().direct_reference().struct_field().field()];
    auto arg0_col_name = arg0_col->result_name;
    auto arg0_col_type = arg0_col->result_type;
    const DB::ActionsDAG::Node * node = nullptr;
    if (arg2.has_literal() && arg2.literal().has_null() && !arg0_col->result_type->isNullable())
    {
        node = ActionsDAGUtil::convertNodeType(
            actions_dag,
            &actions_dag->findInOutputs(arg0_col_name),
            DB::makeNullable(arg0_col_type)->getName(),
            arg0_col_name);
        actions_dag->addOrReplaceInOutputs(*node);
        args.push_back(node);
    }
    else
    {
        args.push_back(arg0_col);
    }

    // lag's offset is negative
    auto literal_result = parseLiteral(arg1.literal());
    assert(literal_result.second.safeGet<DB::Int32>() < 0);
    auto real_field = 0 - literal_result.second.safeGet<DB::Int32>();
    node = &actions_dag->addColumn(ColumnWithTypeAndName(
        literal_result.first->createColumnConst(1, real_field), literal_result.first, getUniqueName(toString(real_field))));
    node = ActionsDAGUtil::convertNodeType(actions_dag, node, DB::DataTypeInt64().getName());
    actions_dag->addOrReplaceInOutputs(*node);
    args.push_back(node);

    if (arg2.has_literal() && !arg2.literal().has_null())
    {
        node = parseExpression(actions_dag, arg2);
        actions_dag->addOrReplaceInOutputs(*node);
        args.push_back(node);
    }
    return args;
}
FunctionParserRegister<LagParser> lag_register;
}
