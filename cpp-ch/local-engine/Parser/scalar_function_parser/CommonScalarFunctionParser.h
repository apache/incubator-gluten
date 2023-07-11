#pragma once
#include <Parser/FunctionParser.h>
#include <Common/Exception.h>

namespace DataTypeDecimalBase
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{
class CommonScalarFunctionParser : public FunctionParser
{
public:
    CommonScalarFunctionParser(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    virtual ~CommonScalarFunctionParser() override = default;
    virtual String getName() const override { throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented"); }
    virtual const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAGPtr & actions_dag) const override
    {
        FunctionParser::CommonFunctionInfo func_info(substrait_func);
        return parse(func_info, actions_dag);
    }
    virtual const DB::ActionsDAG::Node * parse(const CommonFunctionInfo & func_info, DB::ActionsDAGPtr & actions_dag) const override
    {
        return FunctionParser::parse(func_info, actions_dag);
    }
};
}
