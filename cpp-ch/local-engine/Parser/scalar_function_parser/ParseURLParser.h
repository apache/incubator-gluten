#pragma once
#include <Parser/FunctionParser.h>
namespace local_engine
{
class ParseURLParser final : public FunctionParser
{
public:
    using CommonFunctionInfo = FunctionParser::CommonFunctionInfo;
    static constexpr auto name = "parse_url";
    ParseURLParser(SerializedPlanParser * plan_parser) : FunctionParser(plan_parser) { }
    ~ParseURLParser() override = default;
    String getName() const override { return name; }
    
    String getCHFunctionName(const CommonFunctionInfo & func_info) const override;
    String getCHFunctionName(const DB::DataTypes & args) const override;

    const DB::ActionsDAG::Node *
    parse(const substrait::Expression_ScalarFunction & substrait_func, DB::ActionsDAGPtr & actions_dag) const override
    {
        return parse(CommonFunctionInfo(substrait_func), actions_dag);
    }
    const DB::ActionsDAG::Node * parse(const CommonFunctionInfo & func_info, DB::ActionsDAGPtr & actions_dag) const override;

    DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const CommonFunctionInfo & func_info, const String & ch_func_name, DB::ActionsDAGPtr & actions_dag) const override;

    const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const CommonFunctionInfo & func_info, const DB::ActionsDAG::Node * func_node, DB::ActionsDAGPtr & actions_dag) const override;

private:
    String getQueryPartName(const substrait::Expression & expr) const;
    String selectCHFunctionName(const CommonFunctionInfo & func_info) const;
};
}
