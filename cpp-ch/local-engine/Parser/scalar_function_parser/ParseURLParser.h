#pragma once
#include <Parser/FunctionParser.h>
namespace local_engine
{
class ParseURLParser final : public FunctionParser
{
public:
    static constexpr auto name = "parse_url";
    ParseURLParser(SerializedPlanParser * plan_parser) : FunctionParser(plan_parser) { }
    ~ParseURLParser() override = default;
    String getName() const override { return name; }
protected:
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override;

    DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const substrait::Expression_ScalarFunction & substrait_func,
        const String & ch_func_name,
        DB::ActionsDAGPtr & actions_dag) const override;

    const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const substrait::Expression_ScalarFunction & substrait_func,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAGPtr & actions_dag) const override;

private:
    String getQueryPartName(const substrait::Expression & expr) const;
    String selectCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const;
};
}
