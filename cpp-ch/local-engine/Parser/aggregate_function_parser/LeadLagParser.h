#pragma once
#include "CommonAggregateFunctionParser.h"

namespace local_engine
{
class LeadParser : public BaseAggregateFunctionParser
{
public:
    explicit LeadParser(SerializedPlanParser * plan_parser_) : BaseAggregateFunctionParser(plan_parser_) { }
    ~LeadParser() override = default;
    static constexpr auto name = "lead";
    String getName() const override { return name; }
    String getCHFunctionName(const CommonFunctionInfo &) const override { return "leadInFrame"; }
    String getCHFunctionName(const DB::DataTypes &) const override { return "leadInFrame"; }
    DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const CommonFunctionInfo & func_info, const String & ch_func_name, DB::ActionsDAGPtr & actions_dag) const override;
};

class LagParser : public BaseAggregateFunctionParser
{
public:
    explicit LagParser(SerializedPlanParser * plan_parser_) : BaseAggregateFunctionParser(plan_parser_) { }
    ~LagParser() override = default;
    static constexpr auto name = "lag";
    String getName() const override { return name; }
    String getCHFunctionName(const CommonFunctionInfo &) const override { return "lagInFrame"; }
    String getCHFunctionName(const DB::DataTypes &) const override { return "lagInFrame"; }
    DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const CommonFunctionInfo & func_info, const String & ch_func_name, DB::ActionsDAGPtr & actions_dag) const override;
};
}
