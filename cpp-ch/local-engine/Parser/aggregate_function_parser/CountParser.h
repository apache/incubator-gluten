#pragma once
#include <Parser/AggregateFunctionParser.h>

/// count in spark supports multiple arguments, different from CH
namespace local_engine
{
class CountParser : public AggregateFunctionParser
{
public:
    explicit CountParser(SerializedPlanParser * plan_parser_) : AggregateFunctionParser(plan_parser_) { }
    ~CountParser() override = default;
    static constexpr auto name = "count";
    String getName() const override { return name; }
    String getCHFunctionName(const CommonFunctionInfo &) const override;
    String getCHFunctionName(const DB::DataTypes &) const override;
    DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const CommonFunctionInfo & func_info, const String & ch_func_name, DB::ActionsDAGPtr & actions_dag) const override;
};
}
