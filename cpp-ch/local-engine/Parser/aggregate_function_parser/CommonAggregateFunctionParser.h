#pragma once
#include <utility>
#include <DataTypes/IDataType.h>
#include <Parser/FunctionParser.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{
class BaseAggregateFunctionParser: public FunctionParser
{
public:
    explicit BaseAggregateFunctionParser(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    virtual ~BaseAggregateFunctionParser() override = default;

    virtual String getName() const override { return "BaseAggregateFunctionParser"; }

    virtual DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const CommonFunctionInfo & func_info,
        const String & ch_func_name,
        DB::ActionsDAGPtr & actions_dag) const override;

    virtual const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const CommonFunctionInfo & func_info,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAGPtr & actions_dag) const override;

    // `PartialMerge` is applied on the merging stages.
    // `If` is applied when the aggreate function has a filter. This should only happen on the 1st stage.
    // If no combinator is applied, return (ch_func_name,arg_column_types)
    virtual std::pair<String, DB::DataTypes>
    tryApplyCHCombinator(const CommonFunctionInfo & func_info, const String & ch_func_name, const DB::DataTypes & arg_column_types) const;

protected:
    Poco::Logger * logger = &Poco::Logger::get("BaseAggregateFunctionParser");
};

}   // namespace local_engine
