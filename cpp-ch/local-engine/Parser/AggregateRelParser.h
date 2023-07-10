#pragma once
#include <Parser/FunctionParser.h>
#include <Parser/RelParser.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>


namespace local_engine
{
class AggregateRelParser : public RelParser
{
public:
    explicit AggregateRelParser(SerializedPlanParser * plan_paser_);
    ~AggregateRelParser() override = default;
    DB::QueryPlanPtr parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;
private:
    struct AggregateInfo
    {
        const substrait::AggregateRel::Measure * measure = nullptr;
        Strings arg_column_names;
        DB::DataTypes arg_column_types;
        Array params;
        String signature_function_name;
        String function_name;
        // If no combinator be applied on it, same as function_name
        String combinator_function_name;
        // For avoiding repeated builds.
        FunctionParser::CommonFunctionInfo parser_func_info;
        // For avoiding repeated builds.
        FunctionParserPtr function_parser;
    };

    Poco::Logger * logger = &Poco::Logger::get("AggregateRelParser");
    bool has_first_stage = false;
    bool has_inter_stage = false;
    bool has_final_stage = false;

    DB::QueryPlanPtr plan = nullptr;
    const substrait::AggregateRel * aggregate_rel = nullptr;
    std::vector<AggregateInfo> aggregates;
    Names grouping_keys;

    void setup(DB::QueryPlanPtr query_plan, const substrait::Rel & rel);
    void addPreProjection();
    void addMergingAggregatedStep();
    void addAggregatingStep();
    void addPostProjection();

    void buildAggregateDescriptions(AggregateDescriptions & descriptions);
};
}
