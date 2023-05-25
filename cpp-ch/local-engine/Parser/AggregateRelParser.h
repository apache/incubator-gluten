#pragma once
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
        String arg_column_name;
        DB::DataTypePtr arg_column_type = nullptr;
        String function_name;
        bool has_mismatch_nullablity = false;
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
    void addPostProjectionForAggregatingResult();
    void addPostProjectionForTypeMismatch();

    void buildAggregateDescriptions(AggregateDescriptions & descriptions);
};
}
