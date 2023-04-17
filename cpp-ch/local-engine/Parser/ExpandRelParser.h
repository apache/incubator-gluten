#pragma once
#include <Parser/RelParser.h>
#include <Parser/SerializedPlanParser.h>

namespace local_engine
{
class ExpandRelParser : public RelParser
{
public:
    explicit ExpandRelParser(SerializedPlanParser * plan_parser_);
    ~ExpandRelParser() override = default;
    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & sort_rel, std::list<const substrait::Rel *> & rel_stack_) override;
private:
    static void buildGroupingSets(const substrait::ExpandRel & expand_rel, std::vector<std::set<size_t>> & grouping_sets);
};
}
