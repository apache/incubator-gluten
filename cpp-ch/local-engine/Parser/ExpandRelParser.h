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
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;
};
}
