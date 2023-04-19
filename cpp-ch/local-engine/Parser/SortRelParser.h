#pragma once
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Parser/RelParser.h>
#include <google/protobuf/repeated_field.h>
namespace local_engine
{
class SortRelParser : public RelParser
{
public:
    explicit SortRelParser(SerializedPlanParser * plan_paser_);
    ~SortRelParser() override = default;

    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & sort_rel, std::list<const substrait::Rel *> & rel_stack_) override;
    static DB::SortDescription parseSortDescription(const google::protobuf::RepeatedPtrField<substrait::SortField> & sort_fields, const DB::Block & header);
private:
    size_t parseLimit(std::list<const substrait::Rel *> & rel_stack_);
};
}
