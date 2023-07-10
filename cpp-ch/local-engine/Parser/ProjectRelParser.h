#pragma once
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Parser/RelParser.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{
class ProjectRelParser : public RelParser
{
public:
    explicit ProjectRelParser(SerializedPlanParser * plan_paser_);
    ~ProjectRelParser() override = default;

    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;
private:
    Poco::Logger * logger = &Poco::Logger::get("ProjectRelParser");

    DB::QueryPlanPtr
    parseProject(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_);
    DB::QueryPlanPtr
    parseGenerate(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_);
};
}
