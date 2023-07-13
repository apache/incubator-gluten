#pragma once

#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Interpreters/Context_fwd.h>

namespace local_engine
{
class SubstraitFileSourceStep : public DB::SourceStepWithFilter
{
public:
    explicit SubstraitFileSourceStep(DB::ContextPtr context_, DB::Pipe pipe_, String name);

    void applyFilters() override;

    String getName() const override { return "SubstraitFileSourceStep"; }

    void initializePipeline(DB::QueryPipelineBuilder &, const DB::BuildQueryPipelineSettings &) override;

private:
    DB::Pipe pipe;
    DB::ContextPtr context;
};

}

