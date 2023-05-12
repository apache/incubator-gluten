#pragma once

#include <Core/Block.h>
#include <Parser/ExpandField.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace local_engine
{
class ExpandStep : public DB::ITransformingStep
{
public:
    // The input stream should only contain grouping columns.
    explicit ExpandStep(const DB::DataStream & input_stream_, const ExpandField & project_set_exprs_);
    ~ExpandStep() override = default;

    String getName() const override { return "ExpandStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override;

private:
    ExpandField project_set_exprs;
    DB::Block header;
    DB::Block output_header;

    void updateOutputStream() override;

    static DB::Block buildOutputHeader(const DB::Block & header, const ExpandField & project_set_exprs_);
};
}
