#pragma once
#include <Core/Block.h>
#include <Parser/ExpandField.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
namespace local_engine
{
class EmptyHashAggregateStep : public DB::ITransformingStep
{
public:
    explicit EmptyHashAggregateStep(const DB::DataStream & input_stream_);
    ~EmptyHashAggregateStep() override = default;

    String getName() const override { return "EmptyHashAggregateStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override;
    void updateOutputStream() override;
};
}
