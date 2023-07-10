#pragma once
#include <Core/Block.h>
#include <Parser/ExpandField.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace local_engine
{
/// Some special cases will introduce an empty project step which output columns list is empty.
/// e.g. count(1). These cases we just return virtual blocks which can contain the information
/// of row number.
class EmptyProjectStep : public DB::ITransformingStep
{
public:
    explicit EmptyProjectStep(const DB::DataStream & input_stream_);
    ~EmptyProjectStep() override = default;

    String getName() const override { return "EmptyProjectStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override;
    void updateOutputStream() override;
};
}
