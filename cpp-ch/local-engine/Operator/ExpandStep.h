#pragma once

#include <Core/Block.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace local_engine
{
class ExpandStep : public DB::ITransformingStep
{
public:
    // The input stream should only contain grouping columns.
    explicit ExpandStep(
        const DB::DataStream & input_stream_,
        const std::vector<size_t> & aggregating_expressions_columns_,
        const std::vector<std::set<size_t>> & grouping_sets_,
        const std::string & grouping_id_name_);
    ~ExpandStep() override = default;

    String getName() const override { return "ExpandStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override;
private:
    std::vector<size_t> aggregating_expressions_columns;
    std::vector<std::set<size_t>> grouping_sets;
    std::string grouping_id_name;
    DB::Block header;
    DB::Block output_header;

    void updateOutputStream() override;

    static DB::Block buildOutputHeader(
        const DB::Block & header,
        const std::vector<size_t> & aggregating_expressions_columns_,
        const std::string & grouping_id_name_);
};
}
