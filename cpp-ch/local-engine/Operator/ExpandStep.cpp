#include "ExpandStep.h"
#include "ExpandTransorm.h"
#include <memory>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/IProcessor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>

namespace local_engine
{
static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

ExpandStep::ExpandStep(
    const DB::DataStream & input_stream_,
    const ExpandField & project_set_exprs_)
    : DB::ITransformingStep(
        input_stream_,
        buildOutputHeader(input_stream_.header, project_set_exprs_),
        getTraits())
    , project_set_exprs(project_set_exprs_)
{
    header = input_stream_.header;
    output_header = getOutputStream().header;
}

DB::Block ExpandStep::buildOutputHeader(
    const DB::Block & input_header,
    const ExpandField & project_set_exprs_)
{
    DB::ColumnsWithTypeAndName cols;
    const auto & types = project_set_exprs_.getTypes();
    const auto & names = project_set_exprs_.getNames();

    for (size_t i = 0; i < project_set_exprs_.getExpandCols(); ++i)
    {
        String col_name;
        if (!names[i].empty())
            col_name = names[i];
        else
            col_name = "expand_" + std::to_string(i);
        cols.push_back(DB::ColumnWithTypeAndName(types[i], col_name));
    }
    return DB::Block(cols);
}

void ExpandStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    DB::QueryPipelineProcessorsCollector collector(pipeline, this);
    auto build_transform = [&](DB::OutputPortRawPtrs outputs){
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto expand_op = std::make_shared<ExpandTransform>(header, output_header, project_set_exprs);
            new_processors.push_back(expand_op);
            DB::connect(*output, expand_op->getInputs().front());
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
    processors = collector.detachProcessors();
}

void ExpandStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}
void ExpandStep::updateOutputStream()
{
    createOutputStream(input_streams.front(), output_header, getDataStreamTraits());
}

}
