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
            .preserves_distinct_columns = false, /// Actually, we may check that distinct names are in aggregation keys
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
    const std::vector<size_t> & aggregating_expressions_columns_,
    const std::vector<std::set<size_t>> & grouping_sets_,
    const std::string & grouping_id_name_)
    : DB::ITransformingStep(
        input_stream_,
        buildOutputHeader(input_stream_.header, aggregating_expressions_columns_, grouping_id_name_),
        getTraits())
    , aggregating_expressions_columns(aggregating_expressions_columns_)
    , grouping_sets(grouping_sets_)
    , grouping_id_name(grouping_id_name_)
{
    header = input_stream_.header;
    output_header = getOutputStream().header;
}

DB::Block ExpandStep::buildOutputHeader(
    const DB::Block & input_header,
    const std::vector<size_t> & aggregating_expressions_columns_,
    const std::string & grouping_id_name_)
{
    DB::ColumnsWithTypeAndName cols;
    std::set<size_t> agg_cols;

    for (size_t i = 0; i < input_header.columns(); ++i)
    {
        const auto & old_col = input_header.getByPosition(i);
        if (i < aggregating_expressions_columns_.size())
        {
            // do nothing with the aggregating columns.
            cols.push_back(old_col);
            continue;
        }
        if (old_col.type->isNullable())
            cols.push_back(old_col);
        else
        {
            auto null_map = DB::ColumnUInt8::create(0, 0);
            auto null_col = DB::ColumnNullable::create(old_col.column, std::move(null_map));
            auto null_type = std::make_shared<DB::DataTypeNullable>(old_col.type);
            cols.push_back(DB::ColumnWithTypeAndName(null_col, null_type, old_col.name));
        }
    }

    // add group id column
    auto grouping_id_col = DB::ColumnInt64::create(0, 0);
    auto grouping_id_type = std::make_shared<DB::DataTypeInt64>();
    cols.emplace_back(DB::ColumnWithTypeAndName(std::move(grouping_id_col), grouping_id_type, grouping_id_name_));
    return DB::Block(cols);
}

void ExpandStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    DB::QueryPipelineProcessorsCollector collector(pipeline, this);
    auto build_transform = [&](DB::OutputPortRawPtrs outputs){
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto expand_op = std::make_shared<ExpandTransform>(header, output_header, aggregating_expressions_columns, grouping_sets);
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
