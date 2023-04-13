#include "ExpandTransorm.h"
#include <memory>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/IProcessor.h>

#include <Common/logger_useful.h>
#include <Poco/Logger.h>

namespace local_engine
{
ExpandTransform::ExpandTransform(
    const DB::Block & input_,
    const DB::Block & output_,
    const std::vector<size_t> & aggregating_expressions_columns_,
    const std::vector<std::set<size_t>> & grouping_sets_)
    : DB::IProcessor({input_}, {output_})
    , aggregating_expressions_columns(aggregating_expressions_columns_)
    , grouping_sets(grouping_sets_)
{}

ExpandTransform::Status ExpandTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (has_output)
    {
        output.push(nextChunk());
        return Status::PortFull;
    }

    if (has_input)
    {
        return Status::Ready;
    }

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }
    input_chunk = input.pull();
    has_input = true;
    return Status::Ready;
}

void ExpandTransform::work()
{
    assert(expanded_chunks.empty());
    size_t agg_cols_size = aggregating_expressions_columns.size();
    for (int set_id = 0; static_cast<size_t>(set_id) < grouping_sets.size(); ++set_id)
    {
        const auto & sets = grouping_sets[set_id];
        DB::Columns cols;
        const auto & original_cols = input_chunk.getColumns();
        for (size_t i = 0; i < original_cols.size(); ++i)
        {
            const auto & original_col = original_cols[i];
            size_t rows = original_col->size();
            if (i < agg_cols_size)
            {
                cols.push_back(original_col);
                continue;
            }
            // the output columns should all be nullable.
            if (!sets.contains(i))
            {
                auto null_map = DB::ColumnUInt8::create(rows, 1);
                auto col = DB::ColumnNullable::create(original_col, std::move(null_map));
                cols.push_back(std::move(col));
            }
            else
            {
                if (original_col->isNullable())
                    cols.push_back(original_col);
                else
                {
                    auto null_map = DB::ColumnUInt8::create(rows, 0);
                    auto col = DB::ColumnNullable::create(original_col, std::move(null_map));
                    cols.push_back(std::move(col));
                }
            }
        }
        auto id_col = DB::DataTypeInt64().createColumnConst(input_chunk.getNumRows(), set_id);
        cols.push_back(std::move(id_col));
        expanded_chunks.push_back(DB::Chunk(cols, input_chunk.getNumRows()));
    }
    has_output = true;
    has_input = false;
}

DB::Chunk ExpandTransform::nextChunk()
{
    assert(!expanded_chunks.empty());
    DB::Chunk ret;
    ret.swap(expanded_chunks.front());
    expanded_chunks.pop_front();
    has_output = !expanded_chunks.empty();
    return ret;
}
}
