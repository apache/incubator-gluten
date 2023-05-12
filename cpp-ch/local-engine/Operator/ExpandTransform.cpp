#include <memory>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/IProcessor.h>
#include "ExpandTransorm.h"

#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace local_engine
{
ExpandTransform::ExpandTransform(const DB::Block & input_, const DB::Block & output_, const ExpandField & project_set_exprs_)
    : DB::IProcessor({input_}, {output_}), project_set_exprs(project_set_exprs_)
{
}

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
    const auto & original_cols = input_chunk.getColumns();
    size_t rows = input_chunk.getNumRows();

    for (size_t i = 0; i < project_set_exprs.getExpandRows(); ++i)
    {
        DB::Columns cols;
        for (size_t j = 0; j < project_set_exprs.getExpandCols(); ++j)
        {
            const auto & type = project_set_exprs.getTypes()[j];
            const auto & kind = project_set_exprs.getKinds()[i][j];
            const auto & field = project_set_exprs.getFields()[i][j];

            if (kind == EXPAND_FIELD_KIND_SELECTION)
            {
                const auto & original_col = original_cols[field.get<Int32>()];
                if (type->isNullable() == original_col->isNullable())
                {
                    cols.push_back(original_col);
                }
                else if (type->isNullable() && !original_col->isNullable())
                {
                    auto null_map = DB::ColumnUInt8::create(rows, 0);
                    auto col = DB::ColumnNullable::create(original_col, std::move(null_map));
                    cols.push_back(std::move(col));
                }
                else
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Miss match nullable, column {} is nullable, but type {} is not nullable",
                        original_col->getName(),
                        type->getName());
                }
            }
            else
            {
                if (field.isNull())
                {
                    // Add null column
                    auto null_map = DB::ColumnUInt8::create(rows, 1);
                    auto nested_type = DB::removeNullable(type);
                    auto col = DB::ColumnNullable::create(nested_type->createColumn()->cloneResized(rows), std::move(null_map));
                    cols.push_back(std::move(col));
                }
                else
                {
                    // Add constant column: gid, gpos, etc.
                    auto col = type->createColumnConst(rows, field);
                    cols.push_back(std::move(col->convertToFullColumnIfConst()));
                }
            }
        }
        expanded_chunks.push_back(DB::Chunk(cols, rows));
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
