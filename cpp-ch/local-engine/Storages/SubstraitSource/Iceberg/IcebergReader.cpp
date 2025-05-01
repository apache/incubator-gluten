/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "IcebergReader.h"

#include <Columns/FilterDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/SubstraitSource/Delta/Bitmap/DeltaDVRoaringBitmapArray.h>
#include <Storages/SubstraitSource/Iceberg/EqualityDeleteFileReader.h>
#include <Storages/SubstraitSource/Iceberg/PositionalDeleteFileReader.h>
#include <Common/BlockTypeUtils.h>

using namespace DB;

namespace local_engine::iceberg
{

std::unique_ptr<IcebergReader> IcebergReader::create(
    const FormatFilePtr & file_,
    const Block & to_read_header_,
    const Block & output_header_,
    const std::function<FormatFile::InputFormatPtr(const DB::Block &)> & input_format_callback)
{
    assert(file_->getFileSchema().columns() != 0);

    const auto & delete_files = file_->getFileInfo().iceberg().delete_files();
    std::map<IcebergReadOptions::FileContent, std::vector<int>> partitions;
    for (size_t i = 0; i < delete_files.size(); ++i)
        partitions[delete_files[i].filecontent()].push_back(i);

    assert(!partitions.contains(IcebergReadOptions::DATA));
    const auto & context = file_->getContext();

    DB::Block new_header = to_read_header_.cloneEmpty();

    /// Load POSITION_DELETES
    const auto it_pos = partitions.find(IcebergReadOptions::POSITION_DELETES);
    std::unique_ptr<DeltaDVRoaringBitmapArray> delete_bitmap_array;
    if (it_pos != partitions.end())
        delete_bitmap_array
            = createBitmapExpr(context, file_->getFileSchema(), file_->getFileInfo(), delete_files, it_pos->second, new_header);

    /// Load EQUALITY_DELETES
    const auto it_equal = partitions.find(IcebergReadOptions::EQUALITY_DELETES);
    ExpressionActionsPtr delete_expr = it_equal == partitions.end()
        ? nullptr
        : EqualityDeleteFileReader::createDeleteExpr(context, file_->getFileSchema(), delete_files, it_equal->second, new_header);

    auto input = input_format_callback(new_header);
    if (!input)
        return nullptr;
    return std::make_unique<IcebergReader>(
        file_, new_header, output_header_, input, delete_expr, std::move(delete_bitmap_array), to_read_header_.columns());
}

IcebergReader::IcebergReader(
    const FormatFilePtr & file_,
    const Block & to_read_header_,
    const Block & output_header_,
    const FormatFile::InputFormatPtr & input_format_,
    const ExpressionActionsPtr & delete_expr_,
    std::unique_ptr<DeltaDVRoaringBitmapArray> delete_bitmap_array_,
    size_t start_remove_index_)
    : NormalFileReader(file_, to_read_header_, output_header_, input_format_)
    , delete_expr(delete_expr_)
    , delete_expr_column_name(EqualityDeleteActionBuilder::COLUMN_NAME)
    , delete_bitmap_array(std::move(delete_bitmap_array_))
    , start_remove_index(start_remove_index_)
{
    assert(readHeader.columns() >= start_remove_index);
}

IcebergReader::~IcebergReader() = default;


Chunk IcebergReader::doPull()
{
    if (!delete_expr && !delete_bitmap_array)
        return NormalFileReader::doPull();

    while (true)
    {
        Chunk chunk = NormalFileReader::doPull();
        if (chunk.getNumRows() == 0)
            return chunk;

        Block deleted_block;
        if (delete_expr)
            deleted_block = applyEqualityDelete(chunk);

        if (delete_bitmap_array)
        {
            if (!delete_expr)
            {
                auto delete_mask = ColumnUInt8::create(chunk.getNumRows(), 1);
                deleted_block = readHeader.cloneWithColumns(chunk.detachColumns());
                deleted_block.insert({delete_mask->getPtr(), UINT8(), delete_expr_column_name});
            }
            deleted_block = applyPositionDelete(std::move(deleted_block));
        }

        chunk = filterRows(std::move(deleted_block));

        if (chunk.getNumRows() == 0)
            continue;

        /// Remove attached columns for apply delete
        if (readHeader.columns() > start_remove_index)
        {
            auto rows = chunk.getNumRows();
            auto columns = chunk.detachColumns();
            columns.erase(columns.begin() + start_remove_index, columns.end());
            chunk.setColumns(std::move(columns), rows);
        }
        return chunk;
    }
}

Block IcebergReader::applyEqualityDelete(Chunk & chunk) const
{
    assert(delete_expr);
    assert(!delete_expr_column_name.empty());
    size_t num_rows_before_filtration = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    Block block = readHeader.cloneWithColumns(columns);
    delete_expr->execute(block, num_rows_before_filtration);
    return block;
}

DB::Block IcebergReader::applyPositionDelete(DB::Block block) const
{
    assert(delete_bitmap_array);
    size_t num_of_rows = block.rows();
    size_t filter_column_position = block.getPositionByName(delete_expr_column_name);
    size_t pos_column_position = block.getPositionByName(ParquetVirtualMeta::TMP_ROWINDEX);
    MutableColumns mutation = block.mutateColumns();
    auto & filter_column = assert_cast<ColumnUInt8 &>(*mutation[filter_column_position]);
    auto & pos_column = assert_cast<ColumnInt64 &>(*mutation[pos_column_position]);

    DB::ColumnInt64::Container & pos = pos_column.getData();
    DB::ColumnUInt8::Container & filter = filter_column.getData();

    for (int i = 0; i < num_of_rows; i++)
        if (delete_bitmap_array->rb_contains(pos[i]))
            filter[i] = 0;
    block.setColumns(std::move(mutation));
    return block;
}

namespace
{
void removeFilterIfNeed(Columns & columns, size_t filter_column_position)
{
    columns.erase(columns.begin() + filter_column_position);
}
}

DB::Chunk IcebergReader::filterRows(Block block) const
{
    size_t num_rows_before_filtration = block.rows();
    auto columns = block.getColumns();
    DataTypes types = block.getDataTypes();
    size_t filter_column_position = block.getPositionByName(delete_expr_column_name);
    block.clear();

    // filter out rows that are deleted
    size_t num_columns = columns.size();
    ColumnPtr filter_column = columns[filter_column_position];

    std::unique_ptr<IFilterDescription> filter_description = std::make_unique<FilterDescription>(*filter_column);

    /** Let's find out how many rows will be in result.
      * To do this, we filter out the first non-constant column
      *  or calculate number of set bytes in the filter.
      */
    size_t first_non_constant_column = num_columns;
    size_t min_size_in_memory = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < num_columns; ++i)
    {
        DataTypePtr type_not_null = removeNullableOrLowCardinalityNullable(types[i]);
        if (i != filter_column_position && !isColumnConst(*columns[i]) && type_not_null->isValueRepresentedByNumber())
        {
            size_t size_in_memory = type_not_null->getSizeOfValueInMemory() + (isNullableOrLowCardinalityNullable(types[i]) ? 1 : 0);
            if (size_in_memory < min_size_in_memory)
            {
                min_size_in_memory = size_in_memory;
                first_non_constant_column = i;
            }
        }
    }
    (void)min_size_in_memory; /// Suppress error of clang-analyzer-deadcode.DeadStores

    size_t num_filtered_rows = 0;
    if (first_non_constant_column != num_columns)
    {
        columns[first_non_constant_column] = filter_description->filter(*columns[first_non_constant_column], -1);
        num_filtered_rows = columns[first_non_constant_column]->size();
    }
    else
        num_filtered_rows = filter_description->countBytesInFilter();

    /// If the current block is completely filtered out, let's move on to the next one.
    if (num_filtered_rows == 0)
        return {};

    Chunk chunk;
    /// If all the rows pass through the filter.
    if (num_filtered_rows == num_rows_before_filtration)
    {
        /// No need to touch the rest of the columns.
        removeFilterIfNeed(columns, filter_column_position);
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        return chunk;
    }

    /// Filter the rest of the columns.
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto & current_column = columns[i];

        if (i == filter_column_position)
            continue;

        if (i == first_non_constant_column)
            continue;

        if (isColumnConst(*current_column))
            current_column = current_column->cut(0, num_filtered_rows);
        else
            current_column = filter_description->filter(*current_column, num_filtered_rows);
    }

    removeFilterIfNeed(columns, filter_column_position);
    chunk.setColumns(std::move(columns), num_filtered_rows);
    return chunk;
}

}