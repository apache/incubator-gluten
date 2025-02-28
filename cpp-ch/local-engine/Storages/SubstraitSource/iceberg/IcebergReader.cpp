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

#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Storages/SubstraitSource/iceberg/EqualityDeleteFileReader.h>

using namespace DB;

namespace local_engine::iceberg
{

std::unique_ptr<IcebergReader> IcebergReader::create(
    const FormatFilePtr & file_,
    const Block & to_read_header_,
    const Block & output_header_,
    const FormatFile::InputFormatPtr & input_format_)
{
    const auto & delete_files = file_->getFileInfo().iceberg().delete_files();
    std::map<IcebergReadOptions::FileContent, std::vector<int>> partitions;
    for (size_t i = 0; i < delete_files.size(); ++i)
        partitions[delete_files[i].filecontent()].push_back(i);

    // TODO: constexpr auto position_delete = IcebergReadOptions::POSITION_DELETES;
    assert(!partitions.contains(IcebergReadOptions::DATA));
    const auto it = partitions.find(IcebergReadOptions::EQUALITY_DELETES);

    const auto & context = file_->getContext();

    ExpressionActionsPtr delete_expr;
    if (it != partitions.end())
    {
        const auto & equality_delete_files = it->second;
        assert(!equality_delete_files.empty());

        EqualityDeleteActionBuilder expressionInputs{context, to_read_header_.getNamesAndTypesList()};
        for (auto deleteIndex : equality_delete_files)
        {
            const auto & delete_file = delete_files[deleteIndex];
            if (delete_file.recordcount() > 0)
                EqualityDeleteFileReader{context, to_read_header_, delete_file}.readDeleteValues(expressionInputs);
        }
        delete_expr = expressionInputs.finish();
    }
    return std::make_unique<IcebergReader>(file_, to_read_header_, output_header_, input_format_, delete_expr);
}

IcebergReader::IcebergReader(
    const FormatFilePtr & file_,
    const Block & to_read_header_,
    const Block & output_header_,
    const FormatFile::InputFormatPtr & input_format_,
    const ExpressionActionsPtr & delete_expr_)
    : NormalFileReader(file_, to_read_header_, output_header_, input_format_)
    , delete_expr(delete_expr_)
    , delete_expr_column_name(EqualityDeleteActionBuilder::COLUMN_NAME)
{
}

Chunk IcebergReader::doPull()
{
    if (!delete_expr)
        return NormalFileReader::doPull();

    while (true)
    {
        Chunk chunk = NormalFileReader::doPull();
        if (chunk.getNumRows() == 0)
            return chunk;

        deleteRows(chunk);

        if (chunk.getNumRows() != 0)
            return chunk;
    }
}

namespace
{
void removeFilterIfNeed(Columns & columns, size_t filter_column_position)
{
    columns.erase(columns.begin() + filter_column_position);
}
}

void IcebergReader::deleteRows(Chunk & chunk) const
{
    // filter out rows that are deleted
    assert(delete_expr);
    assert(!delete_expr_column_name.empty());

    size_t num_rows_before_filtration = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    DataTypes types;
    size_t filter_column_position;
    {
        Block block = readHeader.cloneWithColumns(columns);
        columns.clear();
        delete_expr->execute(block, num_rows_before_filtration);
        filter_column_position = block.getPositionByName(delete_expr_column_name);
        columns = block.getColumns();
        types = block.getDataTypes();
    }

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
        return;

    /// If all the rows pass through the filter.
    if (num_filtered_rows == num_rows_before_filtration)
    {
        /// No need to touch the rest of the columns.
        removeFilterIfNeed(columns, filter_column_position);
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        return;
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
}

}