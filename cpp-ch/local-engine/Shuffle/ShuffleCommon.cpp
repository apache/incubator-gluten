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
#include <Shuffle/ShuffleCommon.h>
#include <Storages/IO/AggregateSerializationUtils.h>
#include <Poco/StringTokenizer.h>

namespace local_engine
{
void ColumnsBuffer::add(DB::Block & block, int start, int end)
{
    if (!header)
        header = block.cloneEmpty();

    if (accumulated_columns.empty())
    {
        accumulated_columns.reserve(block.columns());
        for (size_t i = 0; i < block.columns(); i++)
        {
            auto column = block.getColumns()[i]->cloneEmpty();
            column->reserve(prefer_buffer_size);
            accumulated_columns.emplace_back(std::move(column));
        }
    }

    assert(!accumulated_columns.empty());
    for (size_t i = 0; i < block.columns(); ++i)
    {
        if (!accumulated_columns[i]->onlyNull())
        {
            accumulated_columns[i]->insertRangeFrom(*block.getByPosition(i).column, start, end - start);
        }
        else
        {
            accumulated_columns[i]->insertMany(DB::Field(), end - start);
        }
    }
}

void ColumnsBuffer::appendSelective(
    size_t column_idx,
    const DB::Block & source,
    const DB::IColumn::Selector & selector,
    size_t from,
    size_t length)
{
    if (!header)
        header = source.cloneEmpty();

    if (accumulated_columns.empty())
    {
        accumulated_columns.reserve(source.columns());
        for (size_t i = 0; i < source.columns(); i++)
        {
            auto column = source.getColumns()[i]->convertToFullIfNeeded()->cloneEmpty();
            column->reserve(prefer_buffer_size);
            accumulated_columns.emplace_back(std::move(column));
        }
    }

    if (!accumulated_columns[column_idx]->onlyNull())
    {
        accumulated_columns[column_idx]->insertRangeSelective(
            *source.getByPosition(column_idx).column->convertToFullIfNeeded(),
            selector,
            from,
            length);
    }
    else
    {
        accumulated_columns[column_idx]->insertMany(DB::Field(), length);
    }
}

size_t ColumnsBuffer::size() const
{
    return accumulated_columns.empty() ? 0 : accumulated_columns[0]->size();
}

bool ColumnsBuffer::empty() const
{
    return accumulated_columns.empty() ? true : accumulated_columns[0]->empty();
}

DB::Block ColumnsBuffer::releaseColumns()
{
    DB::Columns columns(std::make_move_iterator(accumulated_columns.begin()), std::make_move_iterator(accumulated_columns.end()));
    accumulated_columns.clear();

    if (columns.empty())
        return header.cloneEmpty();
    else
        return header.cloneWithColumns(columns);
}

DB::Block ColumnsBuffer::getHeader()
{
    return header;
}

ColumnsBuffer::ColumnsBuffer(size_t prefer_buffer_size_)
    : prefer_buffer_size(prefer_buffer_size_)
{
}
}