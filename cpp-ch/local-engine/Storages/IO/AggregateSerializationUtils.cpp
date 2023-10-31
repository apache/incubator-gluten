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
#include "AggregateSerializationUtils.h"
#include <Common/Arena.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>


using namespace DB;

namespace local_engine
{

bool isFixedSizeStateAggregateFunction(const String& name)
{
    // TODO max(String) should exclude, but fallback now
    static const std::set<String> function_set = {"min", "max", "sum", "count", "avg"};
    return function_set.contains(name);
}

DB::ColumnWithTypeAndName convertAggregateStateToFixedString(DB::ColumnWithTypeAndName col)
{
    if (!isAggregateFunction(col.type))
    {
        return col;
    }
    const auto *aggregate_col = checkAndGetColumn<ColumnAggregateFunction>(*col.column);
    // only support known fixed size aggregate function
    if (!isFixedSizeStateAggregateFunction(aggregate_col->getAggregateFunction()->getName()))
    {
        return col;
    }
    size_t state_size = aggregate_col->getAggregateFunction()->sizeOfData();
    auto res_type = std::make_shared<DataTypeFixedString>(state_size);
    auto res_col = res_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnFixedString &>(*res_col).getChars();
    column_chars_t.reserve(aggregate_col->size() * state_size);
    for (const auto & item : aggregate_col->getData())
    {
        column_chars_t.insert_assume_reserved(item, item + state_size);
    }
    return DB::ColumnWithTypeAndName(std::move(res_col), res_type, col.name);
}

DB::ColumnWithTypeAndName convertAggregateStateToString(DB::ColumnWithTypeAndName col)
{
    if (!isAggregateFunction(col.type))
    {
        return col;
    }
    const auto *aggregate_col = checkAndGetColumn<ColumnAggregateFunction>(*col.column);
    auto res_type = std::make_shared<DataTypeString>();
    auto res_col = res_type->createColumn();
    PaddedPODArray<UInt8> & column_chars = assert_cast<ColumnString &>(*res_col).getChars();
    column_chars.reserve(aggregate_col->size() * 60);
    IColumn::Offsets & column_offsets = assert_cast<ColumnString &>(*res_col).getOffsets();
    auto value_writer = WriteBufferFromVector<PaddedPODArray<UInt8>>(column_chars);
    column_offsets.reserve(aggregate_col->size());
    for (const auto & item : aggregate_col->getData())
    {
        aggregate_col->getAggregateFunction()->serialize(item, value_writer);
        writeChar('\0', value_writer);
        column_offsets.emplace_back(value_writer.count());
    }
    return DB::ColumnWithTypeAndName(std::move(res_col), res_type, col.name);
}

DB::ColumnWithTypeAndName convertFixedStringToAggregateState(DB::ColumnWithTypeAndName col, DB::DataTypePtr type)
{
    chassert(isAggregateFunction(type));
    auto res_col = type->createColumn();
    const auto * agg_type = checkAndGetDataType<DataTypeAggregateFunction>(type.get());
    ColumnAggregateFunction & real_column = typeid_cast<ColumnAggregateFunction &>(*res_col);
    auto & arena = real_column.createOrGetArena();
    ColumnAggregateFunction::Container & vec = real_column.getData();

    vec.reserve(col.column->size());
    auto agg_function = agg_type->getFunction();
    size_t size_of_state = agg_function->sizeOfData();
    size_t align_of_state = agg_function->alignOfData();

    for (size_t i = 0; i < col.column->size(); ++i)
    {
        AggregateDataPtr place = arena.alignedAlloc(size_of_state, align_of_state);

        agg_function->create(place);

        auto value = col.column->getDataAt(i);
        memcpy(place, value.data, value.size);

        vec.push_back(place);
    }
    return DB::ColumnWithTypeAndName(std::move(res_col), type, col.name);
}
DB::Block convertAggregateStateInBlock(DB::Block block)
{
    ColumnsWithTypeAndName columns;
    for (const auto & item : block.getColumnsWithTypeAndName())
    {
        columns.emplace_back(convertAggregateStateToString(item));
    }
    return columns;
}
}

