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
#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/SubstraitSource/Delta/Bitmap/DeltaDVRoaringBitmapArray.h>


namespace local_engine
{

struct AggregateFunctionDVRoaringBitmapData
{
    AggregateFunctionDVRoaringBitmapData() { }

    DeltaDVRoaringBitmapArray roaring_bitmap_array;

    void insertResultInto(DB::ColumnInt64 & cardinality, DB::ColumnInt64 & last, DB::ColumnString & bitmap)
    {
        cardinality.getData().push_back(roaring_bitmap_array.cardinality());
        auto last_value = roaring_bitmap_array.last();
        if (last_value.has_value())
            last.getData().push_back(last_value.value());
        else
            last.insertDefault();

        bitmap.insert(roaring_bitmap_array.serialize());
    }

    void write(DB::WriteBuffer & buf) const
    {
        DB::writeString(roaring_bitmap_array.serialize(), buf);
    }

    void read(DB::ReadBuffer & buf)
    {
        roaring_bitmap_array.deserialize(buf);
    }
};


template <typename T, typename Data>
class AggregateFunctionDVRoaringBitmap final : public DB::IAggregateFunctionDataHelper<Data, AggregateFunctionDVRoaringBitmap<T, Data>>
{
public:
    AggregateFunctionDVRoaringBitmap(const DB::DataTypes & argument_types_, const DB::Array & parameters_)
        : DB::IAggregateFunctionDataHelper<Data, AggregateFunctionDVRoaringBitmap<T, Data>>(
              argument_types_, parameters_, createResultType())
    {
    }

    static DB::DataTypePtr createResultType()
    {
        DB::DataTypes types;
        auto cardinality = std::make_shared<DB::DataTypeInt64>();
        auto last = std::make_shared<DB::DataTypeInt64>();
        auto bitmap = std::make_shared<DB::DataTypeString>();

        types.emplace_back(cardinality);
        types.emplace_back(last);
        types.emplace_back(bitmap);

        return std::make_shared<DB::DataTypeTuple>(types);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(DB::AggregateDataPtr __restrict place, const DB::IColumn ** columns, size_t row_num, DB::Arena *) const override
    {
        this->data(place).roaring_bitmap_array.rb_add(assert_cast<const DB::ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(DB::AggregateDataPtr __restrict place, DB::ConstAggregateDataPtr rhs, DB::Arena *) const override
    {
        this->data(place).roaring_bitmap_array.rb_merge(this->data(rhs).roaring_bitmap_array);
    }

    void insertResultInto(DB::AggregateDataPtr __restrict place, DB::IColumn & to, DB::Arena *) const override
    {
        auto & to_tuple = assert_cast<DB::ColumnTuple &>(to);
        auto & cardinality = assert_cast<DB::ColumnInt64 &>(to_tuple.getColumn(0));
        auto & last = assert_cast<DB::ColumnInt64 &>(to_tuple.getColumn(1));
        auto & bitmap = assert_cast<DB::ColumnString &>(to_tuple.getColumn(2));
        this->data(place).insertResultInto(cardinality, last, bitmap);
    }

    String getName() const override { return "bitmapaggregator"; }

    void serialize(DB::ConstAggregateDataPtr place, DB::WriteBuffer & buf, std::optional<size_t> version) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(DB::AggregateDataPtr place, DB::ReadBuffer & buf, std::optional<size_t> version, DB::Arena * arena) const override
    {
        this->data(place).read(buf);
    }
};
}
