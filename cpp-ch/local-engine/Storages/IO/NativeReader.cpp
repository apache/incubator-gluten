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
#include "NativeReader.h"

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/Arena.h>
#include <Storages/IO/NativeWriter.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_INDEX;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
    extern const int TOO_LARGE_ARRAY_SIZE;
}
}

using namespace DB;

namespace local_engine
{
void NativeReader::readData(const ISerialization & serialization, ColumnPtr & column, ReadBuffer & istr, size_t rows, double avg_value_size_hint)
{
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.avg_value_size_hint = avg_value_size_hint;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    ISerialization::DeserializeBinaryBulkStatePtr state;

    serialization.deserializeBinaryBulkStatePrefix(settings, state);
    serialization.deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

    if (column->size() != rows)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "Cannot read all data in NativeReader. Rows read: {}. Rows expected: {}", column->size(), rows);
}

void NativeReader::readAggData(const DB::DataTypeAggregateFunction & data_type, DB::ColumnPtr & column, DB::ReadBuffer & istr, size_t rows)
{
    ColumnAggregateFunction & real_column = typeid_cast<ColumnAggregateFunction &>(*column->assumeMutable());
    auto & arena = real_column.createOrGetArena();
    ColumnAggregateFunction::Container & vec = real_column.getData();

    vec.reserve(rows);
    auto agg_function = data_type.getFunction();
    size_t size_of_state = agg_function->sizeOfData();
    size_t align_of_state = agg_function->alignOfData();

    for (size_t i = 0; i < rows; ++i)
    {
        AggregateDataPtr place = arena.alignedAlloc(size_of_state, align_of_state);

        agg_function->create(place);

        auto n = istr.read(place, size_of_state);
        chassert(n == size_of_state);
        vec.push_back(place);
    }
}


Block NativeReader::getHeader() const
{
    return header;
}

Block NativeReader::read()
{
    Block res;

    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    if (istr.eof())
    {
        return res;
    }

    /// Dimensions
    size_t columns = 0;
    size_t rows = 0;

    readVarUInt(columns, istr);
    readVarUInt(rows, istr);

    if (columns > 1'000'000uz)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Suspiciously many columns in Native format: {}", columns);
    if (rows > 1'000'000'000'000uz)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Suspiciously many rows in Native format: {}", rows);

    if (columns == 0 && !header && rows != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Zero columns but {} rows in Native format.", rows);

    for (size_t i = 0; i < columns; ++i)
    {
        ColumnWithTypeAndName column;

        column.name = "col_" + std::to_string(i);

        /// Type
        String type_name;
        readBinary(type_name, istr);
        bool agg_opt_column = false;
        String real_type_name = type_name;
        if (type_name.ends_with(NativeWriter::AGG_STATE_SUFFIX))
        {
            agg_opt_column = true;
            real_type_name = type_name.substr(0, type_name.length() - NativeWriter::AGG_STATE_SUFFIX.length());
        }
        column.type = data_type_factory.get(real_type_name);
        bool is_agg_state_type = isAggregateFunction(column.type);
        SerializationPtr serialization = column.type->getDefaultSerialization();

        /// Data
        ColumnPtr read_column = column.type->createColumn(*serialization);

        double avg_value_size_hint = avg_value_size_hints.empty() ? 0 : avg_value_size_hints[i];
        if (rows)    /// If no rows, nothing to read.
        {
            if (is_agg_state_type && agg_opt_column)
            {
                const DataTypeAggregateFunction * agg_type = checkAndGetDataType<DataTypeAggregateFunction>(column.type.get());
                readAggData(*agg_type, read_column, istr, rows);
            }
            else
            {
                readData(*serialization, read_column, istr, rows, avg_value_size_hint);
            }
        }
        column.column = std::move(read_column);

        res.insert(std::move(column));
    }

    if (res.rows() != rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Row count mismatch after deserialization, got: {}, expected: {}", res.rows(), rows);

    return res;
}

}
