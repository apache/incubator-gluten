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

#include <string>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <parquet/schema.h>

using BlockRowType = DB::ColumnsWithTypeAndName;
using BlockFieldType = DB::ColumnWithTypeAndName;
using AnotherRowType = DB::NamesAndTypesList;
using AnotherFieldType = DB::NameAndTypePair;

namespace parquet
{
class ColumnDescriptor;
}

namespace DB
{
struct FormatSettings;
class ReadBuffer;
}

namespace arrow::io
{
class RandomAccessFile;
}

namespace local_engine::test
{
const char * get_data_dir();
std::string data_file(const char * file);

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFileForParquet(DB::ReadBuffer & in, const DB::FormatSettings & settings);

DB::DataTypePtr toDataType(const parquet::ColumnDescriptor & type);

AnotherRowType readParquetSchema(const std::string & file);

DB::ActionsDAGPtr parseFilter(const std::string & filter, const AnotherRowType & name_and_types);

}

inline DB::DataTypePtr BIGINT()
{
    return std::make_shared<DB::DataTypeInt64>();
}
inline DB::DataTypePtr INT()
{
    return std::make_shared<DB::DataTypeInt32>();
}
inline DB::DataTypePtr INT16()
{
    return std::make_shared<DB::DataTypeInt16>();
}
inline DB::DataTypePtr INT8()
{
    return std::make_shared<DB::DataTypeInt8>();
}
inline DB::DataTypePtr UBIGINT()
{
    return std::make_shared<DB::DataTypeUInt64>();
}
inline DB::DataTypePtr UINT()
{
    return std::make_shared<DB::DataTypeUInt32>();
}
inline DB::DataTypePtr UINT16()
{
    return std::make_shared<DB::DataTypeUInt16>();
}
inline DB::DataTypePtr UINT8()
{
    return std::make_shared<DB::DataTypeUInt8>();
}

inline DB::DataTypePtr DOUBLE()
{
    return std::make_shared<DB::DataTypeFloat64>();
}

inline DB::DataTypePtr STRING()
{
    return std::make_shared<DB::DataTypeString>();
}

inline DB::DataTypePtr DATE()
{
    return std::make_shared<DB::DataTypeDate32>();
}

inline BlockFieldType toBlockFieldType(const AnotherFieldType & type)
{
    return BlockFieldType(type.type, type.name);
}

inline AnotherFieldType toAnotherFieldType(const parquet::ColumnDescriptor & type)
{
    return {type.name(), local_engine::test::toDataType(type)};
}

inline BlockRowType toBlockRowType(const AnotherRowType & type, const bool reverse = false)
{
    BlockRowType result;
    result.reserve(type.size());
    if (reverse)
        for (auto it = type.rbegin(); it != type.rend(); ++it)
            result.emplace_back(toBlockFieldType(*it));
    else
        for (const auto & field : type)
            result.emplace_back(toBlockFieldType(field));
    return result;
}

template <class Predicate>
BlockRowType toBlockRowType(const AnotherRowType & type, Predicate predicate)
{
    BlockRowType result;
    result.reserve(type.size());
    for (const auto & field : type)
        if (predicate(field))
            result.emplace_back(toBlockFieldType(field));
    return result;
}

inline parquet::ByteArray ByteArrayFromString(const std::string & s)
{
    const auto * const ptr = reinterpret_cast<const uint8_t *>(s.data());
    return parquet::ByteArray(static_cast<uint32_t>(s.size()), ptr);
}