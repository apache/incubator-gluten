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

#include <Core/Block.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <substrait/type.pb.h>

namespace local_engine
{
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

inline DB::Block makeBlockHeader(const DB::ColumnsWithTypeAndName & data_)
{
    return DB::Block(data_);
}

DB::NamesAndTypesList blockToNameAndTypeList(const DB::Block & header);
DB::DataTypePtr wrapNullableType(bool nullable, DB::DataTypePtr nested_type);

inline DB::DataTypePtr wrapNullableType(const substrait::Type_Nullability nullable, const DB::DataTypePtr & nested_type)
{
    return wrapNullableType(nullable == substrait::Type_Nullability_NULLABILITY_NULLABLE, nested_type);
}

}
