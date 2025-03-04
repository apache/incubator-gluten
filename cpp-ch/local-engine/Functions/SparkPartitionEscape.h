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
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <bitset>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

using namespace DB;

namespace local_engine
{

class SparkPartitionEscape : public DB::IFunction
{
public:
    static const std::vector<char> ESCAPE_CHAR_LIST;
    static const std::bitset<128> ESCAPE_BITSET;
    static constexpr auto name = "sparkPartitionEscape";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<SparkPartitionEscape>(); }
    SparkPartitionEscape() = default;
    ~SparkPartitionEscape() override = default;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & /*arguments*/) const override;
    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t /*input_rows_count*/) const override;
};

}
