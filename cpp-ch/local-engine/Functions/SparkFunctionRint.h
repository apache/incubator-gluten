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

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

namespace local_engine
{
class SparkFunctionRint : public DB::IFunction
{
public:
    static constexpr auto name = "sparkRint";

    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionRint>(); }

    SparkFunctionRint() = default;

    ~SparkFunctionRint() override = default;

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo &) const override { return true; }

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments,
        const DB::DataTypePtr & result_type,
        size_t input_rows_count) const override;

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes &) const override
    {
        return std::make_shared<DB::DataTypeFloat64>();
    }
};
}
