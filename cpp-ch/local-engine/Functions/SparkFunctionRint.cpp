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

#include "SparkFunctionRint.h"

#include <Columns/ColumnsNumber.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{

DB::ColumnPtr SparkFunctionRint::executeImpl(
    const DB::ColumnsWithTypeAndName & arguments,
    const DB::DataTypePtr & result_type,
    size_t  /*input_rows_count*/) const
{
    if (arguments.size() != 1)
        throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly one argument.", getName());
    if (!isFloat(*arguments[0].type))
        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must be float32 or float64, got {}", getName(), arguments[0].type->getName());

    auto output = result_type->createColumn();
    bool is_float32 = DB::WhichDataType(*arguments[0].type).isFloat32();
    auto input = arguments[0].column->convertToFullIfNeeded();
    auto& output_data = static_cast<DB::ColumnFloat64 *>(output.get())->getData();
    output_data.resize(input->size());
    for (size_t i = 0; i < input->size(); ++i)
    {
        if (is_float32)
            output_data[i] = std::rint(DB::checkAndGetColumn<const DB::ColumnFloat32&>(*input).getData()[i]);
        else
            output_data[i] = std::rint(DB::checkAndGetColumn<const DB::ColumnFloat64&>(*input).getData()[i]);
    }
    return std::move(output);
}


REGISTER_FUNCTION(SparkFunctionRint)
{
    factory.registerFunction<SparkFunctionRint>();
}
}