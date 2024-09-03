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
#include <exception>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/WindowGroupLimitFunctions.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/WindowTransform.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace local_engine
{
WindowFunctionTopRowNumber::WindowFunctionTopRowNumber(const String name, const DB::DataTypes & arg_types, const DB::Array & parameters_)
    : DB::WindowFunction(name, arg_types, parameters_, std::make_shared<DB::DataTypeUInt64>())
{
    if (parameters.size() != 1)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "{} needs a limit parameter", name);
    limit = parameters[0].safeGet<UInt64>();
    LOG_ERROR(getLogger("WindowFunctionTopRowNumber"), "xxx {} limit: {}", name, limit);
}

void WindowFunctionTopRowNumber::windowInsertResultInto(const DB::WindowTransform * transform, size_t function_index) const
{
    LOG_ERROR(
        getLogger("WindowFunctionTopRowNumber"),
        "xxx current row number: {}, current_row: {}@{}, partition_ended: {}",
        transform->current_row_number,
        transform->current_row.block,
        transform->current_row.row,
        transform->partition_ended);
    /// If the rank value is larger then limit, and current block only contains rows which are all belong to one partition.
    /// We cant drop this block directly.
    if (!transform->partition_ended && !transform->current_row.row && transform->current_row_number > limit)
    {
        /// It's safe to make it mutable here. but it's still too dangerous, it may be changed in the future and make it unsafe.
        auto * mutable_transform = const_cast<DB::WindowTransform *>(transform);
        DB::WindowTransformBlock & current_block = mutable_transform->blockAt(mutable_transform->current_row);
        current_block.rows = 0;
        auto clear_columns = [](DB::Columns & cols)
        {
            DB::Columns new_cols;
            for (const auto & col : cols)
            {
                new_cols.push_back(std::move(col->cloneEmpty()));
            }
            cols = new_cols;
        };
        clear_columns(current_block.original_input_columns);
        clear_columns(current_block.input_columns);
        clear_columns(current_block.casted_columns);
        mutable_transform->current_row.block += 1;
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "{} is not implemented", name);
    }
    else
    {
        auto & to_col = *transform->blockAt(transform->current_row).output_columns[function_index];
        assert_cast<DB::ColumnUInt64 &>(to_col).getData().push_back(transform->current_row_number);
    }
}

void registerWindowGroupLimitFunctions(DB::AggregateFunctionFactory & factory)
{
    const DB::AggregateFunctionProperties properties
        = {.returns_default_when_only_null = true, .is_order_dependent = true, .is_window_function = true};
    factory.registerFunction(
        "top_row_number",
        {[](const String & name, const DB::DataTypes & args_type, const DB::Array & parameters, const DB::Settings *)
         { return std::make_shared<WindowFunctionTopRowNumber>(name, args_type, parameters); },
         properties},
        DB::AggregateFunctionFactory::Case::Insensitive);
}
}
