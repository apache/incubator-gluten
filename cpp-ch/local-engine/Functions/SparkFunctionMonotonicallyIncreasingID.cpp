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

#include <atomic>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
/**
 * Returns monotonically increasing 64-bit integers. The generated ID is guaranteed
 *     to be monotonically increasing and unique, but not consecutive. The current implementation
 *     puts the partition ID in the upper 31 bits, and the lower 33 bits represent the record number
 *     within each partition. The assumption is that the data frame has less than 1 billion
 *     partitions, and each partition has less than 8 billion records.
 *     The function is non-deterministic because its result depends on partition IDs.
 */
class SparkFunctionMonotonicallyIncreasingID : public DB::IFunction
{
public:
    static constexpr auto name = "sparkMonotonicallyIncreasingId";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionMonotonicallyIncreasingID>(); }

    String getName() const override { return name; }

    bool isStateful() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }
    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw DB::Exception(
                DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1.",
                getName(),
                arguments.size());

        if (!isInteger(arguments[0]))
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must be integer", getName());

        return std::make_shared<DB::DataTypeInt64>();
    }

    DB::ColumnPtr
    executeImplDryRun(const DB::ColumnsWithTypeAndName &, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return DB::ColumnInt64::create(input_rows_count);
    }

    DB::ColumnPtr
    executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto partitionIndex = arguments[0].column->getInt(0);
        const auto partitionMask = partitionIndex << 33;

        auto result = result_type->createColumn();
        result->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto a=partitionMask + count.load(std::memory_order_relaxed);
            result->insert(a);
            count.fetch_add(1, std::memory_order_relaxed);
        }

        return result;
    }

private:
    mutable std::atomic<size_t> count{0};
};


REGISTER_FUNCTION(SparkFunctionMonotonicallyIncreasingID)
{
    factory.registerFunction<SparkFunctionMonotonicallyIncreasingID>();
}

}