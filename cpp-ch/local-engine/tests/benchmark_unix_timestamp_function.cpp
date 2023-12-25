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

#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>
#include <Functions/SparkFunctionUnixTimestamp.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/FunctionParser.h>
#include <benchmark/benchmark.h>

using namespace DB;

static Block createDataBlock(String type_str, size_t rows)
{
    auto type = DataTypeFactory::instance().get(type_str);
    auto column = type->createColumn();
    for (size_t i = 0; i < rows; ++i)
    {
        if (type_str == "Int32")
        {
            column->insert(static_cast<Int32>(i));
        }
        else if (type_str == "UInt16")
        {
            column->insert(static_cast<UInt16>(i));
        }
    }
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), type, "d"));
    return std::move(block);
}

static void BM_CHUnixTimestamp_For_Int32(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    factory.registerFunction<DB::FunctionToUnixTimestamp>();

    auto function = factory.get("toUnixTimestamp", local_engine::SerializedPlanParser::global_context);
    Block block = createDataBlock("Int32", 30000000);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)[[maybe_unused]]
        auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows());
}

static void BM_SparkUnixTimestamp_For_Int32(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    factory.registerFunction<local_engine::SparkFunctionUnixTimestamp>();

    auto function = factory.get("spark_unix_timestamp", local_engine::SerializedPlanParser::global_context);
    Block block = createDataBlock("Int32", 30000000);
    auto executable = function->build(block.getColumnsWithTypeAndName());
    for (auto _ : state)[[maybe_unused]]
         auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows());
}


BENCHMARK(BM_CHUnixTimestamp_For_Int32)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_SparkUnixTimestamp_For_Int32)->Unit(benchmark::kMillisecond)->Iterations(10);