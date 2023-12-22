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
#include <Parser/SerializedPlanParser.h>
#include <Parser/FunctionParser.h>
#include <Parser/scalar_function_parser/unixTimestamp.h>
#include <benchmark/benchmark.h>

using namespace DB;

static Block createDataBlock(size_t rows)
{
    auto type = DataTypeFactory::instance().get("String");
    auto column = type->createColumn();
    for (size_t i = 0; i < rows; ++i)
    {
        String s = "2023-12-12";
        column->insert(s);
    }
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), type, "d"));
    return std::move(block);
}

static void BM_CHUnixTimestamp(benchmark::State & state)
{
    // using namespace DB;
    // auto & factory = FunctionFactory::instance();
    // factory.registerFunction<DB::FunctionFloor>();

    // auto function = factory.get("floor", local_engine::SerializedPlanParser::global_context);
    // Block int64_block = createDataBlock("Int64", 30000000);
    // auto executable = function->build(int64_block.getColumnsWithTypeAndName());
    // for (auto _ : state)[[maybe_unused]]
    //     auto result = executable->execute(int64_block.getColumnsWithTypeAndName(), executable->getResultType(), int64_block.rows());
}

static void BM_SparkUnixTimestamp(benchmark::State & state)
{
    using namespace DB;
    auto & factory = local_engine::FunctionParserFactory::instance();
    local_engine::SerializedPlanParser parser(local_engine::SerializedPlanParser::global_context);
    auto function_parser = factory.get("unix_timestamp", &parser);
    Block block = createDataBlock(30000000);
    // auto executable = function->build(block.getColumnsWithTypeAndName());
    // for (auto _ : state)[[maybe_unused]]
    //     auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows());
}


BENCHMARK(BM_CHUnixTimestamp)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_SparkUnixTimestamp)->Unit(benchmark::kMillisecond)->Iterations(10);