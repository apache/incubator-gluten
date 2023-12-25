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
#include <Functions/SparkFunctionFloor.h>
#include <Functions/FunctionsRound.h>
#include <Parser/SerializedPlanParser.h>
#include <benchmark/benchmark.h>

using namespace DB;

static Block createDataBlock(String type_str, size_t rows)
{
    auto type = DataTypeFactory::instance().get(type_str);
    auto column = type->createColumn();
    for (size_t i = 0; i < rows; ++i)
    {
        if (isInt(type))
        {
            column->insert(i);
        }
        else if (isFloat(type))
        {
            double d = i * 1.0;
            column->insert(d);
        }
    }
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), type, "d"));
    return std::move(block);
}

static void BM_CHFloorFunction_For_Int64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("floor", local_engine::SerializedPlanParser::global_context);
    Block int64_block = createDataBlock("Int64", 30000000);
    auto executable = function->build(int64_block.getColumnsWithTypeAndName());
    for (auto _ : state)[[maybe_unused]]
        auto result = executable->execute(int64_block.getColumnsWithTypeAndName(), executable->getResultType(), int64_block.rows());
}

static void BM_CHFloorFunction_For_Float64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("floor", local_engine::SerializedPlanParser::global_context);
    Block float64_block = createDataBlock("Float64", 30000000);
    auto executable = function->build(float64_block.getColumnsWithTypeAndName());
    for (auto _ : state)[[maybe_unused]]
        auto result = executable->execute(float64_block.getColumnsWithTypeAndName(), executable->getResultType(), float64_block.rows());
}

static void BM_SparkFloorFunction_For_Int64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkFloor", local_engine::SerializedPlanParser::global_context);
    Block int64_block = createDataBlock("Int64", 30000000);
    auto executable = function->build(int64_block.getColumnsWithTypeAndName());
    for (auto _ : state) [[maybe_unused]]
        auto result = executable->execute(int64_block.getColumnsWithTypeAndName(), executable->getResultType(), int64_block.rows());
}

static void BM_SparkFloorFunction_For_Float64(benchmark::State & state)
{
    using namespace DB;
    auto & factory = FunctionFactory::instance();
    auto function = factory.get("sparkFloor", local_engine::SerializedPlanParser::global_context);
    Block float64_block = createDataBlock("Float64", 30000000);
    auto executable = function->build(float64_block.getColumnsWithTypeAndName());
    for (auto _ : state) [[maybe_unused]] 
        auto result = executable->execute(float64_block.getColumnsWithTypeAndName(), executable->getResultType(), float64_block.rows());
}

BENCHMARK(BM_CHFloorFunction_For_Int64)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_CHFloorFunction_For_Float64)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_SparkFloorFunction_For_Int64)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_SparkFloorFunction_For_Float64)->Unit(benchmark::kMillisecond)->Iterations(10);