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
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/SparkFunctionDivide.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sources/BlocksSource.h>
#include <benchmark/benchmark.h>
#include <Common/QueryContext.h>

using namespace DB;

static Block createDataBlock(String name1, String name2, size_t rows)
{
    auto type = DataTypeFactory::instance().get("Float64");
    auto column1 = type->createColumn();
    auto column2 = type->createColumn();
    for (size_t i = 0; i < rows; ++i)
    {
        double d = i * 1.0;
        column1->insert(d);
        column2->insert(0);
    }
    ColumnWithTypeAndName col_type_name_1(std::move(column1), type, name1);
    ColumnWithTypeAndName col_type_name_2(std::move(column2), type, name2);
    Block block;
    block.insert(col_type_name_1);
    block.insert(col_type_name_2);
    return std::move(block);
}


static std::string join(const ActionsDAG::NodeRawConstPtrs & v, char c)
{
    std::string res;
    for (size_t i = 0; i < v.size(); ++i)
    {
        if (i)
            res += c;
        res += v[i]->result_name;
    }
    return res;
}

static const ActionsDAG::Node *
addFunction(ActionsDAG & actions_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args)
{
    auto function_builder = FunctionFactory::instance().get(function, local_engine::QueryContext::globalContext());
    std::string args_name = join(args, ',');
    auto result_name = function + "(" + args_name + ")";
    return &actions_dag.addFunction(function_builder, args, result_name);
}

static void BM_CHDivideFunction(benchmark::State & state)
{
    ActionsDAG dag;
    Block block = createDataBlock("d1", "d2", 30000000);
    ColumnWithTypeAndName col1 = block.getByPosition(0);
    ColumnWithTypeAndName col2 = block.getByPosition(1);
    const ActionsDAG::Node * left_arg = &dag.addColumn(col1);
    const ActionsDAG::Node * right_arg = &dag.addColumn(col2);
    addFunction(dag, "divide", {left_arg, right_arg});
    ExpressionActions expr_actions(std::move(dag));
    for (auto _ : state)
        expr_actions.execute(block);
}

static void BM_SparkDivideFunction(benchmark::State & state)
{
    ActionsDAG dag;
    Block block = createDataBlock("d1", "d2", 30000000);
    ColumnWithTypeAndName col1 = block.getByPosition(0);
    ColumnWithTypeAndName col2 = block.getByPosition(1);
    const ActionsDAG::Node * left_arg = &dag.addColumn(col1);
    const ActionsDAG::Node * right_arg = &dag.addColumn(col2);
    addFunction(dag, "sparkDivide", {left_arg, right_arg});
    ExpressionActions expr_actions(std::move(dag));
    for (auto _ : state)
        expr_actions.execute(block);
}

static void BM_GlutenDivideFunctionParser(benchmark::State & state)
{
    ActionsDAG dag;
    Block block = createDataBlock("d1", "d2", 30000000);
    ColumnWithTypeAndName col1 = block.getByPosition(0);
    ColumnWithTypeAndName col2 = block.getByPosition(1);
    const ActionsDAG::Node * left_arg = &dag.addColumn(col1);
    const ActionsDAG::Node * right_arg = &dag.addColumn(col2);
    const ActionsDAG::Node * divide_arg = addFunction(dag, "divide", {left_arg, right_arg});
    DataTypePtr float64_type = std::make_shared<DataTypeFloat64>();
    ColumnWithTypeAndName col_zero(float64_type->createColumnConst(1, 0), float64_type, toString(0));
    ColumnWithTypeAndName col_null(float64_type->createColumnConst(1, Field{}), float64_type, "null");
    const ActionsDAG::Node * zero_arg = &dag.addColumn(col_zero);
    const ActionsDAG::Node * null_arg = &dag.addColumn(col_null);
    const ActionsDAG::Node * equals_arg = addFunction(dag, "equals", {right_arg, zero_arg});
    const ActionsDAG::Node * if_arg = addFunction(dag, "if", {equals_arg, null_arg, divide_arg});
    ExpressionActions expr_actions(std::move(dag));
    for (auto _ : state)
        expr_actions.execute(block);
}

BENCHMARK(BM_CHDivideFunction)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_SparkDivideFunction)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_GlutenDivideFunctionParser)->Unit(benchmark::kMillisecond)->Iterations(10);