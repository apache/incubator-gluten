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
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Parser/FunctionParser.h>
#include <benchmark/benchmark.h>
#include <Common/QueryContext.h>

using namespace DB;

static Block createDataBlock(size_t rows)
{
   auto type = DataTypeFactory::instance().get("String");
   auto column = type->createColumn();
   for (size_t i = 0; i < rows; ++i)
   {
       char ch = static_cast<char>(i % 128);
       std::string str = "escape_" + ch;
       column->insert(str);
   }
   Block block;
   block.insert(ColumnWithTypeAndName(std::move(column), type, "d"));
   return std::move(block);
}

static void BM_CHSparkPartitionEscape(benchmark::State & state)
{
   using namespace DB;
   auto & factory = FunctionFactory::instance();
   auto function = factory.get("sparkPartitionEscape", local_engine::QueryContext::globalContext());
   Block block = createDataBlock(1000000);
   auto executable = function->build(block.getColumnsWithTypeAndName());
   for (auto _ : state) [[maybe_unused]]
       auto result = executable->execute(block.getColumnsWithTypeAndName(), executable->getResultType(), block.rows(), false);
}

BENCHMARK(BM_CHSparkPartitionEscape)->Unit(benchmark::kMillisecond)->Iterations(50);