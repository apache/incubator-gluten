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
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromFile.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/ch_parquet/OptimizedArrowColumnToCHColumn.h>
#include <Storages/ch_parquet/OptimizedParquetBlockInputFormat.h>
#include <Storages/ch_parquet/arrow/reader.h>
#include <benchmark/benchmark.h>
#include <parquet/arrow/reader.h>
#include <substrait/plan.pb.h>
#include <Common/DebugUtils.h>

static void BM_ParquetReadString(benchmark::State & state)
{
    using namespace DB;
    Block header{
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_returnflag"),
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_linestatus")};
    std::string file = "/data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/"
                       "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    FormatSettings format_settings;
    Block res;
    for (auto _ : state)
    {
        auto in = std::make_unique<ReadBufferFromFile>(file);
        auto format = std::make_shared<ParquetBlockInputFormat>(*in, header, format_settings, 1, 8192);
        auto pipeline = QueryPipeline(std::move(format));
        auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        while (reader->pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

static void BM_ParquetReadDate32(benchmark::State & state)
{
    using namespace DB;
    Block header{
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_shipdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_commitdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_receiptdate")};
    std::string file = "/data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/"
                       "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    FormatSettings format_settings;
    Block res;
    for (auto _ : state)
    {
        auto in = std::make_unique<ReadBufferFromFile>(file);
        auto format = std::make_shared<ParquetBlockInputFormat>(*in, header, format_settings, 1, 8192);
        auto pipeline = QueryPipeline(std::move(format));
        auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);
        while (reader->pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

static void BM_OptimizedParquetReadString(benchmark::State & state)
{
    using namespace DB;
    using namespace local_engine;
    Block header{
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_returnflag"),
        ColumnWithTypeAndName(DataTypeString().createColumn(), std::make_shared<DataTypeString>(), "l_linestatus")};
    std::string file = "file:///data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/"
                       "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    Block res;

    for (auto _ : state)
    {
        substrait::ReadRel::LocalFiles files;
        substrait::ReadRel::LocalFiles::FileOrFiles * file_item = files.add_items();
        file_item->set_uri_file(file);
        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        file_item->mutable_parquet()->CopyFrom(parquet_format);

        auto builder = std::make_unique<QueryPipelineBuilder>();
        builder->init(
            Pipe(std::make_shared<local_engine::SubstraitFileSource>(local_engine::SerializedPlanParser::global_context, header, files)));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        auto reader = PullingPipelineExecutor(pipeline);
        while (reader.pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

static void BM_OptimizedParquetReadDate32(benchmark::State & state)
{
    using namespace DB;
    using namespace local_engine;
    Block header{
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_shipdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_commitdate"),
        ColumnWithTypeAndName(DataTypeDate32().createColumn(), std::make_shared<DataTypeDate32>(), "l_receiptdate")};
    std::string file = "file:///data1/liyang/cppproject/gluten/jvm/src/test/resources/tpch-data/lineitem/"
                       "part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet";
    Block res;

    for (auto _ : state)
    {
        substrait::ReadRel::LocalFiles files;
        substrait::ReadRel::LocalFiles::FileOrFiles * file_item = files.add_items();
        file_item->set_uri_file(file);
        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        file_item->mutable_parquet()->CopyFrom(parquet_format);

        auto builder = std::make_unique<QueryPipelineBuilder>();
        builder->init(
            Pipe(std::make_shared<local_engine::SubstraitFileSource>(local_engine::SerializedPlanParser::global_context, header, files)));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        auto reader = PullingPipelineExecutor(pipeline);
        while (reader.pull(res))
        {
            // debug::headBlock(res);
        }
    }
}

BENCHMARK(BM_ParquetReadString)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_ParquetReadDate32)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_OptimizedParquetReadString)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_OptimizedParquetReadDate32)->Unit(benchmark::kMillisecond)->Iterations(200);
