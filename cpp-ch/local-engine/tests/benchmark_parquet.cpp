#include <filesystem>
#include <iostream>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <IO/ReadBufferFromFile.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <benchmark/benchmark.h>
#include "Storages/ch_parquet/ParquetFileReader.h"

using namespace DB;

const String data_path = "/home/saber/dev/data/tpch100/parquet/lineitem";

DataTypePtr string_type = makeNullable(std::make_shared<DataTypeString>());
DataTypePtr int64_type = makeNullable(std::make_shared<DataTypeInt64>());
DataTypePtr double_type = makeNullable(std::make_shared<DataTypeFloat64>());
DataTypePtr date_type = makeNullable(std::make_shared<DataTypeDate32>());

//DataTypePtr string_type = std::make_shared<DataTypeString>();
//DataTypePtr int64_type = std::make_shared<DataTypeInt64>();
//DataTypePtr double_type = std::make_shared<DataTypeFloat64>();
//DataTypePtr date_type = std::make_shared<DataTypeDate32>();


SharedContextHolder shared_context = Context::createShared();
ContextMutablePtr context = Context::createGlobal(shared_context.get());

void testCustomParquet(int chunk_size, int)
{
    for (const auto & entry : fs::directory_iterator(data_path))
    {
        if (entry.path().extension() != ".parquet")
            continue;
        auto file = std::make_shared<ReadBufferFromFile>(entry.path());
        ScanParam param;
        //        param.header.insert({int64_type, "l_orderkey"});
        //        param.header.insert({int64_type, "l_partkey"});
        //        param.header.insert({int64_type, "l_suppkey"});
        //        param.header.insert({int64_type, "l_linenumber"});
        param.header.insert({double_type, "l_quantity"});
        param.header.insert({double_type, "l_extendedprice"});
        param.header.insert({double_type, "l_discount"});
        param.header.insert({double_type, "l_tax"});
        param.header.insert({string_type, "l_returnflag"});
        param.header.insert({string_type, "l_linestatus"});
        param.header.insert({date_type, "l_shipdate"});
        //        param.header.insert({date_type, "l_commitdate"});
        //        param.header.insert({date_type, "l_receiptdate"});
        //        param.header.insert({string_type, "l_shipinstruct"});
        //        param.header.insert({string_type, "l_shipmode"});
        //        param.header.insert({string_type, "l_comment"});

        param.case_sensitive = false;

        param.active_columns = {0, 1, 2, 3, 4, 5};

        ActionsDAGPtr dag = std::make_shared<ActionsDAG>();
        FunctionOverloadResolverPtr func_builder_lt
            = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionComparison<LessOp, NameLess>>(context));
        FunctionOverloadResolverPtr func_builder_ge = std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>>(context));
        FunctionOverloadResolverPtr func_builder_le = std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionComparison<LessOrEqualsOp, NameLessOrEquals>>(context));
        FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());


        //        const auto & l_shipdate = dag->addInput("l_shipdate", date_type);
        //        const auto & l_quantity = dag->addInput("l_quantity", double_type);
        //        const auto & l_discount = dag->addInput("l_discount", double_type);
        //        const auto & const_8766 = dag->addColumn({date_type->createColumnConst(1, 8766), date_type, "8766"});
        //        const auto & const_9131 = dag->addColumn({date_type->createColumnConst(1, 9131), date_type, "9131"});
        //        const auto & const_24 = dag->addColumn({double_type->createColumnConst(1, 24.0), double_type, "24"});
        //        const auto & const_005 = dag->addColumn({double_type->createColumnConst(1, 0.05), double_type, "0.05"});
        //        const auto & const_007 = dag->addColumn({double_type->createColumnConst(1, 0.07), double_type, "0.07"});
        //
        //        const auto & condition1_node = dag->addFunction(func_builder_ge, {&l_shipdate, &const_8766}, "");
        //        const auto & condition2_node = dag->addFunction(func_builder_lt, {&l_shipdate, &const_9131}, "");
        //        const auto & condition3_node = dag->addFunction(func_builder_ge, {&l_discount, &const_005}, "");
        //        const auto & condition4_node = dag->addFunction(func_builder_le, {&l_discount, &const_007}, "");
        //        const auto & condition5_node = dag->addFunction(func_builder_lt, {&l_quantity, &const_24}, "");
        //
        //        const auto & and_node_1 = dag->addFunction(func_builder_and, {&condition1_node, &condition2_node}, "");
        //        const auto & and_node_2 = dag->addFunction(func_builder_and, {&and_node_1, &condition3_node}, "");
        //        const auto & and_node_3 = dag->addFunction(func_builder_and, {&and_node_2, &condition4_node}, "");
        //        const auto & and_node_4 = dag->addFunction(func_builder_and, {&and_node_3, &condition5_node}, "");

        const auto & l_shipdate = dag->addInput("l_shipdate", date_type);
        const auto & const_10472 = dag->addColumn({date_type->createColumnConst(1, 10472), date_type, "10472"});
        const auto & condition1_node = dag->addFunction(func_builder_lt, {&l_shipdate, &const_10472}, "");


        dag->addOrReplaceInOutputs(condition1_node);
//        if (type == 1)
//        {
            param.filter = std::make_shared<PushDownFilter>(dag);
            param.groupFilter = param.filter->getRowGroupFilter();
//        }

        auto reader = std::make_shared<ParquetFileReader>(file.get(), param, chunk_size);
        reader->init();
        [[maybe_unused]] size_t count = 0;
        while (true)
        {
            auto chunk = reader->getNext();
            if (chunk.getNumRows() == 0)
                break;
            count += chunk.getNumRows();
        }
    }
}

void testCommunityParquet(int chunk_size)
{
    for (const auto & entry : fs::directory_iterator(data_path))
    {
        if (entry.path().extension() != ".parquet")
            continue;
        auto file = std::make_shared<ReadBufferFromFile>(entry.path());
        Block header;
        //        header.insert({int64_type, "l_orderkey"});
        //        header.insert({int64_type, "l_partkey"});
        //        header.insert({int64_type, "l_suppkey"});
        //        header.insert({int64_type, "l_linenumber"});
        header.insert({double_type, "l_quantity"});
        header.insert({double_type, "l_extendedprice"});
        header.insert({double_type, "l_discount"});
        //        header.insert({double_type, "l_tax"});
        //        header.insert({string_type, "l_returnflag"});
        //        header.insert({string_type, "l_linestatus"});
        header.insert({date_type, "l_shipdate"});
        //        header.insert({date_type, "l_commitdate"});
        //        header.insert({date_type, "l_receiptdate"});
        //        header.insert({string_type, "l_shipinstruct"});
        //        header.insert({string_type, "l_shipmode"});
        //        header.insert({string_type, "l_comment"});

        FormatSettings settings;
        auto input_format = std::make_shared<ParquetBlockInputFormat>(file.get(), nullptr, header, settings, 1, chunk_size);
        QueryPipelineBuilder builder;
        builder.init(Pipe(input_format));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
        auto executor = std::make_shared<PullingPipelineExecutor>(pipeline);
        Chunk chunk;
        [[maybe_unused]] size_t count = 0;
        while (executor->pull(chunk))
        {
            count += chunk.getNumRows();
        }
    }
}


void BM_CustomParquet(benchmark::State & state)
{
    for (auto _ : state)
    {
        testCustomParquet(state.range(0), state.range(1));
    }
}
void BM_CommunityParquet(benchmark::State & state)
{
    for (auto _ : state)
    {
        testCommunityParquet(state.range(0));
    }
}

BENCHMARK(BM_CustomParquet)
    //    ->Arg(2048)
    //    ->Arg(4096)
    ->Args({8192, 1})
//    ->Args({8192, 2})
//    ->Args({8192 * 2, 1})
//    ->Args({8192 * 2, 2})
    //    ->Arg(8192 * 2)
    //    ->Arg(8192 * 3)
    //    ->Arg(8192 * 4)
    ->MinWarmUpTime(2)
    ->MinTime(10)
    ->Repetitions(5)
    //    ->Threads(8)
    ->Unit(benchmark::TimeUnit::kMillisecond);
//
//BENCHMARK(BM_CommunityParquet)
////    ->Arg(2048)
////    ->Arg(4096)
//    ->Arg(8192)
////    ->Arg(8192 * 2)
////    ->Arg(8192 * 3)
////    ->Arg(8192 * 4)
//    ->MinWarmUpTime(2)
//    ->MinTime(20)
////        ->Threads(8)
//    ->Unit(benchmark::TimeUnit::kMillisecond);


// Run the benchmark
BENCHMARK_MAIN();
