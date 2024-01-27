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
#include <fstream>
#include <iostream>
#include <Builder/SerializedPlanBuilder.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TreeRewriter.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/SparkRowToCHColumn.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/CustomMergeTreeSink.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <gtest/gtest.h>
#include <substrait/plan.pb.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>
#include "testConfig.h"

using namespace local_engine;
using namespace dbms;

TEST(TestSelect, ReadRel)
{
    GTEST_SKIP();
    dbms::SerializedSchemaBuilder schema_builder;
    auto * schema = schema_builder.column("sepal_length", "FP64")
                        .column("sepal_width", "FP64")
                        .column("petal_length", "FP64")
                        .column("petal_width", "FP64")
                        .column("type", "I64")
                        .column("type_string", "String")
                        .build();
    dbms::SerializedPlanBuilder plan_builder;
    auto plan = plan_builder.read(TEST_DATA(/ data / iris.parquet), std::move(schema)).build();

    ASSERT_TRUE(plan->relations(0).root().input().has_read());
    ASSERT_EQ(plan->relations_size(), 1);
    local_engine::LocalExecutor local_executor;
    local_engine::SerializedPlanParser parser(local_engine::SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_GT(spark_row_info->getNumRows(), 0);
        local_engine::SparkRowToCHColumn converter;
        auto block = converter.convertSparkRowInfoToCHColumn(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
    }
}

TEST(TestSelect, ReadDate)
{
    GTEST_SKIP();
    dbms::SerializedSchemaBuilder schema_builder;
    auto * schema = schema_builder.column("date", "Date").build();
    dbms::SerializedPlanBuilder plan_builder;
    auto plan = plan_builder.read(TEST_DATA(/ data / date.parquet), std::move(schema)).build();

    ASSERT_TRUE(plan->relations(0).root().input().has_read());
    ASSERT_EQ(plan->relations_size(), 1);
    local_engine::LocalExecutor local_executor;
    local_engine::SerializedPlanParser parser(local_engine::SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_GT(spark_row_info->getNumRows(), 0);
        local_engine::SparkRowToCHColumn converter;
        auto block = converter.convertSparkRowInfoToCHColumn(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
    }
}

TEST(TestSelect, TestFilter)
{
    GTEST_SKIP();
    dbms::SerializedSchemaBuilder schema_builder;
    // sorted by key
    auto * schema = schema_builder.column("sepal_length", "FP64")
                        .column("sepal_width", "FP64")
                        .column("petal_length", "FP64")
                        .column("petal_width", "FP64")
                        .column("type", "I64")
                        .column("type_string", "String")
                        .build();
    dbms::SerializedPlanBuilder plan_builder;
    // sepal_length * 0.8
    auto * mul_exp = dbms::scalarFunction(dbms::MULTIPLY, {dbms::selection(2), dbms::literal(0.8)});
    // sepal_length * 0.8 < 4.0
    auto * less_exp = dbms::scalarFunction(dbms::LESS_THAN, {mul_exp, dbms::literal(4.0)});
    // type_string = '类型1'
    auto * type_0 = dbms::scalarFunction(dbms::EQUAL_TO, {dbms::selection(5), dbms::literal("类型1")});

    auto * filter = dbms::scalarFunction(dbms::AND, {less_exp, type_0});
    auto plan = plan_builder.registerSupportedFunctions().filter(filter).read(TEST_DATA(/ data / iris.parquet), std::move(schema)).build();
    ASSERT_EQ(plan->relations_size(), 1);
    local_engine::LocalExecutor local_executor;
    local_engine::SerializedPlanParser parser(SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_EQ(spark_row_info->getNumRows(), 1);
        local_engine::SparkRowToCHColumn converter;
        auto block = converter.convertSparkRowInfoToCHColumn(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
    }
}

TEST(TestSelect, TestAgg)
{
    GTEST_SKIP();
    dbms::SerializedSchemaBuilder schema_builder;
    // sorted by key
    auto * schema = schema_builder.column("sepal_length", "FP64")
                        .column("sepal_width", "FP64")
                        .column("petal_length", "FP64")
                        .column("petal_width", "FP64")
                        .column("type", "I64")
                        .column("type_string", "String")
                        .build();
    dbms::SerializedPlanBuilder plan_builder;
    auto * mul_exp = dbms::scalarFunction(dbms::MULTIPLY, {dbms::selection(2), dbms::literal(0.8)});
    auto * less_exp = dbms::scalarFunction(dbms::LESS_THAN, {mul_exp, dbms::literal(4.0)});
    auto * measure = dbms::measureFunction(dbms::SUM, {dbms::selection(2)});
    auto plan = plan_builder.registerSupportedFunctions()
                    .aggregate({}, {measure})
                    .filter(less_exp)
                    .read(TEST_DATA(/ data / iris.parquet), std::move(schema))
                    .build();
    ASSERT_EQ(plan->relations_size(), 1);
    local_engine::LocalExecutor local_executor;
    local_engine::SerializedPlanParser parser(SerializedPlanParser::global_context);
    auto query_plan = parser.parse(std::move(plan));
    local_executor.execute(std::move(query_plan));
    ASSERT_TRUE(local_executor.hasNext());
    while (local_executor.hasNext())
    {
        local_engine::SparkRowInfoPtr spark_row_info = local_executor.next();
        ASSERT_EQ(spark_row_info->getNumRows(), 1);
        ASSERT_EQ(spark_row_info->getNumCols(), 1);
        local_engine::SparkRowToCHColumn converter;
        auto block = converter.convertSparkRowInfoToCHColumn(*spark_row_info, local_executor.getHeader());
        ASSERT_EQ(spark_row_info->getNumRows(), block->rows());
        auto reader = SparkRowReader(block->getDataTypes());
        reader.pointTo(spark_row_info->getBufferAddress() + spark_row_info->getOffsets()[1], spark_row_info->getLengths()[0]);
        ASSERT_EQ(reader.getDouble(0), 103.2);
    }
}

TEST(TestSelect, MergeTreeWriteTest)
{
    GTEST_SKIP();
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    auto shared_context = Context::createShared();
    auto global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setPath("/home/kyligence/Documents/clickhouse_conf/data/");
    global_context->getDisksMap().emplace();
    auto int64_type = std::make_shared<DB::DataTypeInt64>();
    auto int32_type = std::make_shared<DB::DataTypeInt32>();
    auto double_type = std::make_shared<DB::DataTypeFloat64>();
    columns_description.add(ColumnDescription("l_orderkey", int64_type));
    columns_description.add(ColumnDescription("l_partkey", int64_type));
    columns_description.add(ColumnDescription("l_suppkey", int64_type));
    columns_description.add(ColumnDescription("l_linenumber", int32_type));
    columns_description.add(ColumnDescription("l_quantity", double_type));
    columns_description.add(ColumnDescription("l_extendedprice", double_type));
    columns_description.add(ColumnDescription("l_discount", double_type));
    columns_description.add(ColumnDescription("l_tax", double_type));
    columns_description.add(ColumnDescription("l_shipdate_new", double_type));
    columns_description.add(ColumnDescription("l_commitdate_new", double_type));
    columns_description.add(ColumnDescription("l_receiptdate_new", double_type));
    metadata->setColumns(columns_description);
    metadata->partition_key.expression_list_ast = std::make_shared<ASTExpressionList>();
    metadata->sorting_key = KeyDescription::getSortingKeyFromAST(makeASTFunction("tuple"), columns_description, global_context, {});
    metadata->primary_key.expression = std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>());
    auto param = DB::MergeTreeData::MergingParams();
    auto settings = std::make_unique<DB::MergeTreeSettings>();
    settings->set("min_bytes_for_wide_part", Field(0));
    settings->set("min_rows_for_wide_part", Field(0));

    local_engine::CustomStorageMergeTree custom_merge_tree(
        DB::StorageID("default", "test"), "test-intel/", *metadata, false, global_context, "", param, std::move(settings));

    auto sink = std::make_shared<local_engine::CustomMergeTreeSink>(custom_merge_tree, metadata, global_context);

    substrait::ReadRel::LocalFiles files;
    substrait::ReadRel::LocalFiles::FileOrFiles * file = files.add_items();
    std::string file_path = "file:///home/kyligence/Documents/test-dataset/intel-gazelle-test-150.snappy.parquet";
    file->set_uri_file(file_path);
    substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
    file->mutable_parquet()->CopyFrom(parquet_format);
    auto source = std::make_shared<SubstraitFileSource>(SerializedPlanParser::global_context, metadata->getSampleBlock(), files);

    QueryPipelineBuilder query_pipeline;
    query_pipeline.init(Pipe(source));
    query_pipeline.setSinks(
        [&](const Block &, Pipe::StreamType type) -> ProcessorPtr
        {
            if (type != Pipe::StreamType::Main)
                return nullptr;

            return std::make_shared<local_engine::CustomMergeTreeSink>(custom_merge_tree, metadata, global_context);
        });
    auto executor = query_pipeline.execute();
    executor->execute(1, false);
}

TEST(TESTUtil, TestByteToLong)
{
    Int64 expected = 0xf085460ccf7f0000l;
    char * arr = new char[8];
    arr[0] = -16;
    arr[1] = -123;
    arr[2] = 70;
    arr[3] = 12;
    arr[4] = -49;
    arr[5] = 127;
    arr[6] = 0;
    arr[7] = 0;
    std::reverse(arr, arr + 8);
    Int64 result = reinterpret_cast<Int64 *>(arr)[0];
    std::cout << std::to_string(result);

    ASSERT_EQ(expected, result);
}


TEST(TestSimpleAgg, TestGenerate)
{
    GTEST_SKIP();
    //    dbms::SerializedSchemaBuilder schema_builder;
    //    auto * schema = schema_builder.column("l_orderkey", "I64")
    //                        .column("l_partkey", "I64")
    //                        .column("l_suppkey", "I64")
    //                        .build();
    //    dbms::SerializedPlanBuilder plan_builder;
    //    auto * measure = dbms::measureFunction(dbms::SUM, {dbms::selection(6)});
    //    auto plan
    //        = plan_builder.registerSupportedFunctions()
    //              .aggregate({}, {measure})
    //              .read(
    //                  //"/home/kyligence/Documents/test-dataset/intel-gazelle-test-" + std::to_string(state.range(0)) + ".snappy.parquet",
    //                  "/data0/tpch100_zhichao/parquet_origin/lineitem/part-00087-066b93b4-39e1-4d46-83ab-d7752096b599-c000.snappy.parquet",
    //                  std::move(schema))
    //              .build();
    local_engine::SerializedPlanParser parser(local_engine::SerializedPlanParser::global_context);
    ////    auto query_plan = parser.parse(std::move(plan));

    //std::ifstream t("/home/hongbin/develop/experiments/221011_substrait_agg_on_empty_table.json");
    //std::ifstream t("/home/hongbin/develop/experiments/221101_substrait_agg_on_simple_table_last_phrase.json");
    std::ifstream t("/home/hongbin/develop/experiments/221102_substrait_agg_and_countdistinct_second_phrase.json");
    std::string str((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
    auto query_plan = parser.parseJson(str);
    local_engine::LocalExecutor local_executor;
    local_executor.execute(std::move(query_plan));
    while (local_executor.hasNext())
    {
        auto * block = local_executor.nextColumnar();
        debug::headBlock(*block);
    }
}

TEST(ReadBufferFromFile, seekBackwards)
{
    static constexpr size_t N = 256;
    static constexpr size_t BUF_SIZE = 64;

    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path());
        for (size_t i = 0; i < N; ++i)
            writeIntBinary(i, out);
    }

    ReadBufferFromFile in(tmp_file->path(), BUF_SIZE);
    size_t x;

    /// Read something to initialize the buffer.
    in.seek(BUF_SIZE * 10, SEEK_SET);
    readIntBinary(x, in);

    /// Check 2 consecutive  seek calls without reading.
    in.seek(BUF_SIZE * 2, SEEK_SET);
    //    readIntBinary(x, in);
    in.seek(BUF_SIZE, SEEK_SET);

    readIntBinary(x, in);
    ASSERT_EQ(x, 8);
}

int main(int argc, char ** argv)
{
    auto * init = new String("{\"advancedExtensions\":{\"enhancement\":{\"@type\":\"type.googleapis.com/substrait.Expression\",\"literal\":{\"map\":{\"keyValues\":[{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level\"},\"value\":{\"string\":\"error\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_settings.max_bytes_before_external_sort\"},\"value\":{\"string\":\"5368709120\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.endpoint\"},\"value\":{\"string\":\"localhost:9000\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.velox.IOThreads\"},\"value\":{\"string\":\"0\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.input_read_timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_settings.query_plan_enable_optimizations\"},\"value\":{\"string\":\"false\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.worker.id\"},\"value\":{\"string\":\"1\"}},{\"key\":{\"string\":\"spark.memory.offHeap.enabled\"},\"value\":{\"string\":\"true\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.iam.role.session.name\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.input_connect_timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.shuffle.codec\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.local_engine.settings.log_processors_profiles\"},\"value\":{\"string\":\"true\"}},{\"key\":{\"string\":\"spark.gluten.memory.offHeap.size.in.bytes\"},\"value\":{\"string\":\"10737418240\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.shuffle.codecBackend\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.sql.orc.compression.codec\"},\"value\":{\"string\":\"snappy\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_settings.max_bytes_before_external_group_by\"},\"value\":{\"string\":\"5368709120\"}},{\"key\":{\"string\":\"spark.hadoop.input.write.timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.secret.key\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.access.key\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.dfs_client_log_severity\"},\"value\":{\"string\":\"INFO\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.path.style.access\"},\"value\":{\"string\":\"true\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.timezone\"},\"value\":{\"string\":\"Asia/Shanghai\"}},{\"key\":{\"string\":\"spark.hadoop.input.read.timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.use.instance.credentials\"},\"value\":{\"string\":\"false\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_settings.output_format_orc_compression_method\"},\"value\":{\"string\":\"snappy\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.iam.role\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.gluten.memory.task.offHeap.size.in.bytes\"},\"value\":{\"string\":\"10737418240\"}},{\"key\":{\"string\":\"spark.hadoop.input.connect.timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.hadoop.dfs.client.log.severity\"},\"value\":{\"string\":\"INFO\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver\"},\"value\":{\"string\":\"2\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.input_write_timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.connection.ssl.enabled\"},\"value\":{\"string\":\"false\"}}]}}}}}");

    BackendInitializerUtil::init_json(std::move(init));
    SCOPE_EXIT({ BackendFinalizerUtil::finalizeGlobally(); });

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
