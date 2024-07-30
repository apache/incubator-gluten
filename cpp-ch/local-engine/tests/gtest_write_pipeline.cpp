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

#include <gluten_test_util.h>
#include <incbin.h>
#include <testConfig.h>
#include <Core/Settings.h>
#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parser/TypeParser.h>
#include <Parser/WriteRelParser.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/Output/FileWriterWrappers.h>
#include <gtest/gtest.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>


using namespace local_engine;
using namespace DB;

Chunk testChunk()
{
    auto nameCol = STRING()->createColumn();
    nameCol->insert("one");
    nameCol->insert("two");
    nameCol->insert("three");

    auto valueCol = UINT()->createColumn();
    valueCol->insert(1);
    valueCol->insert(2);
    valueCol->insert(3);
    MutableColumns x;
    x.push_back(std::move(nameCol));
    x.push_back(std::move(valueCol));
    return {std::move(x), 3};
}

TEST(LocalExecutor, StorageObjectStorageSink)
{
    /// 0. Create ObjectStorage for HDFS
    auto settings = SerializedPlanParser::global_context->getSettingsRef();
    const std::string query
        = R"(CREATE TABLE hdfs_engine_xxxx (name String, value UInt32) ENGINE=HDFS('hdfs://localhost:8020/clickhouse/test2', 'Parquet'))";
    DB::ParserCreateQuery parser;
    std::string error_message;
    const char * pos = query.data();
    auto ast = DB::tryParseQuery(
        parser,
        pos,
        pos + query.size(),
        error_message,
        /* hilite = */ false,
        "QUERY TEST",
        /* allow_multi_statements = */ false,
        0,
        settings.max_parser_depth,
        settings.max_parser_backtracks,
        true);
    auto & create = ast->as<ASTCreateQuery &>();
    auto arg = create.storage->children[0];
    const auto * func = arg->as<const ASTFunction>();
    EXPECT_TRUE(func && func->name == "HDFS");

    DB::StorageHDFSConfiguration config;
    StorageObjectStorage::Configuration::initialize(config, arg->children[0]->children, SerializedPlanParser::global_context, false);

    const std::shared_ptr<DB::HDFSObjectStorage> object_storage
        = std::dynamic_pointer_cast<DB::HDFSObjectStorage>(config.createObjectStorage(SerializedPlanParser::global_context, false));
    EXPECT_TRUE(object_storage != nullptr);

    RelativePathsWithMetadata files_with_metadata;
    object_storage->listObjects("/clickhouse", files_with_metadata, 0);

    /// 1. Create ObjectStorageSink
    DB::StorageObjectStorageSink sink{
        object_storage, config.clone(), {}, {{STRING(), "name"}, {UINT(), "value"}}, SerializedPlanParser::global_context, ""};

    /// 2. Create Chunk
    auto chunk = testChunk();
    /// 3. comsume
    sink.consume(chunk);
    sink.onFinish();
}


INCBIN(resource_embedded_write_json, SOURCE_DIR "/utils/extern-local-engine/tests/json/native_write_plan.json");
TEST(WritePipeline, SubstraitFileSink)
{
    const auto tmpdir = std::string{"file:///tmp/test_table/test"};
    const auto filename = std::string{"data.parquet"};
    const std::string split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"1399183","text":{"fieldDelimiter":"|","maxBlockSize":"8192"},"schema":{"names":["s_suppkey","s_name","s_address","s_nationkey","s_phone","s_acctbal","s_comment"],"struct":{"types":[{"i64":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i64":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":15,"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}}]}},"metadataColumns":[{}]}]})";
    const std::string split
        = replaceLocalFilesWildcards(split_template, GLUTEN_SOURCE_DIR("/backends-clickhouse/src/test/resources/csv-data/supplier.csv"));

    const auto context = DB::Context::createCopy(SerializedPlanParser::global_context);
    context->setSetting(local_engine::SPARK_TASK_WRITE_TMEP_DIR, tmpdir);
    context->setSetting(local_engine::SPARK_TASK_WRITE_FILENAME, filename);
    SerializedPlanParser parser(context);
    parser.addSplitInfo(local_engine::JsonStringToBinary<substrait::ReadRel::LocalFiles>(split));

    const auto plan = local_engine::JsonStringToMessage<substrait::Plan>(
        {reinterpret_cast<const char *>(gresource_embedded_write_jsonData), gresource_embedded_write_jsonSize});

    EXPECT_EQ(1, plan.relations_size());
    const substrait::PlanRel & root_rel = plan.relations().at(0);
    EXPECT_TRUE(root_rel.has_root());
    EXPECT_TRUE(root_rel.root().input().has_write());

    const substrait::WriteRel & write_rel = root_rel.root().input().write();
    EXPECT_TRUE(write_rel.has_named_table());

    const substrait::NamedObjectWrite & named_table = write_rel.named_table();

    google::protobuf::StringValue optimization;
    named_table.advanced_extension().optimization().UnpackTo(&optimization);
    auto config = local_engine::parse_write_parameter(optimization.value());
    EXPECT_EQ(2, config.size());
    EXPECT_EQ("parquet", config["format"]);
    EXPECT_EQ("1", config["isSnappy"]);


    EXPECT_TRUE(write_rel.has_table_schema());
    const substrait::NamedStruct & table_schema = write_rel.table_schema();
    auto block = TypeParser::buildBlockFromNamedStruct(table_schema);
    auto names = block.getNames();
    DB::Names expected{"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment111"};
    EXPECT_EQ(expected, names);

    auto partitionCols = collect_partition_cols(block, table_schema);
    DB::Names expected_partition_cols;
    EXPECT_EQ(expected_partition_cols, partitionCols);


    auto local_executor = parser.createExecutor(plan);
    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x);
    EXPECT_EQ(1, x.rows());
    const auto & col_a = *(x.getColumns()[0]);
    EXPECT_EQ(filename, col_a.getDataAt(0));
    const auto & col_b = *(x.getColumns()[1]);
    EXPECT_EQ(SubstraitFileSink::NO_PARTITION_ID, col_b.getDataAt(0));
    const auto & col_c = *(x.getColumns()[2]);
    EXPECT_EQ(10000, col_c.getInt(0));
}

INCBIN(resource_embedded_write_one_partition_json, SOURCE_DIR "/utils/extern-local-engine/tests/json/native_write_one_partition.json");

TEST(WritePipeline, SubstraitPartitionedFileSink)
{
    const std::string split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"1399183","text":{"fieldDelimiter":"|","maxBlockSize":"8192"},"schema":{"names":["s_suppkey","s_name","s_address","s_nationkey","s_phone","s_acctbal","s_comment"],"struct":{"types":[{"i64":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i64":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":15,"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}}]}},"metadataColumns":[{}]}]})";
    const std::string split
        = replaceLocalFilesWildcards(split_template, GLUTEN_SOURCE_DIR("/backends-clickhouse/src/test/resources/csv-data/supplier.csv"));

    const auto context = DB::Context::createCopy(SerializedPlanParser::global_context);
    context->setSetting(local_engine::SPARK_TASK_WRITE_TMEP_DIR, std::string{"file:///tmp/test_table/test_partition"});
    context->setSetting(local_engine::SPARK_TASK_WRITE_FILENAME, std::string{"data.parquet"});
    SerializedPlanParser parser(context);
    parser.addSplitInfo(local_engine::JsonStringToBinary<substrait::ReadRel::LocalFiles>(split));

    const auto plan = local_engine::JsonStringToMessage<substrait::Plan>(
        {reinterpret_cast<const char *>(gresource_embedded_write_one_partition_jsonData), gresource_embedded_write_one_partition_jsonSize});

    EXPECT_EQ(1, plan.relations_size());
    const substrait::PlanRel & root_rel = plan.relations().at(0);
    EXPECT_TRUE(root_rel.has_root());
    EXPECT_TRUE(root_rel.root().input().has_write());

    const substrait::WriteRel & write_rel = root_rel.root().input().write();
    EXPECT_TRUE(write_rel.has_named_table());

    const substrait::NamedObjectWrite & named_table = write_rel.named_table();

    google::protobuf::StringValue optimization;
    named_table.advanced_extension().optimization().UnpackTo(&optimization);
    auto config = local_engine::parse_write_parameter(optimization.value());
    EXPECT_EQ(2, config.size());
    EXPECT_EQ("parquet", config["format"]);
    EXPECT_EQ("1", config["isSnappy"]);


    EXPECT_TRUE(write_rel.has_table_schema());
    const substrait::NamedStruct & table_schema = write_rel.table_schema();
    auto block = TypeParser::buildBlockFromNamedStruct(table_schema);
    auto names = block.getNames();
    DB::Names expected{"s_suppkey", "s_name", "s_address", "s_phone", "s_acctbal", "s_comment", "s_nationkey"};
    EXPECT_EQ(expected, names);

    auto partitionCols = local_engine::collect_partition_cols(block, table_schema);
    DB::Names expected_partition_cols{"s_nationkey"};
    EXPECT_EQ(expected_partition_cols, partitionCols);

    auto local_executor = parser.createExecutor(plan);
    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x, 25);
    EXPECT_EQ(25, x.rows());
    // const auto & col_b = *(x.getColumns()[1]);
    // EXPECT_EQ(16, col_b.getInt(0));
}

TEST(WritePipeline, ComputePartitionedExpression)
{
    const auto context = DB::Context::createCopy(SerializedPlanParser::global_context);

    auto partition_by = SubstraitPartitionedFileSink::make_partition_expression({"s_nationkey", "name"});

    ASTs arguments(1, partition_by);
    ASTPtr partition_by_string = makeASTFunction("toString", std::move(arguments));

    Block sample_block{{STRING(), "name"}, {UINT(), "s_nationkey"}};
    auto syntax_result = TreeRewriter(context).analyze(partition_by_string, sample_block.getNamesAndTypesList());
    auto partition_by_expr = ExpressionAnalyzer(partition_by_string, syntax_result, context).getActions(false);


    auto partition_by_column_name = partition_by_string->getColumnName();

    Chunk chunk = testChunk();
    const auto & columns = chunk.getColumns();
    Block block_with_partition_by_expr = sample_block.cloneWithoutColumns();
    block_with_partition_by_expr.setColumns(columns);
    partition_by_expr->execute(block_with_partition_by_expr);

    size_t chunk_rows = chunk.getNumRows();
    EXPECT_EQ(3, chunk_rows);

    const auto * partition_by_result_column = block_with_partition_by_expr.getByName(partition_by_column_name).column.get();
    EXPECT_EQ("s_nationkey=1/name=one", partition_by_result_column->getDataAt(0));
    EXPECT_EQ("s_nationkey=2/name=two", partition_by_result_column->getDataAt(1));
    EXPECT_EQ("s_nationkey=3/name=three", partition_by_result_column->getDataAt(2));
}