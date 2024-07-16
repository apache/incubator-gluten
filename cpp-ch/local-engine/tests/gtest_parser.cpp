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
#include <Core/Settings.h>
#include <Parser/SerializedPlanParser.h>
#include <gtest/gtest.h>


using namespace local_engine;
using namespace DB;

// Plan for https://github.com/ClickHouse/ClickHouse/pull/65234
INCBIN(resource_embedded_pr_65234_json, SOURCE_DIR "/utils/extern-local-engine/tests/json/clickhouse_pr_65234.json");

TEST(SerializedPlanParser, PR65234)
{
    const std::string split
        = R"({"items":[{"uriFile":"file:///home/chang/SourceCode/rebase_gluten/backends-clickhouse/target/scala-2.12/test-classes/tests-working-home/tpch-data/supplier/part-00000-16caa751-9774-470c-bd37-5c84c53373c8-c000.snappy.parquet","length":"84633","parquet":{},"schema":{},"metadataColumns":[{}]}]})";
    SerializedPlanParser parser(SerializedPlanParser::global_context);
    parser.addSplitInfo(test::pb_util::JsonStringToBinary<substrait::ReadRel::LocalFiles>(split));
    auto query_plan
        = parser.parseJson({reinterpret_cast<const char *>(gresource_embedded_pr_65234_jsonData), gresource_embedded_pr_65234_jsonSize});
}

#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>

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
    /// 3. comsume
    sink.consume(testChunk());
    sink.onFinish();
}

namespace DB
{
SinkToStoragePtr createFilelinkSink(
    const StorageMetadataPtr & metadata_snapshot,
    const String & table_name_for_log,
    const String & path,
    CompressionMethod compression_method,
    const std::optional<FormatSettings> & format_settings,
    const String & format_name,
    const ContextPtr & context,
    int flags);
}

INCBIN(resource_embedded_readcsv_json, SOURCE_DIR "/utils/extern-local-engine/tests/json/read_student_option_schema.csv.json");
TEST(LocalExecutor, StorageFileSink)
{
    const std::string split
        = R"({"items":[{"uriFile":"file:///home/chang/SourceCode/rebase_gluten/backends-velox/src/test/resources/datasource/csv/student_option_schema.csv","length":"56","text":{"fieldDelimiter":",","maxBlockSize":"8192","header":"1"},"schema":{"names":["id","name","language"],"struct":{"types":[{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}}]}},"metadataColumns":[{}]}]})";
    SerializedPlanParser parser(SerializedPlanParser::global_context);
    parser.addSplitInfo(test::pb_util::JsonStringToBinary<substrait::ReadRel::LocalFiles>(split));
    auto local_executor = parser.createExecutor<true>(
        {reinterpret_cast<const char *>(gresource_embedded_readcsv_jsonData), gresource_embedded_readcsv_jsonSize});

    while (local_executor->hasNext())
    {
        const Block & x = *local_executor->nextColumnar();
        EXPECT_EQ(4, x.rows());
    }

    StorageInMemoryMetadata metadata;
    metadata.setColumns(ColumnsDescription::fromNamesAndTypes({{"name", STRING()}, {"value", UINT()}}));
    StorageMetadataPtr metadata_ptr = std::make_shared<StorageInMemoryMetadata>(metadata);

    /*
    auto sink = createFilelinkSink(
        metadata_ptr,
        "test_table",
        "/tmp/test_table.parquet",
        CompressionMethod::None,
        {},
        "Parquet",
        SerializedPlanParser::global_context,
        0);

    sink->consume(testChunk());
    sink->onFinish();
    */
}