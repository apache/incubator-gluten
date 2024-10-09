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
#include <DataTypes/DataTypeNullable.h>
#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Squashing.h>
#include <Parser/LocalExecutor.h>
#include <Parser/RelParsers/WriteRelParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parser/TypeParser.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/SparkMergeTreeWriteSettings.h>
#include <Storages/MergeTree/SparkMergeTreeWriter.h>
#include <Storages/ObjectStorage/HDFS/Configuration.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Storages/Output/FileWriterWrappers.h>
#include <google/protobuf/wrappers.pb.h>
#include <gtest/gtest.h>
#include <substrait/plan.pb.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>

namespace DB::Setting
{
extern const SettingsUInt64 max_parser_depth;
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsBool allow_settings_after_format_in_insert;
extern const SettingsUInt64 max_query_size;
extern const SettingsUInt64 min_insert_block_size_rows;
extern const SettingsUInt64 min_insert_block_size_bytes;
}

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
    auto settings = QueryContext::globalContext()->getSettingsRef();
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
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks],
        true);
    auto & create = ast->as<ASTCreateQuery &>();
    auto arg = create.storage->children[0];
    const auto * func = arg->as<const ASTFunction>();
    EXPECT_TRUE(func && func->name == "HDFS");

    DB::StorageHDFSConfiguration config;
    StorageObjectStorage::Configuration::initialize(config, arg->children[0]->children, QueryContext::globalContext(), false);

    const std::shared_ptr<DB::HDFSObjectStorage> object_storage
        = std::dynamic_pointer_cast<DB::HDFSObjectStorage>(config.createObjectStorage(QueryContext::globalContext(), false));
    EXPECT_TRUE(object_storage != nullptr);

    RelativePathsWithMetadata files_with_metadata;
    object_storage->listObjects("/clickhouse", files_with_metadata, 0);

    /// 1. Create ObjectStorageSink
    DB::StorageObjectStorageSink sink{
        object_storage, config.clone(), {}, {{STRING(), "name"}, {UINT(), "value"}}, QueryContext::globalContext(), ""};

    /// 2. Create Chunk
    auto chunk = testChunk();
    /// 3. comsume
    sink.consume(chunk);
    sink.onFinish();
}


INCBIN(native_write, SOURCE_DIR "/utils/extern-local-engine/tests/json/native_write_plan.json");
TEST(WritePipeline, SubstraitFileSink)
{
    const auto context = DB::Context::createCopy(QueryContext::globalContext());
    GlutenWriteSettings settings{
        .task_write_tmp_dir = "file:///tmp/test_table/test",
        .task_write_filename = "data.parquet",
    };
    settings.set(context);

    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"1399183","text":{"fieldDelimiter":"|","maxBlockSize":"8192"},"schema":{"names":["s_suppkey","s_name","s_address","s_nationkey","s_phone","s_acctbal","s_comment"],"struct":{"types":[{"i64":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i64":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":15,"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}}]}},"metadataColumns":[{}]}]})";
    constexpr std::string_view file{GLUTEN_SOURCE_DIR("/backends-clickhouse/src/test/resources/csv-data/supplier.csv")};
    auto [plan, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(native_write), split_template, file, context);

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

    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x);
    EXPECT_EQ(1, x.rows());
    const auto & col_a = *(x.getColumns()[0]);
    EXPECT_EQ(settings.task_write_filename, col_a.getDataAt(0));
    const auto & col_b = *(x.getColumns()[1]);
    EXPECT_EQ(SubstraitFileSink::NO_PARTITION_ID, col_b.getDataAt(0));
    const auto & col_c = *(x.getColumns()[2]);
    EXPECT_EQ(10000, col_c.getInt(0));
}

INCBIN(native_write_one_partition, SOURCE_DIR "/utils/extern-local-engine/tests/json/native_write_one_partition.json");

TEST(WritePipeline, SubstraitPartitionedFileSink)
{
    const auto context = DB::Context::createCopy(QueryContext::globalContext());
    GlutenWriteSettings settings{
        .task_write_tmp_dir = "file:///tmp/test_table/test_partition",
        .task_write_filename = "data.parquet",
    };
    settings.set(context);

    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"1399183","text":{"fieldDelimiter":"|","maxBlockSize":"8192"},"schema":{"names":["s_suppkey","s_name","s_address","s_nationkey","s_phone","s_acctbal","s_comment"],"struct":{"types":[{"i64":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i64":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"decimal":{"scale":2,"precision":15,"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}}]}},"metadataColumns":[{}]}]})";
    constexpr std::string_view file{GLUTEN_SOURCE_DIR("/backends-clickhouse/src/test/resources/csv-data/supplier.csv")};
    auto [plan, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(native_write_one_partition), split_template, file, context);

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

    EXPECT_TRUE(local_executor->hasNext());
    const Block & x = *local_executor->nextColumnar();
    debug::headBlock(x, 25);
    EXPECT_EQ(25, x.rows());
    // const auto & col_b = *(x.getColumns()[1]);
    // EXPECT_EQ(16, col_b.getInt(0));
}

TEST(WritePipeline, ComputePartitionedExpression)
{
    const auto context = DB::Context::createCopy(QueryContext::globalContext());

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

void do_remove(const std::string & folder)
{
    namespace fs = std::filesystem;
    if (const std::filesystem::path ph(folder); fs::exists(ph))
        fs::remove_all(ph);
}

Chunk person_chunk()
{
    auto id = INT()->createColumn();
    id->insert(100);
    id->insert(200);
    id->insert(300);
    id->insert(400);
    id->insert(500);
    id->insert(600);
    id->insert(700);

    auto name = STRING()->createColumn();
    name->insert("Joe");
    name->insert("Marry");
    name->insert("Mike");
    name->insert("Fred");
    name->insert("Albert");
    name->insert("Michelle");
    name->insert("Dan");

    auto age = makeNullable(INT())->createColumn();
    Field null_field;
    age->insert(30);
    age->insert(null_field);
    age->insert(18);
    age->insert(50);
    age->insert(null_field);
    age->insert(30);
    age->insert(50);


    MutableColumns x;
    x.push_back(std::move(id));
    x.push_back(std::move(name));
    x.push_back(std::move(age));
    return {std::move(x), 7};
}

TEST(WritePipeline, MergeTree)
{
    ThreadStatus thread_status;

    const auto context = DB::Context::createCopy(QueryContext::globalContext());
    context->setPath("./");
    const Settings & settings = context->getSettingsRef();

    const std::string query
        = R"(create table if not exists person (id Int32, Name String, Age Nullable(Int32)) engine = MergeTree() ORDER BY id)";

    const char * begin = query.data();
    const char * end = query.data() + query.size();
    ParserQuery parser(end, settings[Setting::allow_settings_after_format_in_insert]);

    ASTPtr ast = parseQuery(parser, begin, end, "", settings[Setting::max_query_size], settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);

    EXPECT_TRUE(ast->as<ASTCreateQuery>());
    auto & create = ast->as<ASTCreateQuery &>();

    ColumnsDescription column_descriptions
        = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, context, LoadingStrictnessLevel::CREATE);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(column_descriptions);
    metadata.setComment("args.comment");
    ASTPtr partition_by_key;
    metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, context);

    MergeTreeData::MergingParams merging_params;
    merging_params.mode = MergeTreeData::MergingParams::Ordinary;


    /// This merging param maybe used as part of sorting key
    std::optional<String> merging_param_key_arg;
    /// Get sorting key from engine arguments.
    ///
    /// NOTE: store merging_param_key_arg as additional key column. We do it
    /// before storage creation. After that storage will just copy this
    /// column if sorting key will be changed.
    metadata.sorting_key
        = KeyDescription::getSortingKeyFromAST(create.storage->order_by->ptr(), metadata.columns, context, merging_param_key_arg);

    std::unique_ptr<MergeTreeSettings> storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());

    UUID uuid;
    UUIDHelpers::getHighBytes(uuid) = 0xffffffffffff0fffull | 0x0000000000004000ull;
    UUIDHelpers::getLowBytes(uuid) = 0x3fffffffffffffffull | 0x8000000000000000ull;

    SCOPE_EXIT({ do_remove("WritePipeline_MergeTree"); });

    auto merge_tree = std::make_shared<StorageMergeTree>(
        StorageID("", "", uuid),
        "WritePipeline_MergeTree",
        metadata,
        LoadingStrictnessLevel::CREATE,
        context,
        "",
        merging_params,
        std::move(storage_settings));

    Block header{{INT(), "id"}, {STRING(), "Name"}, {makeNullable(INT()), "Age"}};
    DB::Squashing squashing(header, settings[Setting::min_insert_block_size_rows], settings[Setting::min_insert_block_size_bytes]);
    squashing.add(person_chunk());
    auto x = Squashing::squash(squashing.flush());
    x.getChunkInfos().add(std::make_shared<DeduplicationToken::TokenInfo>());

    ASSERT_EQ(7, x.getNumRows());
    ASSERT_EQ(3, x.getNumColumns());


    auto metadata_snapshot = std::make_shared<const StorageInMemoryMetadata>(metadata);
    ASTPtr none;
    auto sink = std::static_pointer_cast<MergeTreeSink>(merge_tree->write(none, metadata_snapshot, context, false));

    sink->consume(x);
    sink->onFinish();
}

INCBIN(_1_mergetree_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/1_mergetree.json");
INCBIN(_1_mergetree_hdfs_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/1_mergetree_hdfs.json");
INCBIN(_1_read_, SOURCE_DIR "/utils/extern-local-engine/tests/json/mergetree/1_plan.json");

TEST(WritePipeline, SparkMergeTree)
{
    ThreadStatus thread_status;

    const auto context = DB::Context::createCopy(QueryContext::globalContext());
    context->setPath("./");
    const Settings & settings = context->getSettingsRef();

    const auto extension_table = local_engine::JsonStringToMessage<substrait::ReadRel::ExtensionTable>(EMBEDDED_PLAN(_1_mergetree_));
    MergeTreeTableInstance merge_tree_table(extension_table);

    EXPECT_EQ(merge_tree_table.database, "default");
    EXPECT_EQ(merge_tree_table.table, "lineitem_mergetree");
    EXPECT_EQ(merge_tree_table.relative_path, "lineitem_mergetree");
    EXPECT_EQ(merge_tree_table.table_configs.storage_policy, "default");

    do_remove(merge_tree_table.relative_path);

    const auto dest_storage = merge_tree_table.getStorage(QueryContext::globalMutableContext());
    EXPECT_TRUE(dest_storage);
    EXPECT_FALSE(dest_storage->getStoragePolicy()->getAnyDisk()->isRemote());
    DB::StorageMetadataPtr metadata_snapshot = dest_storage->getInMemoryMetadataPtr();
    Block header = metadata_snapshot->getSampleBlock();

    constexpr std::string_view split_template
        = R"({"items":[{"uriFile":"{replace_local_files}","length":"19230111","parquet":{},"schema":{},"metadataColumns":[{}],"properties":{"fileSize":"19230111","modificationTime":"1722330598029"}}]})";
    constexpr std::string_view file{GLUTEN_SOURCE_TPCH_DIR("lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet")};

    SparkMergeTreeWritePartitionSettings gm_write_settings{
        .part_name_prefix{"this_is_prefix"},
    };
    gm_write_settings.set(context);

    auto writer = local_engine::SparkMergeTreeWriter::create(merge_tree_table, gm_write_settings, context);
    SparkMergeTreeWriter & spark_merge_tree_writer = *writer;

    auto [_, local_executor] = test::create_plan_and_executor(EMBEDDED_PLAN(_1_read_), split_template, file);
    EXPECT_TRUE(local_executor->hasNext());

    do
    {
        spark_merge_tree_writer.write(*local_executor->nextColumnar());
    } while (local_executor->hasNext());

    spark_merge_tree_writer.finalize();
    auto part_infos = spark_merge_tree_writer.getAllPartInfo();
    auto json_info = local_engine::SparkMergeTreeWriter::partInfosToJson(part_infos);
    std::cerr << json_info << std::endl;

    ///
    {
        const auto extension_table_hdfs
            = local_engine::JsonStringToMessage<substrait::ReadRel::ExtensionTable>(EMBEDDED_PLAN(_1_mergetree_hdfs_));
        MergeTreeTableInstance merge_tree_table_hdfs(extension_table_hdfs);
        EXPECT_EQ(merge_tree_table_hdfs.database, "default");
        EXPECT_EQ(merge_tree_table_hdfs.table, "lineitem_mergetree_hdfs");
        EXPECT_EQ(merge_tree_table_hdfs.relative_path, "3.5/test/lineitem_mergetree_hdfs");
        EXPECT_EQ(merge_tree_table_hdfs.table_configs.storage_policy, "__hdfs_main");

        const auto dest_storage_hdfs = merge_tree_table_hdfs.getStorage(QueryContext::globalMutableContext());
        EXPECT_TRUE(dest_storage_hdfs);
        EXPECT_TRUE(dest_storage_hdfs->getStoragePolicy()->getAnyDisk()->isRemote());
    }
}