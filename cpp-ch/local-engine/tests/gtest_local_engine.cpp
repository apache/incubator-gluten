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
#include <incbin.h>

#include <Databases/DatabasesCommon.h>
#include <Databases/TablesLoader.h>
#include <Databases/registerDatabases.h>
#include <Disks/DiskLocal.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/registerInterpreters.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/SparkRowToCHColumn.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <gtest/gtest.h>
#include <tests/utils/gluten_test_util.h>
#include <config.pb.h>
#include <Common/CHUtil.h>
#include <Common/GlutenConfig.h>
#include <Common/PoolId.h>
#include <Common/QueryContext.h>
#include <Common/escapeForFileName.h>

using namespace local_engine;
using namespace DB;

TEST(TESTUtil, TestByteToLong)
{
    Int64 expected = 0xf085460ccf7f0000l;
    char arr[8];
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

TEST(ReadBufferFromFile, seekBackwards)
{
    static constexpr size_t N = 256;
    static constexpr size_t BUF_SIZE = 64;

    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path());
        for (size_t i = 0; i < N; ++i)
            writeIntBinary(i, out);
        out.finalize();
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

INCBIN(_config_json, SOURCE_DIR "/utils/extern-local-engine/tests/json/gtest_local_engine_config.json");

namespace DB
{
void registerOutputFormatParquet(DB::FormatFactory & factory);
}

static bool isSystemOrInformationSchema(const String & database_name)
{
    return database_name == DatabaseCatalog::SYSTEM_DATABASE || database_name == DatabaseCatalog::INFORMATION_SCHEMA
        || database_name == DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE;
}
namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
extern const SettingsMaxThreads max_threads;
}
}

static void executeCreateQuery(
    const String & query,
    ContextMutablePtr context,
    const String & database,
    const String & file_name,
    bool create,
    bool has_force_restore_data_flag)
{
    const Settings & settings = context->getSettingsRef();
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        query.data(),
        query.data() + query.size(),
        "in file " + file_name,
        0,
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    auto & ast_create_query = ast->as<ASTCreateQuery &>();
    ast_create_query.setDatabase(database);

    InterpreterCreateQuery interpreter(ast, context);
    interpreter.setInternal(true);
    if (!create)
    {
        interpreter.setForceAttach(true);
        interpreter.setForceRestoreData(has_force_restore_data_flag);
    }
    interpreter.setLoadDatabaseWithoutTables(true);
    interpreter.execute();
}

int main(int argc, char ** argv)
{
    SparkConfigs::update(
        local_engine::JsonStringToBinary<gluten::ConfigMap>(EMBEDDED_PLAN(_config_json)),
        [&](const SparkConfigs::ConfigMap & spark_conf_map) { BackendInitializerUtil::initBackend(spark_conf_map); },
        true);

    ContextMutablePtr context = QueryContext::globalMutableContext();
    // context->setSetting("background_schedule_pool_size", 1);
    // context->setSetting("tables_loader_foreground_pool_size", 1);
    // context->setSetting("tables_loader_background_pool_size", 1);
    // context->setSetting("background_pool_size", 1);
    // context->setSetting("background_move_pool_size", 1);
    // context->setSetting("background_fetches_pool_size", 1);
    // context->setSetting("background_common_pool_size", 1);


    SCOPE_EXIT({ BackendFinalizerUtil::finalizeGlobally(); });

    DB::registerInterpreters();
    DB::registerTableFunctions();
    DB::registerDatabases();
    DB::registerStorages();
    // DB::registerDisks(/* global_skip_access_check= */ true);

    /// Loop over databases.
    std::map<String, String> databases;

    /// Some databases don't have an .sql metadata file.
    std::map<String, String> orphan_directories_and_symlinks;

    auto db_disk = Context::getGlobalContextInstance()->getDatabaseDisk();
    auto metadata_dir_path = fs::path("metadata");
    for (const auto it = db_disk->iterateDirectory(metadata_dir_path); it->isValid(); it->next())
    {
        auto sub_path = fs::path(it->path());
        if (sub_path.filename().empty())
            sub_path = sub_path.parent_path();

        if (db_disk->isSymlinkSupported() && db_disk->isSymlink(sub_path))
        {
            String db_name = sub_path.filename().string();
            if (!isSystemOrInformationSchema(db_name))
                orphan_directories_and_symlinks.emplace(unescapeForFileName(db_name), sub_path);
            continue;
        }
        if (db_disk->existsDirectory(sub_path))
            continue;

        const auto current_file = sub_path.filename().string();

        if (fs::path(current_file).extension() == ".sql")
        {
            String db_name = fs::path(current_file).stem();
            orphan_directories_and_symlinks.erase(db_name);
            if (!isSystemOrInformationSchema(db_name))
                databases.emplace(unescapeForFileName(db_name), metadata_dir_path / db_name);
        }
    }
    TablesLoader::Databases loaded_databases;


    for (const auto & [name, database_path] : databases)
    {
        if (name != "tpch100")
            continue;
        String database_attach_query;
        String database_metadata_file = database_path + ".sql";
        assert(db_disk->existsFile(fs::path(database_metadata_file)));
        database_attach_query = readMetadataFile(db_disk, database_metadata_file);
        executeCreateQuery(database_attach_query, context, name, database_metadata_file, true, false);
        loaded_databases.insert({name, DatabaseCatalog::instance().getDatabase(name)});
    }

    assert(loaded_databases.size() == 1);
    assert(loaded_databases.begin()->first == "tpch100");
    assert(loaded_databases.begin()->second);

    auto mode = getLoadingStrictnessLevel(/* attach */ true, /* force_attach */ true, false, /* secondary */ false);
    TablesLoader loader{context, std::move(loaded_databases), mode};
    auto load_tasks = loader.loadTablesAsync();
    waitLoad(TablesLoaderForegroundPoolId, load_tasks);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}