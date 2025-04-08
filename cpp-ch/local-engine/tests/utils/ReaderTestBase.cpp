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

#include "ReaderTestBase.h"

#include <Databases/DatabaseMemory.h>
#include <Interpreters/Squashing.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/BlockIO.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/MemorySettings.h>
#include <Storages/Output/NormalFileWriter.h>
#include <Storages/StorageMemory.h>
#include <Storages/SubstraitSource/FileReader.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <base/demangle.h>
#include <Poco/Path.h>
#include <Poco/URI.h>
#include <Common/DebugUtils.h>
#include <Common/QueryContext.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 output_format_parquet_row_group_size;
}
}
using namespace DB;

namespace local_engine::test
{

bool BaseReaders::pull(DB::Chunk & chunk)
{
    assert(readers.size() > 0);

    while (index < readers.size())
    {
        if (readers[index]->pull(chunk))
            return true;
        ++index;
    }
    return false;
}

void ReaderTestBase::writeToFile(const std::string & filePath, const DB::Block & block) const
{
    writeToFile(filePath, DB::Blocks{block});
}

void ReaderTestBase::writeToFile(
    const std::string & filePath,
    const std::vector<DB::Block> & blocks,
    bool rowGroupPerBlock) const
{

    const auto & settings = context_->getSettingsRef();
    auto row_group_rows = settings[Setting::output_format_parquet_row_group_size];

    if (rowGroupPerBlock)
    {
        /// we can't set FormatSettings per block, set it minimum value of all blocks
        const auto min_block_it = std::ranges::min_element(blocks,
            [](const DB::Block& a, const DB::Block& b) {
                return a.rows() < b.rows();
        });

        context_->setSetting("output_format_parquet_row_group_size", Field(min_block_it->rows()));
    }

    SCOPE_EXIT({
        if (rowGroupPerBlock)
            context_->setSetting("output_format_parquet_row_group_size", Field(row_group_rows));
    });

    assert(!blocks.empty());
    const Poco::Path file{filePath};
    const Poco::URI fileUri{file};
    const auto writer = NormalFileWriter::create(context_, fileUri.toString(), blocks[0], file.getExtension());
    for (const auto & block : blocks)
        writer->write(block);
    writer->close();
}

DatabasePtr ReaderTestBase::createMemoryDatabaseIfNotExists(const String & database_name)
{
    DB::DatabasePtr system_database = DB::DatabaseCatalog::instance().tryGetDatabase(database_name);
    if (!system_database)
    {
        system_database = std::make_shared<DB::DatabaseMemory>(database_name, context_);
        DB::DatabaseCatalog::instance().attachDatabase(database_name, system_database);
    }
    return system_database;
}

void ReaderTestBase::createMemoryTableIfNotExists(
    const String & database_name, const String & table_name, const std::vector<DB::Block> & blocks)
{
    EXPECT_FALSE(blocks.empty()) << "Blocks should not be empty";

    runClickhouseSQL(fmt::format("DROP TABLE IF EXISTS {}.{}", database_name, table_name));

    StorageID table_id(database_name, table_name);
    ColumnsDescription columns_description{blocks[0].getNamesAndTypesList()};
    ConstraintsDescription constraints;
    MemorySettings memory_settings;

    auto storage_memory
        = std::make_shared<DB::StorageMemory>(table_id, columns_description, constraints, "My in-memory table", memory_settings);
    auto metadata_snapshot = storage_memory->getInMemoryMetadataPtr();
    DB::SinkToStoragePtr sink = storage_memory->write(nullptr, metadata_snapshot, context_, false);
    auto pipeline = std::make_unique<DB::QueryPipeline>(std::move(sink));
    auto writer = std::make_unique<DB::PushingPipelineExecutor>(*pipeline);
    for (auto & block : blocks)
        writer->push(block);
    writer->finish();

    auto database = createMemoryDatabaseIfNotExists(database_name);
    database->attachTable(context_, table_name, storage_memory, {});
}

void ReaderTestBase::SetUp()
{
    EXPECT_EQ(query_id_, 0) << "query_id_ should be 0 at the beginning of the test";
    EXPECT_EQ(context_, nullptr) << "context_ should be null at the beginning of the test";
    query_id_ = QueryContext::instance().initializeQuery(demangle(typeid(*this).name()));
    context_ = QueryContext::instance().currentQueryContext();
}

void ReaderTestBase::TearDown()
{
    EXPECT_NE(query_id_, 0) << "query_id_ should not be 0 at the end of the test";
    EXPECT_NE(context_, nullptr) << "context_ should not be null at the end of the test";
    QueryContext::instance().finalizeQuery(query_id_);
    query_id_ = 0;
    context_ = nullptr;
}

template <typename T>
requires couldbe_collected<T>
Block ReaderTestBase::collectResult(T & input) const
{
    const Block & header = input.getHeader();
    Squashing squashing(header, std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max());

    Chunk chunk;
    while (input.pull(chunk))
    {
        auto result = squashing.add(std::move(chunk));
        EXPECT_TRUE(!result.hasRows());
    }
    chunk = Squashing::squash(squashing.flush());
    return chunk.hasRows() ? header.cloneWithColumns(chunk.detachColumns()) : header.cloneEmpty();
}

template Block ReaderTestBase::collectResult<PullingPipelineExecutor>(PullingPipelineExecutor & input) const;
template Block ReaderTestBase::collectResult<BaseReader>(BaseReader & input) const;
template Block ReaderTestBase::collectResult<BaseReaders>(BaseReaders & input) const;

Block ReaderTestBase::runClickhouseSQL(const std::string & query) const
{
    BlockIO io = executeQuery(query, context_).second;

    if (io.pipeline.pulling())
    {
        auto executor = std::make_unique<DB::PullingPipelineExecutor>(io.pipeline);
        return collectResult(*executor);
    }

    if (io.pipeline.pushing() || io.pipeline.completed())
    {
        EXPECT_TRUE(false) << " Not Implemented";
        return {};
    }

    return {};
}

void ReaderTestBase::headBlock(const DB::Block & block, size_t count, size_t truncate) const
{
    LOG_INFO(test_logger, "\n{}", debug::showString(block, count, truncate));
}

void ReaderTestBase::headColumn(const DB::ColumnPtr & column, size_t count, size_t truncate) const
{
    LOG_INFO(test_logger, "\n{}", debug::showString(column, count, truncate));
}

}
