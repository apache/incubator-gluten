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

#pragma once

#include <concepts>
#include <Columns/IColumn_fwd.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/SubstraitSource/FileReader.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <gtest/gtest.h>
#include <Common/Logger.h>


namespace DB
{
class PullingPipelineExecutor;
class Block;
}

namespace local_engine::test
{

template <typename T>
concept couldbe_collected = requires(T t) {
    { t.pull(std::declval<DB::Chunk &>()) } -> std::same_as<bool>;
    { t.getHeader() } -> std::same_as<const DB::Block &>;
};


struct BaseReaders
{
    std::vector<std::unique_ptr<BaseReader>> & readers;

    int index = 0;

    bool pull(DB::Chunk & chunk);
    const DB::Block & getHeader() const { return readers.front()->getHeader(); }
};

class ReaderTestBase : public testing::Test
{
    int64_t query_id_ = 0;

protected:
    LoggerPtr test_logger = getLogger("ReaderTestBase");
    DB::ContextMutablePtr context_ = nullptr;

    void writeToFile(const std::string & filePath, const DB::Block & block) const;
    void writeToFile(const std::string & filePath, const std::vector<DB::Block> & blocks, bool rowGroupPerBlock = false) const;

    DB::DatabasePtr createMemoryDatabaseIfNotExists(const String & database_name);
    void createMemoryTableIfNotExists(const String & database_name, const String & table_name, const std::vector<DB::Block> & blocks);

    template <typename T>
    requires couldbe_collected<T>
    DB::Block collectResult(T & input) const;

public:
    ReaderTestBase() = default;

    void SetUp() override;
    void TearDown() override;

    DB::Block runClickhouseSQL(const std::string & query) const;

    // debug aid
    void headBlock(const DB::Block & block, size_t count = 10, size_t truncate = 20) const;
    void headColumn(const DB::ColumnPtr & column, size_t count, size_t truncate = 20) const;
};


}