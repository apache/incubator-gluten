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
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn_fwd.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <gtest/gtest.h>
#include <Common/Logger.h>

namespace local_engine
{
class NormalFileReader;
}
namespace DB
{
class PullingPipelineExecutor;
class Block;
}

namespace local_engine::test
{


// TODO: CppToDataType move to other cpp files
template <typename T> struct CppToDataType;

template <>
struct CppToDataType<Int64>
{
    using Type = DB::DataTypeInt64;
    using ColumnType = DB::ColumnInt64;
    static auto create() { return std::make_shared<Type>(); }
};

template <>
struct CppToDataType<UInt64>
{
    using Type = DB::DataTypeUInt64;
    using ColumnType = DB::ColumnUInt64;
    static auto create() { return std::make_shared<Type>(); }
};

template <typename T >
DB::ColumnPtr makeColumn(const std::vector<T>& data)
requires (std::is_base_of_v<DB::ColumnVector<T>, typename CppToDataType<T>::ColumnType>)
{
    static_assert(!DB::is_decimal<T>);

    auto column = CppToDataType<T>::ColumnType::create(data.size());
    typename DB::ColumnVector<T>::Container & vec = column->getData();
    memcpy(vec.data(), data.data(), data.size() * sizeof(T));
    return column;
}

template <typename T>
DB::ColumnWithTypeAndName makeColumn(const std::vector<T>& data, const std::string & col_name)
requires (std::is_base_of_v<DB::ColumnVector<T>, typename CppToDataType<T>::ColumnType>)
{
    return {makeColumn(data), CppToDataType<T>::create(), col_name};
}
// end of CppToDataType

template <typename T>
concept couldbe_collected = requires(T t)
{
    { t.pull(std::declval<DB::Chunk&>()) } -> std::same_as<bool>;
    { t.getHeader() } -> std::same_as<const DB::Block&>;
};

class ReaderTestBase : public testing::Test
{
    int64_t query_id_ = 0;

protected:
    LoggerPtr test_logger = getLogger("ReaderTestBase");
    DB:: ContextMutablePtr context_ = nullptr;

    void writeToFile(const std::string & filePath, const DB::Block & block) const;

    DB::DatabasePtr createMemoryDatabaseIfNotExists(const String & database_name);
    void createMemoryTableIfNotExists(const String & database_name, const String & table_name, const std::vector<DB::Block> & blocks);

    template <typename T> requires couldbe_collected<T>
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