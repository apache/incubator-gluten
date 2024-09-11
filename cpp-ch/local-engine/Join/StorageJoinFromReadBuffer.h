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
#include <shared_mutex>
#include <Core/Joins.h>
#include <Interpreters/JoinUtils.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
class TableJoin;
class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;
class HashJoin;
class ReadBuffer;
}

namespace local_engine
{

class StorageJoinFromReadBuffer
{
public:
    StorageJoinFromReadBuffer(
        DB::Blocks & data,
        size_t row_count,
        const DB::Names & key_names_,
        bool use_nulls_,
        DB::JoinKind kind,
        DB::JoinStrictness strictness,
        bool has_mixed_join_condition,
        const DB::ColumnsDescription & columns_,
        const DB::ConstraintsDescription & constraints_,
        const String & comment,
        bool overwrite_,
        bool is_null_aware_anti_join_,
        bool has_null_key_values_);

    bool has_null_key_value = false;
    bool is_empty_hash_table = false;

    /// The columns' names in right_header may be different from the names in the ColumnsDescription
    /// in the constructor.
    /// This should be called once.
    DB::JoinPtr getJoinLocked(std::shared_ptr<DB::TableJoin> analyzed_join, DB::ContextPtr context);
    const DB::Block & getRightSampleBlock() const { return right_sample_block; }

private:
    DB::StorageInMemoryMetadata storage_metadata;
    DB::Names key_names;
    bool use_nulls;
    size_t row_count;
    bool overwrite;
    DB::Block right_sample_block;
    std::shared_mutex join_mutex;
    std::list<DB::Block> input_blocks;
    std::shared_ptr<DB::HashJoin> join = nullptr;
    bool is_null_aware_anti_join;

    void readAllBlocksFromInput(DB::ReadBuffer & in);
    void buildJoin(DB::Blocks & data, const DB::Block header, std::shared_ptr<DB::TableJoin> analyzed_join);
    void collectAllInputs(DB::Blocks & data, const DB::Block header);
    void buildJoinLazily(DB::Block header, std::shared_ptr<DB::TableJoin> analyzed_join);
};
}
