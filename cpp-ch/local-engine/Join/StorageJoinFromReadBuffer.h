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
#include <Interpreters/JoinUtils.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
class TableJoin;
class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;
}

namespace local_engine
{

class StorageJoinFromReadBuffer
{
public:
    StorageJoinFromReadBuffer(
        DB::ReadBuffer & in_,
        const DB::Names & key_names_,
        bool use_nulls_,
        std::shared_ptr<DB::TableJoin> table_join_,
        const DB::ColumnsDescription & columns_,
        const DB::ConstraintsDescription & constraints_,
        const String & comment,
        bool overwrite_);

    DB::JoinPtr getJoinLocked(std::shared_ptr<DB::TableJoin> analyzed_join, DB::ContextPtr context) const;
    const DB::Block & getRightSampleBlock() const { return right_sample_block_; }

private:
    DB::StorageInMemoryMetadata storage_metadata_;
    const DB::Names key_names_;
    bool use_nulls_;
    DB::JoinPtr join_;
    DB::Block right_sample_block_;
};
}
