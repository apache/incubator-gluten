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
#include "StorageJoinFromReadBuffer.h"

#include <Formats/NativeReader.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Common/Exception.h>

namespace DB
{
class HashJoin;
using HashJoinPtr = std::shared_ptr<HashJoin>;
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int DEADLOCK_AVOIDED;
}
}

using namespace DB;

void restore(DB::ReadBuffer & in, IJoin & join, const Block & sample_block)
{
    NativeReader block_stream(in, 0);

    ProfileInfo info;
    {
        while (Block block = block_stream.read())
        {
            auto final_block = sample_block.cloneWithColumns(block.mutateColumns());
            info.update(final_block);
            join.addBlockToJoin(final_block, true);
        }
    }
}

DB::Block rightSampleBlock(bool use_nulls, const StorageInMemoryMetadata & storage_metadata_, JoinKind kind)
{
    DB::Block block = storage_metadata_.getSampleBlock();
    if (use_nulls && isLeftOrFull(kind))
    {
        for (auto & col : block)
        {
            DB::JoinCommon::convertColumnToNullable(col);
        }
    }
    return block;
}

namespace local_engine
{

StorageJoinFromReadBuffer::StorageJoinFromReadBuffer(
    DB::ReadBuffer & in,
    const Names & key_names,
    bool use_nulls,
    std::shared_ptr<DB::TableJoin> table_join,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    const String & comment,
    const bool overwrite)
    : key_names_(key_names), use_nulls_(use_nulls)
{
    storage_metadata_.setColumns(columns);
    storage_metadata_.setConstraints(constraints);
    storage_metadata_.setComment(comment);

    for (const auto & key : key_names)
        if (!storage_metadata_.getColumns().hasPhysical(key))
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Key column ({}) does not exist in table declaration.", key);
    right_sample_block_ = rightSampleBlock(use_nulls, storage_metadata_, table_join->kind());
    join_ = std::make_shared<HashJoin>(table_join, right_sample_block_, overwrite);
    restore(in, *join_, storage_metadata_.getSampleBlock());
}

DB::JoinPtr StorageJoinFromReadBuffer::getJoinLocked(std::shared_ptr<DB::TableJoin> analyzed_join, DB::ContextPtr /*context*/) const
{
    if (!analyzed_join->sameStrictnessAndKind(join_->getTableJoin().strictness(), join_->getTableJoin().kind()))
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "Table {} has incompatible type of JOIN.", storage_metadata_.comment);

    if ((analyzed_join->forceNullableRight() && !use_nulls_)
        || (!analyzed_join->forceNullableRight() && isLeftOrFull(analyzed_join->kind()) && use_nulls_))
        throw Exception(
            ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
            "Table {} needs the same join_use_nulls setting as present in LEFT or FULL JOIN",
            storage_metadata_.comment);

    /// TODO: check key columns

    /// Set names qualifiers: table.column -> column
    /// It's required because storage join stores non-qualified names
    /// Qualifies will be added by join implementation (HashJoin)
    analyzed_join->setRightKeys(key_names_);

    HashJoinPtr join_clone = std::make_shared<HashJoin>(analyzed_join, right_sample_block_);
    join_clone->reuseJoinedData(static_cast<const HashJoin &>(*join_));

    return join_clone;
}
}
