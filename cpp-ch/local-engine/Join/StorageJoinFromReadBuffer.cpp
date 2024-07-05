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

#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Storages/IO/NativeReader.h>
#include <Common/Exception.h>

#include <Common/logger_useful.h>

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

constexpr auto RIHGT_COLUMN_PREFIX = "broadcast_right_";

DB::Block rightSampleBlock(bool use_nulls, const StorageInMemoryMetadata & storage_metadata_, JoinKind kind)
{
    DB::ColumnsWithTypeAndName new_cols;
    DB::Block block = storage_metadata_.getSampleBlock();
    for (const auto & col : block)
    {
        // Add a prefix to avoid column name conflicts with left table.
        new_cols.emplace_back(col.column, col.type, RIHGT_COLUMN_PREFIX + col.name);
        if (use_nulls && isLeftOrFull(kind))
        {
            auto & new_col = new_cols.back();
            DB::JoinCommon::convertColumnToNullable(new_col);
        }
    }
    return DB::Block(new_cols);
}

namespace local_engine
{

StorageJoinFromReadBuffer::StorageJoinFromReadBuffer(
    DB::ReadBuffer & in,
    size_t row_count_,
    const Names & key_names_,
    bool use_nulls_,
    DB::JoinKind kind,
    DB::JoinStrictness strictness,
    bool has_mixed_join_condition,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    const String & comment,
    const bool overwrite_)
    : key_names({}), use_nulls(use_nulls_), row_count(row_count_), overwrite(overwrite_)
{
    storage_metadata.setColumns(columns);
    storage_metadata.setConstraints(constraints);
    storage_metadata.setComment(comment);

    for (const auto & key : key_names_)
        if (!storage_metadata.getColumns().hasPhysical(key))
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Key column ({}) does not exist in table declaration.", key);
    for (const auto & name : key_names_)
        key_names.push_back(RIHGT_COLUMN_PREFIX + name);
    auto table_join = std::make_shared<DB::TableJoin>(SizeLimits(), true, kind, strictness, key_names);
    right_sample_block = rightSampleBlock(use_nulls, storage_metadata, table_join->kind());
    /// If there is mixed join conditions, need to build the hash join lazily, which rely on the real table join.
    if (!has_mixed_join_condition)
        buildJoin(in, right_sample_block, table_join);
    else
        collectAllInputs(in, right_sample_block);
}

/// The column names may be different in two blocks.
/// and the nullability also could be different, with TPCDS-Q1 as an example.
static DB::ColumnWithTypeAndName convertColumnAsNecessary(const DB::ColumnWithTypeAndName & column, const DB::ColumnWithTypeAndName & sample_column)
{
    if (sample_column.type->equals(*column.type))
        return {column.column, column.type, sample_column.name};
    else if (
        sample_column.type->isNullable() && !column.type->isNullable()
        && DB::removeNullable(sample_column.type)->equals(*column.type))
    {
        auto nullable_column = column;
        DB::JoinCommon::convertColumnToNullable(nullable_column);
        return {nullable_column.column, sample_column.type, sample_column.name};
    }
    else
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Columns have different types. original:{} expected:{}",
            column.dumpStructure(),
            sample_column.dumpStructure());
}

void StorageJoinFromReadBuffer::buildJoin(DB::ReadBuffer & in, const Block header, std::shared_ptr<DB::TableJoin> analyzed_join)
{
    local_engine::NativeReader block_stream(in);
    ProfileInfo info;
    join = std::make_shared<HashJoin>(analyzed_join, header, overwrite, row_count);
    while (Block block = block_stream.read())
    {
        DB::ColumnsWithTypeAndName columns;
        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & column = block.getByPosition(i);
            columns.emplace_back(convertColumnAsNecessary(column, header.getByPosition(i)));
        }
        DB::Block final_block(columns);
        info.update(final_block);
        join->addBlockToJoin(final_block, true);
    }
}

void StorageJoinFromReadBuffer::collectAllInputs(DB::ReadBuffer & in, const DB::Block header)
{
    local_engine::NativeReader block_stream(in);
    ProfileInfo info;
    while (Block block = block_stream.read())
    {
        DB::ColumnsWithTypeAndName columns;
        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & column = block.getByPosition(i);
            columns.emplace_back(convertColumnAsNecessary(column, header.getByPosition(i)));
        }
        DB::Block final_block(columns);
        info.update(final_block);
        input_blocks.emplace_back(std::move(final_block));
    }
}

void StorageJoinFromReadBuffer::buildJoinLazily(DB::Block header, std::shared_ptr<DB::TableJoin> analyzed_join)
{
    {
        std::shared_lock lock(join_mutex);
        if (join)
            return;
    }
    std::unique_lock lock(join_mutex);
    if (join)
        return;
    join = std::make_shared<HashJoin>(analyzed_join, header, overwrite, row_count);
    while(!input_blocks.empty())
    {
        auto & block = *input_blocks.begin();
        DB::ColumnsWithTypeAndName columns;
        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & column = block.getByPosition(i);
            columns.emplace_back(convertColumnAsNecessary(column, header.getByPosition(i)));
        }
        DB::Block final_block(columns);
        join->addBlockToJoin(final_block, true);
        input_blocks.pop_front();
    }
}


/// The column names of 'rgiht_header' could be different from the ones in `input_blocks`, and we must
/// use 'right_header' to build the HashJoin. Otherwise, it will cause exceptions with name mismatches.
///
/// In most cases, 'getJoinLocked' is called only once, and the input_blocks should not be too large.
/// This is will be OK.
DB::JoinPtr StorageJoinFromReadBuffer::getJoinLocked(std::shared_ptr<DB::TableJoin> analyzed_join, DB::ContextPtr /*context*/)
{
    if ((analyzed_join->forceNullableRight() && !use_nulls)
        || (!analyzed_join->forceNullableRight() && isLeftOrFull(analyzed_join->kind()) && use_nulls))
        throw Exception(
            ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
            "Table {} needs the same join_use_nulls setting as present in LEFT or FULL JOIN",
            storage_metadata.comment);
    buildJoinLazily(getRightSampleBlock(), analyzed_join);
    HashJoinPtr join_clone = std::make_shared<HashJoin>(analyzed_join, right_sample_block);
    /// reuseJoinedData will set the flag `HashJoin::from_storage_join` which is required by `FilledStep`
    join_clone->reuseJoinedData(static_cast<const HashJoin &>(*join));
    return join_clone;
}
}
